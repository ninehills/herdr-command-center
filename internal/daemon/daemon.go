// Package daemon owns the singleton lifecycle: a pid lockfile, signal
// handling (SIGTERM stop · SIGUSR1 flush), and a status.json heartbeat that
// the status subcommand reads.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/DIodide/herdr-telemetry/internal/collector"
	"github.com/DIodide/herdr-telemetry/internal/config"
	"github.com/DIodide/herdr-telemetry/internal/notify"
	"github.com/DIodide/herdr-telemetry/internal/sink"
	"github.com/DIodide/herdr-telemetry/internal/updater"
	"github.com/DIodide/herdr-telemetry/internal/wsserver"
)

// Version is set from main (the embedded VERSION) so the updater can compare
// against the latest release.
var Version = "dev"

func lockPath() string   { return filepath.Join(config.StateDir(), "daemon.lock") }
func pidPath() string    { return filepath.Join(config.StateDir(), "daemon.pid") }
func statusPath() string { return filepath.Join(config.StateDir(), "status.json") }
func logPath() string    { return filepath.Join(config.StateDir(), "telemetry.log") }

// tryLock attempts the exclusive daemon flock. The returned file must stay
// open for the daemon's lifetime — the kernel releases the lock on process
// death, which makes the singleton atomic (no check-then-write race) and
// immune to pid reuse.
func tryLock() (*os.File, error) {
	if err := os.MkdirAll(config.StateDir(), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// RunningPID returns the live daemon's pid, or 0. Liveness is judged by the
// flock (held only while a daemon process is alive), not by probing the pid.
func RunningPID() int {
	f, err := tryLock()
	if err != nil {
		// lock is held — a daemon is alive; read its advertised pid
		data, rerr := os.ReadFile(pidPath())
		if rerr != nil {
			return -1 // alive but pid unknown
		}
		pid, perr := strconv.Atoi(string(data))
		if perr != nil || pid <= 0 {
			return -1
		}
		return pid
	}
	// we acquired the lock — no daemon is running
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
	return 0
}

// Ensure spawns a detached daemon unless one is already running. Called by
// the manifest event hooks, so it must be fast and silent when redundant.
func Ensure() error {
	if pid := RunningPID(); pid != 0 {
		return nil
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(self, "daemon")
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout, cmd.Stderr, cmd.Stdin = nil, nil, nil
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

// Stop TERMs the running daemon.
func Stop() error {
	pid := RunningPID()
	if pid == 0 {
		return fmt.Errorf("daemon is not running")
	}
	if pid < 0 {
		return fmt.Errorf("daemon is running but its pid file is unreadable")
	}
	return syscall.Kill(pid, syscall.SIGTERM)
}

// Flush asks the running daemon to flush now.
func Flush() error {
	pid := RunningPID()
	if pid == 0 {
		return fmt.Errorf("daemon is not running")
	}
	if pid < 0 {
		return fmt.Errorf("daemon is running but its pid file is unreadable")
	}
	return syscall.Kill(pid, syscall.SIGUSR1)
}

type Status struct {
	PID        int    `json:"pid"`
	StartedAt  string `json:"started_at"`
	UpdatedAt  string `json:"updated_at"`
	Endpoint   string `json:"endpoint"`
	Enqueued   int64  `json:"events_enqueued"`
	Sent       int64  `json:"events_sent"`
	Dropped    int64  `json:"events_dropped"`
	Spooled    int64  `json:"events_spooled"`
	SendErrors int64  `json:"send_errors"`
	LastError  string `json:"last_error,omitempty"`
	Buffered   int    `json:"buffered"`
	SpoolBytes int64  `json:"spool_bytes"`
}

// ReadStatus loads the last written status.json (may be stale if the
// daemon died — pair with RunningPID).
func ReadStatus() (Status, error) {
	var st Status
	data, err := os.ReadFile(statusPath())
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(data, &st)
}

func writeStatus(st Status) {
	st.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return
	}
	tmp := statusPath() + ".tmp"
	if os.WriteFile(tmp, data, 0o644) == nil {
		_ = os.Rename(tmp, statusPath())
	}
}

// Run is the daemon main loop: acquire the lock, start sink + collector,
// heartbeat status, exit cleanly on SIGTERM.
func Run(cfg config.Config) error {
	if err := os.MkdirAll(config.StateDir(), 0o755); err != nil {
		return err
	}
	lock, err := tryLock()
	if err != nil {
		return fmt.Errorf("daemon already running (lock held: %v)", err)
	}
	defer lock.Close() // kernel drops the flock with the fd
	if err := os.WriteFile(pidPath(), []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return err
	}
	defer os.Remove(pidPath())

	logFile, err := os.OpenFile(logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	log.Printf("daemon started (pid %d, endpoint %s)", os.Getpid(), cfg.Endpoint.URL)

	notifier := notify.New(cfg.Notify)
	host, _ := os.Hostname()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ws, err := wsserver.New(cfg.WebSocket)
	if err != nil {
		return err
	}
	if ws != nil {
		if err := ws.Start(ctx); err != nil {
			return err
		}
		log.Printf("websocket listening on %s%s", cfg.WebSocket.Listen, cfg.WebSocket.Path)
	}

	out := sink.New(cfg)
	out.SetOnState(func(down bool, detail string) {
		if down {
			notifier.Notify(notify.EndpointDown, "herdr-telemetry: endpoint unreachable", detail)
		} else {
			notifier.Notify(notify.EndpointUp, "herdr-telemetry: endpoint reachable", "delivery resumed")
		}
	})
	go out.Run()
	var observer collector.EventObserver
	if ws != nil {
		observer = ws.Publish
	}
	col := collector.New(cfg, out, notifier, observer)

	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)
	started := time.Now().UTC().Format(time.RFC3339)

	// self-update: checks GitHub, re-installs + restarts on a newer release.
	// stop=cancel triggers this daemon's graceful shutdown so the freshly
	// installed binary (started by the detached restarter) can take the lock.
	up := updater.New(cfg.Update, Version, host, out, notifier, cancel, log.Printf)
	go up.Run(ctx)

	notifier.Notify(notify.Start, "herdr-telemetry started", "v"+Version)

	heartbeat := func() {
		writeStatus(Status{
			PID:        os.Getpid(),
			StartedAt:  started,
			Endpoint:   cfg.Endpoint.URL,
			Enqueued:   out.Stats.Enqueued.Load(),
			Sent:       out.Stats.Sent.Load(),
			Dropped:    out.Stats.Dropped.Load(),
			Spooled:    out.Stats.Spooled.Load(),
			SendErrors: out.Stats.SendErrors.Load(),
			LastError:  out.Stats.LastError.Load().(string),
			Buffered:   out.Buffered(),
			SpoolBytes: out.SpoolBytes(),
		})
	}

	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				heartbeat()
			case sig := <-sigs:
				switch sig {
				case syscall.SIGUSR1:
					log.Printf("flush requested")
					out.Kick()
					heartbeat()
				default:
					log.Printf("stopping (%v)", sig)
					cancel()
					return
				}
			}
		}
	}()

	heartbeat()
	col.Run(ctx) // blocks until cancel
	if ws != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = ws.Close(shutdownCtx)
		shutdownCancel()
	}
	out.Stop() // final flush
	heartbeat()
	log.Printf("daemon stopped")
	return nil
}
