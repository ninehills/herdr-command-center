// herdr-command-center — a herdr plugin that streams workspace/agent telemetry
// from the herdr socket API to a configurable HTTP endpoint.
//
//	herdr-command-center daemon         run the collector in the foreground
//	herdr-command-center ensure-daemon  start the daemon if not already running
//	herdr-command-center status         show daemon + delivery status
//	herdr-command-center flush          ask the daemon to flush now
//	herdr-command-center stop           stop the daemon
//	herdr-command-center test           send a single test event to the endpoint
//	herdr-command-center print-config   print the effective configuration
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/daemon"
	"github.com/ninehills/herdr-command-center/internal/notify"
	"github.com/ninehills/herdr-command-center/internal/sink"
	"github.com/ninehills/herdr-command-center/internal/updater"
	"github.com/ninehills/herdr-command-center/internal/watch"
)

//go:embed VERSION
var versionRaw string

var version = strings.TrimSpace(versionRaw)

func main() {
	daemon.Version = version
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	// Remote watch and version do not depend on a valid local plugin config.
	switch os.Args[1] {
	case "watch":
		if err := watch.Run(os.Args[2:]); err != nil {
			fatal(err)
		}
		return
	case "version", "--version", "-V":
		fmt.Println("herdr-command-center", version)
		return
	}
	cfg, cfgPath, err := config.Load()
	if err != nil {
		fatal(err)
	}

	switch os.Args[1] {
	case "daemon":
		if err := daemon.Run(cfg); err != nil {
			fatal(err)
		}
	case "ensure-daemon":
		if err := daemon.Ensure(); err != nil {
			fatal(err)
		}
	case "stop":
		if err := daemon.Stop(); err != nil {
			fatal(err)
		}
		fmt.Println("daemon stopped")
	case "flush":
		if err := daemon.Flush(); err != nil {
			fatal(err)
		}
		fmt.Println("flush requested")
	case "status":
		status(cfg, cfgPath)
	case "test":
		testEvent(cfg)
	case "check-update":
		checkUpdate(cfg)
	case "notify-test":
		notify.New(cfg.Notify).Notify(notify.Start, "herdr-command-center", "notification test")
		fmt.Println("notify sent (shows only if herdr [ui.toast] delivery is enabled)")
	case "print-config":
		fmt.Printf("# config file: %s\n", cfgPath)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(cfg)
	default:
		usage()
		os.Exit(2)
	}
}

func status(cfg config.Config, cfgPath string) {
	pid := daemon.RunningPID()
	if pid == 0 {
		fmt.Println("daemon:   not running")
	} else {
		fmt.Printf("daemon:   running (pid %d)\n", pid)
	}
	fmt.Printf("config:   %s\n", cfgPath)
	st, err := daemon.ReadStatus()
	// a running daemon knows its own endpoint (env overrides included) —
	// prefer that over the caller's view of the config
	endpoint := cfg.Endpoint.URL
	if err == nil && pid != 0 && st.Endpoint != "" {
		endpoint = st.Endpoint
	}
	if endpoint == "" {
		fmt.Println("endpoint: (unset — events will spool and drop)")
	} else {
		fmt.Printf("endpoint: %s\n", endpoint)
	}
	if err != nil {
		return
	}
	fmt.Printf("sent:     %d events (%d enqueued, %d buffered, %d spooled, %d dropped)\n",
		st.Sent, st.Enqueued, st.Buffered, st.Spooled, st.Dropped)
	if st.SendErrors > 0 {
		fmt.Printf("errors:   %d (last: %s)\n", st.SendErrors, st.LastError)
	}
	fmt.Printf("updated:  %s\n", st.UpdatedAt)
}

func testEvent(cfg config.Config) {
	if cfg.Endpoint.URL == "" {
		fatal(fmt.Errorf("no endpoint.url configured (set it in config.toml or HERDR_COMMAND_CENTER_ENDPOINT)"))
	}
	out := sink.New(cfg)
	out.Enqueue(map[string]any{
		"v":    1,
		"kind": "test",
		"ts":   time.Now().UTC().Format(time.RFC3339),
	})
	go out.Run()
	out.Kick()
	time.Sleep(200 * time.Millisecond)
	out.Stop()
	if n := out.Stats.Sent.Load(); n > 0 {
		fmt.Printf("ok — sent %d event to %s\n", n, cfg.Endpoint.URL)
		return
	}
	fatal(fmt.Errorf("send failed: %s", out.Stats.LastError.Load()))
}

func checkUpdate(cfg config.Config) {
	up := updater.New(cfg.Update, version, "", nil, nil, nil, nil)
	latest, newer, err := up.CheckOnce(context.Background())
	if err != nil {
		fatal(fmt.Errorf("check failed: %w", err))
	}
	if newer {
		fmt.Printf("update available: %s (running %s)\n", latest, version)
		if cfg.Update.Enabled && cfg.Update.AutoApply {
			fmt.Println("the running daemon will auto-apply it on its next check")
		}
	} else {
		fmt.Printf("up to date (%s is the latest release)\n", version)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `herdr-command-center <command>

commands:
  daemon         run the collector in the foreground
  ensure-daemon  start the daemon if not already running (used by plugin hooks)
  status         show daemon + delivery status
  flush          ask the running daemon to flush now
  stop           stop the running daemon
  test           send a single test event to the configured endpoint
  check-update   check GitHub for a newer release
  notify-test    send a test herdr notification
  print-config   print the effective configuration
  watch URL      live agent dashboard (--token TOKEN)
  version        print version`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "herdr-command-center:", err)
	os.Exit(1)
}
