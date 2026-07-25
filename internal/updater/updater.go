// Package updater keeps the plugin current: it periodically asks GitHub for
// the latest release and, when a newer version exists, either notifies or
// re-installs and restarts the daemon.
//
// Apply path reuses herdr's own installer (`herdr plugin install … --yes`)
// so herdr's version metadata and binary path stay consistent, and so the
// prebuilt-binary fetch in the build step handles Go-less hosts. If anything
// fails the current daemon keeps running the old binary — telemetry never
// goes down for an update. Even if the explicit restart fails, the next herdr
// lifecycle hook (`ensure-daemon`) starts the freshly-installed binary, so
// updates are self-healing.
package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/notify"
)

// Emitter enqueues a telemetry event (the sink's Enqueue).
type Emitter interface {
	Enqueue(ev any)
}

type Updater struct {
	cfg      config.Update
	version  string
	host     string
	out      Emitter
	notifier *notify.Notifier
	stop     func() // graceful daemon shutdown (triggers the restart handoff)
	logf     func(string, ...any)
}

func New(cfg config.Update, version, host string, out Emitter, notifier *notify.Notifier, stop func(), logf func(string, ...any)) *Updater {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Updater{cfg: cfg, version: version, host: host, out: out, notifier: notifier, stop: stop, logf: logf}
}

// Run polls on an interval until ctx is done. A short initial delay lets the
// daemon settle before the first check.
func (u *Updater) Run(ctx context.Context) {
	if !u.cfg.Enabled {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(90 * time.Second):
	}
	u.checkOnce(ctx)
	t := time.NewTicker(u.cfg.Interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			u.checkOnce(ctx)
		}
	}
}

func (u *Updater) emit(kind, detail string) {
	if u.out == nil {
		return
	}
	u.out.Enqueue(map[string]any{
		"v":      1,
		"kind":   kind,
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"host":   u.host,
		"detail": detail,
	})
}

// CheckOnce fetches the latest release and returns (latestTag, newer, err).
func (u *Updater) CheckOnce(ctx context.Context) (string, bool, error) {
	latest, err := u.latestTag(ctx)
	if err != nil {
		return "", false, err
	}
	return latest, versionLess(u.version, latest), nil
}

func (u *Updater) checkOnce(ctx context.Context) {
	latest, newer, err := u.CheckOnce(ctx)
	if err != nil {
		u.logf("update check failed: %v", err)
		return
	}
	if !newer {
		return
	}
	u.logf("update available: %s → %s", u.version, latest)
	u.emit("update.available", fmt.Sprintf("%s available (running %s)", latest, u.version))
	u.notifier.Notify(notify.Update, "herdr-command-center "+latest+" available",
		"updating from "+u.version)

	bin := os.Getenv("HERDR_BIN_PATH")
	if !u.cfg.AutoApply || bin == "" {
		if bin == "" {
			u.logf("auto-apply skipped: HERDR_BIN_PATH unset")
		}
		return
	}
	u.apply(ctx, bin, latest)
}

func (u *Updater) apply(ctx context.Context, bin, tag string) {
	u.logf("applying update %s via %s", tag, bin)
	ic, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ic, bin, "plugin", "install", u.cfg.Repo, "--yes")
	out, err := cmd.CombinedOutput()
	if err != nil {
		u.logf("update install failed: %v: %s", err, strings.TrimSpace(string(out)))
		u.emit("update.failed", fmt.Sprintf("%s install failed: %v", tag, err))
		u.notifier.Notify(notify.Update, "herdr-command-center update failed", err.Error())
		return
	}
	u.emit("update.applied", fmt.Sprintf("installed %s; restarting", tag))
	u.notifier.Notify(notify.Update, "herdr-command-center updated to "+tag, "restarting the collector")

	// Hand off to the freshly-installed binary: a detached restarter waits
	// for this process to release the flock, then asks herdr to start the
	// new daemon. Even if this fails, the next lifecycle hook recovers it.
	u.spawnRestart(bin)
	if u.stop != nil {
		u.stop()
	}
}

func (u *Updater) spawnRestart(bin string) {
	// double-detached so it outlives this process's graceful shutdown
	script := fmt.Sprintf("sleep 5; %q plugin action invoke herdr-command-center.start", bin)
	cmd := exec.Command("sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	if err := cmd.Start(); err != nil {
		u.logf("restart spawn failed (hooks will recover): %v", err)
		return
	}
	_ = cmd.Process.Release()
}

func (u *Updater) latestTag(ctx context.Context) (string, error) {
	url := "https://api.github.com/repos/" + u.cfg.Repo + "/releases/latest"
	rc, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rc, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "herdr-command-center-updater")
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("github returned %s", resp.Status)
	}
	var body struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.TagName == "" {
		return "", fmt.Errorf("no tag_name in latest release")
	}
	return body.TagName, nil
}

// versionLess reports whether current < latest, comparing the leading
// dotted-numeric components (a leading "v" and any prerelease suffix are
// ignored). Unparseable input compares equal (no update).
func versionLess(current, latest string) bool {
	c := parseVer(current)
	l := parseVer(latest)
	for i := 0; i < 3; i++ {
		if l[i] != c[i] {
			return l[i] > c[i]
		}
	}
	return false
}

func parseVer(s string) [3]int {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	var out [3]int
	for i, part := range strings.SplitN(s, ".", 3) {
		if i > 2 {
			break
		}
		// stop at the first non-digit (handles "1.2.3-rc1")
		num := part
		for j, r := range part {
			if r < '0' || r > '9' {
				num = part[:j]
				break
			}
		}
		out[i], _ = strconv.Atoi(num)
	}
	return out
}
