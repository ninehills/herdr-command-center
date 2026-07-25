// Package config loads and defaults herdr-command-center's TOML configuration.
//
// Config lives in HERDR_PLUGIN_CONFIG_DIR/config.toml when running as a herdr
// plugin, or ~/.config/herdr-command-center/config.toml otherwise. Every value has
// a safe default; the only thing you must set is endpoint.url.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
)

type Endpoint struct {
	URL       string            `toml:"url"`
	AuthToken string            `toml:"auth_token"`
	Headers   map[string]string `toml:"headers"`
	TimeoutMS int               `toml:"timeout_ms"`
}

type WebSocket struct {
	Enabled   bool   `toml:"enabled"`
	Listen    string `toml:"listen"`
	Path      string `toml:"path"`
	AuthToken string `toml:"auth_token"`
}

type Send struct {
	FlushIntervalMS int  `toml:"flush_interval_ms"`
	MaxBatch        int  `toml:"max_batch"`
	MaxBufferEvents int  `toml:"max_buffer_events"`
	MaxSpoolMB      int  `toml:"max_spool_mb"`
	Gzip            bool `toml:"gzip"`
}

type Collect struct {
	PollIntervalMS     int  `toml:"poll_interval_ms"`
	IdlePollIntervalMS int  `toml:"idle_poll_interval_ms"`
	Workspaces         bool `toml:"workspaces"`
	Tabs               bool `toml:"tabs"`
	Panes              bool `toml:"panes"`
	Agents             bool `toml:"agents"`
	Runs               bool `toml:"runs"`
	Worktrees          bool `toml:"worktrees"`
	Repo               bool `toml:"repo"`
	SessionUsage       bool `toml:"session_usage"`
	FocusEvents        bool `toml:"focus_events"`
	FocusIntervals     bool `toml:"focus_intervals"`
	SnapshotIntervalMS int  `toml:"snapshot_interval_ms"`
	MinRunMS           int  `toml:"min_run_ms"`
	ReplayGraceMS      int  `toml:"replay_grace_ms"`
}

type Privacy struct {
	IncludeCwd         bool     `toml:"include_cwd"`
	IncludeLabels      bool     `toml:"include_labels"`
	HashHost           bool     `toml:"hash_host"`
	WorkspaceAllowlist []string `toml:"workspace_allowlist"`
	WorkspaceDenylist  []string `toml:"workspace_denylist"`
}

// Update controls self-updating: the daemon checks GitHub releases and, when
// a newer version exists, re-installs via herdr and restarts.
type Update struct {
	Enabled            bool   `toml:"enabled"`
	AutoApply          bool   `toml:"auto_apply"`
	CheckIntervalHours int    `toml:"check_interval_hours"`
	Repo               string `toml:"repo"`
}

// Notify controls herdr toast notifications for daemon-health events.
type Notify struct {
	Enabled     bool   `toml:"enabled"`
	OnStart     bool   `toml:"on_start"`
	OnDegraded  bool   `toml:"on_degraded"`
	OnRecovered bool   `toml:"on_recovered"`
	OnEndpoint  bool   `toml:"on_endpoint"`
	OnUpdate    bool   `toml:"on_update"`
	Position    string `toml:"position"`
}

type Config struct {
	Endpoint  Endpoint  `toml:"endpoint"`
	WebSocket WebSocket `toml:"websocket"`
	Send      Send      `toml:"send"`
	Collect   Collect   `toml:"collect"`
	Privacy   Privacy   `toml:"privacy"`
	Update    Update    `toml:"update"`
	Notify    Notify    `toml:"notify"`
}

func Default() Config {
	return Config{
		Endpoint:  Endpoint{TimeoutMS: 5000, Headers: map[string]string{}},
		WebSocket: WebSocket{Listen: "127.0.0.1:9745", Path: "/events"},
		Send: Send{
			FlushIntervalMS: 15000,
			MaxBatch:        200,
			MaxBufferEvents: 2000,
			MaxSpoolMB:      5,
			Gzip:            true,
		},
		Collect: Collect{
			PollIntervalMS:     5000,
			IdlePollIntervalMS: 30000,
			Workspaces:         true,
			Tabs:               true,
			Panes:              true,
			Agents:             true,
			Runs:               true,
			Worktrees:          true,
			Repo:               true,
			SessionUsage:       true,
			FocusEvents:        false,
			FocusIntervals:     false,
			SnapshotIntervalMS: 900000, // 15m full-state snapshot
			MinRunMS:           30000,
			ReplayGraceMS:      2000,
		},
		Privacy: Privacy{
			IncludeCwd:    false,
			IncludeLabels: true,
			HashHost:      true,
		},
		Update: Update{
			Enabled:            true,
			AutoApply:          true,
			CheckIntervalHours: 6,
			Repo:               "ninehills/herdr-command-center",
		},
		Notify: Notify{
			Enabled:     true,
			OnStart:     true,
			OnDegraded:  true,
			OnRecovered: true,
			OnEndpoint:  true,
			OnUpdate:    true,
			Position:    "bottom-right",
		},
	}
}

// ConfigDir resolves where config.toml lives (herdr-injected dir first).
func ConfigDir() string {
	if d := os.Getenv("HERDR_PLUGIN_CONFIG_DIR"); d != "" {
		return d
	}
	base, err := os.UserConfigDir()
	if err != nil {
		base = filepath.Join(os.Getenv("HOME"), ".config")
	}
	return filepath.Join(base, "herdr-command-center")
}

// StateDir resolves where runtime state (lock, spool, status) lives.
func StateDir() string {
	if d := os.Getenv("HERDR_PLUGIN_STATE_DIR"); d != "" {
		return d
	}
	return filepath.Join(os.Getenv("HOME"), ".local", "state", "herdr-command-center")
}

// Load reads config.toml (missing file is fine — defaults apply), then lets
// environment variables override endpoints and tokens for quick setups and CI.
func Load() (Config, string, error) {
	cfg := Default()
	path := filepath.Join(ConfigDir(), "config.toml")
	if _, err := toml.DecodeFile(path, &cfg); err != nil && !os.IsNotExist(err) {
		return cfg, path, fmt.Errorf("parse %s: %w", path, err)
	}
	if v := os.Getenv("HERDR_COMMAND_CENTER_ENDPOINT"); v != "" {
		cfg.Endpoint.URL = v
	}
	if v := os.Getenv("HERDR_COMMAND_CENTER_TOKEN"); v != "" {
		cfg.Endpoint.AuthToken = v
	}
	if v := os.Getenv("HERDR_COMMAND_CENTER_WS_TOKEN"); v != "" {
		cfg.WebSocket.AuthToken = v
	}
	if v := os.Getenv("HERDR_COMMAND_CENTER_WS_LISTEN"); v != "" {
		cfg.WebSocket.Listen = v
	}
	cfg.clamp()
	return cfg, path, nil
}

// clamp keeps hand-edited configs from busy-looping or panicking the
// daemon: every interval gets a sane floor, sizes get a minimum of 1.
func (c *Config) clamp() {
	atLeast := func(v *int, min int) {
		if *v < min {
			*v = min
		}
	}
	atLeast(&c.Endpoint.TimeoutMS, 500)
	atLeast(&c.Send.FlushIntervalMS, 1000)
	atLeast(&c.Send.MaxBatch, 1)
	atLeast(&c.Send.MaxBufferEvents, c.Send.MaxBatch)
	atLeast(&c.Send.MaxSpoolMB, 1)
	atLeast(&c.Collect.PollIntervalMS, 1000)
	atLeast(&c.Collect.IdlePollIntervalMS, c.Collect.PollIntervalMS)
	atLeast(&c.Collect.MinRunMS, 0)
	atLeast(&c.Collect.ReplayGraceMS, 500)
	// snapshot_interval_ms = 0 means disabled; anything else gets a floor
	if c.Collect.SnapshotIntervalMS != 0 {
		atLeast(&c.Collect.SnapshotIntervalMS, 10000)
	}
	atLeast(&c.Update.CheckIntervalHours, 1)
	if c.WebSocket.Listen == "" {
		c.WebSocket.Listen = "127.0.0.1:9745"
	}
	if c.WebSocket.Path == "" || c.WebSocket.Path[0] != '/' {
		c.WebSocket.Path = "/events"
	}
	if c.Update.Repo == "" {
		c.Update.Repo = "ninehills/herdr-command-center"
	}
	if c.Notify.Position == "" {
		c.Notify.Position = "bottom-right"
	}
}

func (c Config) PollInterval() time.Duration {
	return time.Duration(c.Collect.PollIntervalMS) * time.Millisecond
}

func (c Config) IdlePollInterval() time.Duration {
	return time.Duration(c.Collect.IdlePollIntervalMS) * time.Millisecond
}

func (c Config) FlushInterval() time.Duration {
	return time.Duration(c.Send.FlushIntervalMS) * time.Millisecond
}

func (c Config) SnapshotInterval() time.Duration {
	return time.Duration(c.Collect.SnapshotIntervalMS) * time.Millisecond
}

func (c Config) ReplayGrace() time.Duration {
	return time.Duration(c.Collect.ReplayGraceMS) * time.Millisecond
}

func (c Config) HTTPTimeout() time.Duration {
	return time.Duration(c.Endpoint.TimeoutMS) * time.Millisecond
}

func (u Update) Interval() time.Duration {
	return time.Duration(u.CheckIntervalHours) * time.Hour
}

// WorkspaceAllowed applies the allow/deny label filters.
func (c Config) WorkspaceAllowed(label string) bool {
	for _, d := range c.Privacy.WorkspaceDenylist {
		if d == label {
			return false
		}
	}
	if len(c.Privacy.WorkspaceAllowlist) == 0 {
		return true
	}
	for _, a := range c.Privacy.WorkspaceAllowlist {
		if a == label {
			return true
		}
	}
	return false
}
