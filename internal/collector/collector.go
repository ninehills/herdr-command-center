// Package collector turns herdr state into telemetry events.
//
// Two sources, no overlap:
//   - a long-lived socket subscription for LIFECYCLE events (workspace /
//     tab / pane / worktree created·renamed·closed). Herdr replays current
//     state on subscribe, so events inside a grace window after connect
//     warm the cache without emitting.
//   - a polling diff of agent.list for AGENT ACTIVITY (harness seen/gone,
//     status transitions, derived work runs). Polling adapts: fast while
//     any agent is working, slow when everything is idle.
package collector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/harness"
	"github.com/ninehills/herdr-command-center/internal/herdrapi"
	"github.com/ninehills/herdr-command-center/internal/notify"
	"github.com/ninehills/herdr-command-center/internal/sink"
)

// Event is the wire schema (v1). Fields are omitted when empty or disabled
// by privacy config.
type Event struct {
	V               int    `json:"v"`
	Kind            string `json:"kind"`
	TS              string `json:"ts"`
	Host            string `json:"host,omitempty"`
	WorkspaceID     string `json:"workspace_id,omitempty"`
	WorkspaceLabel  string `json:"workspace_label,omitempty"`
	WorkspaceNumber int    `json:"workspace_number,omitempty"`
	TabID           string `json:"tab_id,omitempty"`
	TabLabel        string `json:"tab_label,omitempty"`
	PaneID          string `json:"pane_id,omitempty"`
	TerminalID      string `json:"terminal_id,omitempty"`
	Harness         string `json:"harness,omitempty"`
	Status          string `json:"status,omitempty"`
	PrevStatus      string `json:"prev_status,omitempty"`
	Cwd             string `json:"cwd,omitempty"`
	RunStartedAt    string `json:"run_started_at,omitempty"`
	RunDurationMS   int64  `json:"run_duration_ms,omitempty"`
	// focus.interval fields (also reused by focus.changed)
	StartedAt       string    `json:"started_at,omitempty"`
	EndedAt         string    `json:"ended_at,omitempty"`
	DurationSeconds int64     `json:"duration_seconds,omitempty"`
	Reason          string    `json:"reason,omitempty"`
	Repo            *RepoInfo `json:"repo,omitempty"`
	Model           string    `json:"model,omitempty"`
	TokensIn        int64     `json:"tokens_in,omitempty"`
	TokensOut       int64     `json:"tokens_out,omitempty"`
	TokensCacheR    int64     `json:"tokens_cache_read,omitempty"`
	TokensCacheW    int64     `json:"tokens_cache_write,omitempty"`
	// Session-cumulative stats for live views (run.usage only, never sent
	// to the endpoint)
	Session *harness.SessionStats `json:"session,omitempty"`
	Detail  string                `json:"detail,omitempty"`
}

// RepoInfo is the git identity of the repository enclosing a pane's
// foreground working directory. It rides on agent/snapshot/focus events as
// a nested object and doubles as a stable grouping key for time-by-repo
// aggregation. Path/identity components inherit cwd privacy: when cwd is
// redacted, Root is emitted as a stable "sha256:" hash and Name/Branch are
// suppressed (see shapeRepo).
type RepoInfo struct {
	Root       string `json:"repo_root,omitempty"`
	Name       string `json:"repo_name,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Remote     string `json:"remote,omitempty"` // "owner/repo" for github remotes
	IsWorktree bool   `json:"is_worktree,omitempty"`
}

type agentKey struct{ paneID, harness string }

type agentState struct {
	herdrapi.Agent
	runStart time.Time
	// session-usage tracking (collect.session_usage): counters + model id
	// from the harness's session file — structural fields only, no content
	sess       harness.Tracker
	usageStart harness.Usage
	// last per-run delta + session snapshot streamed to observers via run.usage
	usageReported   harness.Usage
	sessionReported harness.SessionStats
	usageSent       bool // at least one run.usage streamed for this baseline
}

// repoInfo is the raw (unredacted) resolution cached per cwd. Privacy
// shaping happens at emit time via shapeRepo, so the cache stays reusable
// regardless of config.
type repoInfo struct {
	root, name, branch string
	remote             string // "owner/repo" when origin points at github
	isWorktree         bool
	ok                 bool
}

// focusState tracks the currently-focused pane and when it took focus, so
// a completed focus.interval can be emitted when focus leaves. Owned by the
// poll goroutine only (no cross-goroutine access → no lock needed).
type focusState struct {
	pane   herdrapi.Pane
	since  time.Time
	seen   time.Time // last successful observation (for degraded-gap attribution)
	active bool
}

type EventObserver func(Event)

type Collector struct {
	cfg      config.Config
	api      *herdrapi.Client
	out      *sink.Sink
	observer EventObserver
	host     string
	agents   map[agentKey]*agentState // poll goroutine only
	focus    focusState               // poll goroutine only
	// wsMeta is written by the poll goroutine and read by the lifecycle
	// stream goroutine — guard it or the runtime map detector kills the daemon
	wsMu    sync.RWMutex
	wsMeta  map[string]herdrapi.Workspace
	tabMeta map[string]herdrapi.Tab // tab_id → tab (labels), same lock
	// repoCache is read+written by the poll goroutine and invalidated by the
	// lifecycle stream goroutine (worktree events) — same rule as wsMeta.
	repoMu    sync.RWMutex
	repoCache map[string]repoInfo
	pollErrs  int
	notifier  *notify.Notifier
}

func New(cfg config.Config, out *sink.Sink, notifier *notify.Notifier, observer EventObserver) *Collector {
	return &Collector{
		cfg:       cfg,
		api:       herdrapi.New(),
		out:       out,
		observer:  observer,
		host:      hostID(cfg),
		agents:    map[agentKey]*agentState{},
		wsMeta:    map[string]herdrapi.Workspace{},
		tabMeta:   map[string]herdrapi.Tab{},
		repoCache: map[string]repoInfo{},
		notifier:  notifier,
	}
}

func hostID(cfg config.Config) string {
	name, _ := os.Hostname()
	if !cfg.Privacy.HashHost {
		return name
	}
	return shortHash(name)
}

// shortHash is the stable, non-reversible grouping token used for hostnames
// and (under cwd redaction) repo roots.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:6])
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (c *Collector) base(kind string) Event {
	return Event{V: 1, Kind: kind, TS: time.Now().UTC().Format(time.RFC3339Nano), Host: c.host}
}

func (c *Collector) emit(ev Event) {
	if ev, ok := c.shape(ev); ok {
		if c.observer != nil {
			c.observer(ev)
		}
		c.out.Enqueue(ev)
	}
}

func (c *Collector) observe(ev Event) {
	if ev, ok := c.shape(ev); ok && c.observer != nil {
		c.observer(ev)
	}
}

func (c *Collector) shape(ev Event) (Event, bool) {
	if ev.WorkspaceLabel == "" && ev.WorkspaceID != "" {
		c.wsMu.RLock()
		if w, ok := c.wsMeta[ev.WorkspaceID]; ok {
			ev.WorkspaceLabel, ev.WorkspaceNumber = w.Label, w.Number
		}
		c.wsMu.RUnlock()
	}
	if ev.WorkspaceLabel != "" && !c.cfg.WorkspaceAllowed(ev.WorkspaceLabel) {
		return Event{}, false
	}
	// fail CLOSED: with an allowlist configured, an event whose workspace
	// label cannot be resolved must not leak through
	if len(c.cfg.Privacy.WorkspaceAllowlist) > 0 && ev.WorkspaceLabel == "" && ev.WorkspaceID != "" {
		return Event{}, false
	}
	if !c.cfg.Privacy.IncludeLabels {
		ev.WorkspaceLabel, ev.TabLabel = "", ""
	}
	if !c.cfg.Privacy.IncludeCwd {
		ev.Cwd = ""
	} else if home, err := os.UserHomeDir(); err == nil && home != "" {
		ev.Cwd = strings.Replace(ev.Cwd, home, "~", 1)
	}
	ev.Repo = c.shapeRepo(ev.Repo)
	return ev, true
}

// shapeRepo applies cwd-level privacy to a resolved repo object. Repo
// identity is the same sensitivity class as cwd: when cwd is redacted
// (the default), the absolute root collapses to a stable "sha256:" hash —
// still a usable grouping key for time-by-repo — while repo name and branch
// are suppressed entirely. When cwd is included, the root is home-relative
// and name/branch pass through, matching how cwd itself is shaped.
func (c *Collector) shapeRepo(r *RepoInfo) *RepoInfo {
	if r == nil || r.Root == "" {
		return nil
	}
	if !c.cfg.Privacy.IncludeCwd {
		return &RepoInfo{Root: "sha256:" + shortHash(r.Root), IsWorktree: r.IsWorktree}
	}
	out := *r
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		out.Root = strings.Replace(out.Root, home, "~", 1)
	}
	return &out
}

// ── repo.context ─────────────────────────────────────────────────────

// rawRepo returns the (unshaped) repository identity for a working
// directory, memoized per cwd. Resolution touches the socket / git only on
// a cache miss — never in a hot loop — so on steady state it is a map read.
// Negative results are cached too, so a non-repo cwd is not re-probed every
// poll. The cache is invalidated on worktree lifecycle events.
func (c *Collector) rawRepo(cwd, workspaceID string) *repoInfo {
	if !c.cfg.Collect.Repo || cwd == "" {
		return nil
	}
	c.repoMu.RLock()
	ri, ok := c.repoCache[cwd]
	c.repoMu.RUnlock()
	if ok {
		return &ri
	}
	ri = c.resolveRepo(cwd, workspaceID)
	if ri.ok && ri.root != "" && ri.remote == "" {
		ri.remote = gitRemote(ri.root)
	}
	c.repoMu.Lock()
	// Bound the cache. Growth is human-paced (one entry per distinct directory
	// the user's panes visit), never per-tick, but a very long-lived session
	// that cd's through many trees would otherwise accumulate entries forever.
	// A wholesale reset on overflow reuses the worktree-invalidation idiom and
	// is near-free: entries re-resolve lazily and this trips only after
	// thousands of distinct cwds.
	if len(c.repoCache) >= maxRepoCache {
		c.repoCache = map[string]repoInfo{}
	}
	c.repoCache[cwd] = ri
	c.repoMu.Unlock()
	return &ri
}

// maxRepoCache bounds distinct-cwd repo resolutions retained in memory.
const maxRepoCache = 2048

// resolveRepo prefers herdr's native worktree mapping (robust, and it alone
// yields repo name + per-checkout branch + linked-worktree flag), falling
// back to a single timed `git rev-parse` for cwds outside any herdr-tracked
// worktree. Never holds a lock — callers cache the result.
func (c *Collector) resolveRepo(cwd, workspaceID string) repoInfo {
	if workspaceID != "" {
		if wl, err := c.api.WorktreeList(workspaceID, 2*time.Second); err == nil && wl.Source.RepoRoot != "" {
			if underOrEqual(cwd, wl.Source.RepoRoot) {
				ri := repoInfo{root: wl.Source.RepoRoot, name: wl.Source.RepoName, ok: true}
				// longest-prefix worktree match gives this cwd's own branch
				// and whether its checkout is a linked worktree
				best := ""
				for _, wt := range wl.Worktrees {
					if underOrEqual(cwd, wt.Path) && len(wt.Path) > len(best) {
						best, ri.branch, ri.isWorktree = wt.Path, wt.Branch, wt.IsLinkedWorktree
					}
				}
				if best == "" {
					for _, wt := range wl.Worktrees {
						if wt.Path == wl.Source.SourceCheckoutPath {
							ri.branch, ri.isWorktree = wt.Branch, wt.IsLinkedWorktree
						}
					}
				}
				return ri
			}
		}
	}
	return resolveRepoGit(cwd)
}

// resolveRepoGit is the fallback for working directories herdr does not map
// to a workspace checkout (e.g. a pane cd'd into an unrelated repo). One
// short-timeout invocation; a non-repo cwd yields a cached negative.
func resolveRepoGit(cwd string) repoInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", cwd, "rev-parse",
		"--show-toplevel", "--abbrev-ref", "HEAD", "--git-dir", "--git-common-dir")
	out, err := cmd.Output()
	if err != nil {
		return repoInfo{ok: false}
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) == "" {
		return repoInfo{ok: false}
	}
	root := strings.TrimSpace(lines[0])
	ri := repoInfo{root: root, name: filepath.Base(root), ok: true}
	if b := strings.TrimSpace(lines[1]); b != "HEAD" { // HEAD == detached
		ri.branch = b
	}
	if len(lines) >= 4 {
		// A linked worktree's --git-dir (…/.git/worktrees/<name>) points to a
		// different directory than its --git-common-dir (…/.git); a normal
		// checkout's two coincide. Two traps make a raw string compare wrong:
		//   1. git mixes forms — from a SUBDIR of a normal checkout it returns
		//      an absolute --git-dir but a relative --git-common-dir (e.g.
		//      /repo/.git vs ../../.git), which would flag every ordinary
		//      checkout as a worktree.
		//   2. git's absolute output is symlink-resolved, so on macOS (where
		//      /tmp and /var are symlinks) even two normalized absolute strings
		//      can differ for the same directory.
		// Resolve both relative to cwd (git's working dir here) and compare by
		// identity (device+inode), which is immune to both. We do this in Go
		// rather than pass --path-format=absolute so the whole rev-parse can't
		// fail on git < 2.31 and take every repo field down with it.
		ri.isWorktree = !sameDir(absUnder(cwd, strings.TrimSpace(lines[2])),
			absUnder(cwd, strings.TrimSpace(lines[3])))
	}
	return ri
}

// gitRemote returns "owner/repo" when the repository's origin remote points
// at github.com (ssh or https form), else "". Bounded like every other git
// call and cached with the rest of the repoInfo, so it runs once per root.
func gitRemote(root string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "-C", root, "config", "--get", "remote.origin.url").Output()
	if err != nil {
		return ""
	}
	return parseGithubRemote(strings.TrimSpace(string(out)))
}

func parseGithubRemote(url string) string {
	var path string
	switch {
	case strings.HasPrefix(url, "git@github.com:"):
		path = strings.TrimPrefix(url, "git@github.com:")
	case strings.HasPrefix(url, "https://github.com/"):
		path = strings.TrimPrefix(url, "https://github.com/")
	case strings.HasPrefix(url, "ssh://git@github.com/"):
		path = strings.TrimPrefix(url, "ssh://git@github.com/")
	default:
		return ""
	}
	path = strings.TrimSuffix(strings.TrimSuffix(path, "/"), ".git")
	if strings.Count(path, "/") != 1 {
		return ""
	}
	return path
}

// absUnder resolves a git-reported directory to an absolute, cleaned path.
// git emits these either absolute or relative to its working directory (cwd,
// since we invoke with -C cwd); relative forms are joined onto cwd.
func absUnder(cwd, dir string) string {
	if dir == "" || filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	return filepath.Clean(filepath.Join(cwd, dir))
}

// sameDir reports whether two paths name the same directory, comparing by
// device+inode so symlinked path prefixes don't read as distinct. Falls back
// to string equality when either path can't be stat'd (which, for dirs git
// just reported, effectively never happens).
func sameDir(a, b string) bool {
	if a == b {
		return true
	}
	if ai, err := os.Stat(a); err == nil {
		if bi, err := os.Stat(b); err == nil {
			return os.SameFile(ai, bi)
		}
	}
	return false
}

// underOrEqual reports whether path p is base or lives beneath it.
func underOrEqual(p, base string) bool {
	if p == "" || base == "" {
		return false
	}
	p, base = filepath.Clean(p), filepath.Clean(base)
	return p == base || strings.HasPrefix(p, base+string(filepath.Separator))
}

// repoFor builds the raw (unshaped) repo object for an event, or nil.
func (c *Collector) repoFor(cwd, workspaceID string) *RepoInfo {
	raw := c.rawRepo(cwd, workspaceID)
	if raw == nil || !raw.ok {
		return nil
	}
	return &RepoInfo{Root: raw.root, Name: raw.name, Branch: raw.branch, Remote: raw.remote, IsWorktree: raw.isWorktree}
}

// Run drives both sources until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	c.emit(c.base("daemon.started"))
	// wrap in a closure: defer evaluates args immediately, which would
	// stamp the shutdown event with the startup timestamp
	defer func() { c.emit(c.base("daemon.stopped")) }()

	go c.streamLifecycle(ctx)

	// snapshot_interval_ms = 0 disables snapshots (NewTicker(0) would panic)
	var snapshotC <-chan time.Time
	if c.cfg.SnapshotInterval() > 0 {
		snapshotTick := time.NewTicker(c.cfg.SnapshotInterval())
		defer snapshotTick.Stop()
		snapshotC = snapshotTick.C
	}

	c.pollAgents(true) // seed silently
	if c.cfg.SnapshotInterval() > 0 {
		c.snapshot()
	}

	for {
		interval := c.cfg.IdlePollInterval()
		if c.anyWorking() {
			interval = c.cfg.PollInterval()
		}
		select {
		case <-ctx.Done():
			c.closeOpenRuns()
			c.closeFocus("shutdown", time.Now())
			return
		case <-time.After(interval):
			c.pollAgents(false)
		case <-snapshotC:
			c.snapshot()
		}
	}
}

func (c *Collector) anyWorking() bool {
	for _, a := range c.agents {
		if a.AgentStatus == "working" {
			return true
		}
	}
	return false
}

// ── agent polling ────────────────────────────────────────────────────

func (c *Collector) pollAgents(seed bool) {
	// the poll tick also drives focus intervals, so run whenever either the
	// agent diff or focus tracking is enabled
	if !c.cfg.Collect.Agents && !c.cfg.Collect.FocusIntervals {
		return
	}
	list, err := c.api.AgentList(4 * time.Second)
	if err != nil {
		log.Printf("agent.list: %v", err)
		// a persistently unreachable socket must be visible at the endpoint,
		// not just in a local log nobody reads
		c.pollErrs++
		if c.pollErrs == 5 {
			ev := c.base("collector.degraded")
			ev.Detail = "agent.list failing repeatedly: " + err.Error()
			c.out.Enqueue(ev)
			c.notifier.Notify(notify.Degraded, "herdr-command-center degraded",
				"can't reach the herdr socket")
			// we can no longer trust that focus stayed put — close the open
			// interval at the last good observation rather than counting the
			// outage as focus time
			c.closeFocus("degraded", c.focus.seen)
		}
		return
	}
	if c.pollErrs >= 5 {
		ev := c.base("collector.recovered")
		c.out.Enqueue(ev)
		c.notifier.Notify(notify.Recovered, "herdr-command-center recovered",
			"herdr socket reachable again")
	}
	c.pollErrs = 0
	if ws, err := c.api.WorkspaceList(4 * time.Second); err == nil {
		next := make(map[string]herdrapi.Workspace, len(ws))
		for _, w := range ws {
			next[w.WorkspaceID] = w
		}
		c.wsMu.Lock()
		c.wsMeta = next
		c.wsMu.Unlock()
	}
	if tabs, err := c.api.TabList(4 * time.Second); err == nil {
		next := make(map[string]herdrapi.Tab, len(tabs))
		for _, t := range tabs {
			next[t.TabID] = t
		}
		c.wsMu.Lock()
		c.tabMeta = next
		c.wsMu.Unlock()
	}

	if c.cfg.Collect.FocusIntervals {
		c.pollFocus(seed)
	}
	if !c.cfg.Collect.Agents {
		return
	}

	seen := map[agentKey]bool{}
	for _, a := range list {
		k := agentKey{a.PaneID, a.Agent}
		seen[k] = true
		prev, known := c.agents[k]
		if !known {
			st := &agentState{Agent: a}
			c.agents[k] = st
			if seed {
				c.observe(c.agentEvent("agent.seen", a, ""))
			} else {
				c.emit(c.agentEvent("agent.seen", a, ""))
			}
			if a.AgentStatus == "working" {
				st.runStart = time.Now()
				st.usageStart = c.ensureSession(st)
				if seed && c.cfg.Collect.Runs {
					// already mid-run at daemon boot: emit run.started now
					// (true start unknown) so live views see the open run
					ev := c.agentEvent("run.started", a, "")
					ev.RunStartedAt = st.runStart.UTC().Format(time.RFC3339)
					if st.sess != nil {
						ev.Model = st.sess.Model()
					}
					ev.Detail = "observed mid-run at daemon start"
					c.emit(ev)
				}
			}
			continue
		}
		if prev.AgentStatus != a.AgentStatus {
			c.emit(c.agentEvent("agent.status_changed", a, prev.AgentStatus))
			if c.cfg.Collect.Runs {
				c.runTransition(prev, a)
			}
			prev.AgentStatus = a.AgentStatus
		}
		if agentSessionPath(prev.Agent) != agentSessionPath(a) {
			// agent started a new session (e.g. pi /new): re-attach and
			// re-baseline so stale counters never leak across sessions
			prev.sess = nil
			prev.usageSent = false
			prev.AgentSession = a.AgentSession
			if !prev.runStart.IsZero() {
				prev.usageStart = c.ensureSession(prev)
			}
		}
		prev.Cwd, prev.ForegroundCwd, prev.Focused = a.Cwd, a.ForegroundCwd, a.Focused
		c.reportUsage(prev, a)
	}
	for k, st := range c.agents {
		if !seen[k] {
			c.emit(c.agentEvent("agent.gone", st.Agent, st.AgentStatus))
			c.finishRun(st, "agent.gone")
			delete(c.agents, k)
		}
	}
}

// ensureSession attaches (or refreshes) the harness session tracker for a
// working agent and returns the current counter snapshot.
func (c *Collector) ensureSession(st *agentState) harness.Usage {
	if !c.cfg.Collect.SessionUsage {
		return harness.Usage{}
	}
	cwd := st.ForegroundCwd
	if cwd == "" {
		cwd = st.Cwd
	}
	if st.sess == nil {
		if p := agentSessionPath(st.Agent); p != "" {
			// exact match reported by herdr's agent integration
			st.sess = harness.AttachPath(st.Agent.Agent, p, "")
		} else {
			st.sess = harness.Attach(st.Agent.Agent, cwd, "", c.sessionClaims(st))
		}
	}
	if st.sess == nil {
		return harness.Usage{}
	}
	st.sess.Refresh()
	return st.sess.Usage()
}

// agentSessionPath returns the session file herdr reports for this agent.
func agentSessionPath(a herdrapi.Agent) string {
	if a.AgentSession != nil && a.AgentSession.Kind == "path" {
		return a.AgentSession.Value
	}
	return ""
}

// sessionClaims returns the session files already attached by other agents,
// so two agents sharing a cwd never read the same file.
func (c *Collector) sessionClaims(self *agentState) map[string]bool {
	skip := map[string]bool{}
	for _, a := range c.agents {
		if a != self && a.sess != nil {
			skip[a.sess.Path()] = true
		}
	}
	return skip
}

func applyUsage(ev *Event, model string, u harness.Usage) {
	ev.Model = model
	ev.TokensIn = u.In
	ev.TokensOut = u.Out
	ev.TokensCacheR = u.CacheRead
	ev.TokensCacheW = u.CacheWrite
}

// runTransition derives run.started / run.finished from status flips.
// "blocked" pauses inside a run rather than ending it — an agent waiting
// on input is still mid-task.
func (c *Collector) runTransition(st *agentState, now herdrapi.Agent) {
	wasRunning := !st.runStart.IsZero()
	if now.AgentStatus == "working" && !wasRunning {
		st.runStart = time.Now()
		st.sess = nil // re-resolve: the file being written now is this run's
		st.usageStart = c.ensureSession(st)
		st.usageReported = harness.Usage{}
		st.sessionReported = harness.SessionStats{}
		st.usageSent = false
		ev := c.agentEvent("run.started", now, st.AgentStatus)
		ev.RunStartedAt = st.runStart.UTC().Format(time.RFC3339)
		if st.sess != nil {
			ev.Model = st.sess.Model()
		}
		c.emit(ev)
	}
	if wasRunning && (now.AgentStatus == "idle" || now.AgentStatus == "done") {
		st.Agent.AgentStatus = now.AgentStatus
		c.finishRun(st, "")
	}
}

func (c *Collector) finishRun(st *agentState, detail string) {
	if st.runStart.IsZero() {
		return
	}
	dur := time.Since(st.runStart)
	start := st.runStart
	st.runStart = time.Time{}
	if dur < time.Duration(c.cfg.Collect.MinRunMS)*time.Millisecond {
		return // ignore blips
	}
	ev := c.agentEvent("run.finished", st.Agent, "working")
	ev.RunStartedAt = start.UTC().Format(time.RFC3339)
	ev.RunDurationMS = dur.Milliseconds()
	ev.Detail = detail
	if cur := c.ensureSession(st); st.sess != nil {
		delta := cur.Sub(st.usageStart)
		applyUsage(&ev, st.sess.Model(), delta)
		sess := st.sess.Session()
		ev.Session = &sess
		st.sessionReported = sess
		if !delta.IsZero() {
			st.usageReported = delta
		}
	}
	c.emit(ev)
}

// reportUsage streams live per-run usage deltas plus session-cumulative
// stats to WS observers only (never the sink): panels watch counters grow
// during the run, session stats show even while idle, and session-file
// writes that flush after run.finished still land as trailing updates.
func (c *Collector) reportUsage(st *agentState, now herdrapi.Agent) {
	if st.sess == nil {
		// attach eagerly so idle agents show session stats too; run baselines
		// are taken at run.started, so early attaches stay correct
		c.ensureSession(st)
		if st.sess == nil {
			return
		}
	}
	st.sess.Refresh()
	cur := st.sess.Usage().Sub(st.usageStart)
	if cur.IsZero() && st.usageSent {
		return // counters regressed (session replaced) — never wipe
	}
	sess := st.sess.Session()
	if st.usageSent && cur == st.usageReported && sess == st.sessionReported {
		return // nothing new for the panel
	}
	st.usageReported = cur
	st.sessionReported = sess
	st.usageSent = true
	ev := c.agentEvent("run.usage", now, "")
	if !st.runStart.IsZero() {
		ev.RunStartedAt = st.runStart.UTC().Format(time.RFC3339)
		ev.RunDurationMS = time.Since(st.runStart).Milliseconds()
	}
	applyUsage(&ev, st.sess.Model(), cur)
	ev.Session = &sess
	c.observe(ev)
}

func (c *Collector) closeOpenRuns() {
	for _, st := range c.agents {
		c.finishRun(st, "daemon.stopped")
	}
}

// ── focus.interval ───────────────────────────────────────────────────
//
// User attention is the single pane herdr marks focused == true in
// pane.list. That boolean is stable, unlike the pane.focused *event*, which
// fires ~10×/second cycling every pane as herdr's detection scan cursor and
// is useless for attention — so we deliberately do NOT subscribe to it and
// instead derive focus from the poll tick (piggybacking the existing loop,
// no new goroutine). Durations are computed from our own timestamps, so a
// degraded gap is never counted as focus time. Resolution is the poll
// cadence: sub-poll focus flickers are not attention and are not reported.

func (c *Collector) pollFocus(seed bool) {
	panes, err := c.api.PaneList(4 * time.Second)
	if err != nil {
		return // best-effort; the agent.list error path owns degraded handling
	}
	now := time.Now()
	var focused *herdrapi.Pane
	for i := range panes {
		if panes[i].Focused {
			focused = &panes[i]
			break
		}
	}

	// a gap since the last observation (transient outage, laptop sleep) must
	// not inflate the open interval — close it at the last good timestamp
	if c.focus.active && now.Sub(c.focus.seen) > c.focusGap() {
		c.closeFocus("degraded", c.focus.seen)
	}

	if focused == nil {
		// focus left every pane (herdr window unfocused / no panes)
		c.closeFocus("focus_change", now)
		return
	}
	if !c.focus.active {
		c.focus = focusState{pane: *focused, since: now, seen: now, active: true}
		if !seed {
			c.emitFocusChanged(*focused)
		}
		return
	}
	if focused.PaneID != c.focus.pane.PaneID {
		c.closeFocus("focus_change", now)
		c.focus = focusState{pane: *focused, since: now, seen: now, active: true}
		c.emitFocusChanged(*focused)
		return
	}
	// same pane still focused — refresh context (cwd may have changed)
	c.focus.pane = *focused
	c.focus.seen = now
}

// focusGap is the staleness beyond which we assume the socket went dark and
// stop trusting the open interval's wall-clock span.
func (c *Collector) focusGap() time.Duration {
	return 3 * c.cfg.IdlePollInterval()
}

func (c *Collector) closeFocus(reason string, endedAt time.Time) {
	if !c.focus.active {
		return
	}
	p := c.focus.pane
	c.focus.active = false
	ev := c.base("focus.interval")
	ev.PaneID, ev.TabID, ev.TerminalID = p.PaneID, p.TabID, p.TerminalID
	ev.WorkspaceID, ev.Harness = p.WorkspaceID, p.Agent
	ev.Cwd = firstNonEmpty(p.ForegroundCwd, p.Cwd)
	ev.Repo = c.repoFor(ev.Cwd, p.WorkspaceID)
	ev.StartedAt = c.focus.since.UTC().Format(time.RFC3339)
	ev.EndedAt = endedAt.UTC().Format(time.RFC3339)
	if d := int64(endedAt.Sub(c.focus.since).Seconds()); d > 0 {
		ev.DurationSeconds = d
	}
	ev.Reason = reason
	c.emit(ev)
}

// emitFocusChanged is the lightweight now-focused pointer for a live panel.
func (c *Collector) emitFocusChanged(p herdrapi.Pane) {
	ev := c.base("focus.changed")
	ev.PaneID, ev.TabID, ev.TerminalID = p.PaneID, p.TabID, p.TerminalID
	ev.WorkspaceID, ev.Harness = p.WorkspaceID, p.Agent
	ev.Cwd = firstNonEmpty(p.ForegroundCwd, p.Cwd)
	ev.Repo = c.repoFor(ev.Cwd, p.WorkspaceID)
	c.emit(ev)
}

func (c *Collector) agentEvent(kind string, a herdrapi.Agent, prev string) Event {
	ev := c.base(kind)
	ev.PaneID, ev.TabID, ev.TerminalID = a.PaneID, a.TabID, a.TerminalID
	ev.WorkspaceID, ev.Harness = a.WorkspaceID, a.Agent
	ev.Status, ev.PrevStatus = a.AgentStatus, prev
	ev.Cwd = firstNonEmpty(a.ForegroundCwd, a.Cwd)
	ev.Repo = c.repoFor(ev.Cwd, a.WorkspaceID) // raw; emit() shapes it
	c.wsMu.RLock()
	if w, ok := c.wsMeta[a.WorkspaceID]; ok {
		ev.WorkspaceLabel, ev.WorkspaceNumber = w.Label, w.Number
	}
	if t, ok := c.tabMeta[a.TabID]; ok {
		ev.TabLabel = t.Label
	}
	c.wsMu.RUnlock()
	return ev
}

// snapshot emits a compact full-state event so consumers can reconcile
// without replaying every transition.
func (c *Collector) snapshot() {
	type wsSnap struct {
		ID     string `json:"id"`
		Label  string `json:"label,omitempty"`
		Number int    `json:"number"`
		Status string `json:"status"`
		Panes  int    `json:"panes"`
		Tabs   int    `json:"tabs"`
	}
	type agSnap struct {
		Pane    string    `json:"pane"`
		WS      string    `json:"ws"`
		Harness string    `json:"harness"`
		Status  string    `json:"status"`
		Repo    *RepoInfo `json:"repo,omitempty"`
	}
	var wss []wsSnap
	c.wsMu.RLock()
	for _, w := range c.wsMeta {
		if !c.cfg.WorkspaceAllowed(w.Label) {
			continue
		}
		label := w.Label
		if !c.cfg.Privacy.IncludeLabels {
			label = ""
		}
		wss = append(wss, wsSnap{w.WorkspaceID, label, w.Number, w.AgentStatus, w.PaneCount, w.TabCount})
	}
	c.wsMu.RUnlock()
	var ags []agSnap
	for _, a := range c.agents {
		// snapshot bypasses emit(), so shape the repo here directly
		repo := c.shapeRepo(c.repoFor(firstNonEmpty(a.ForegroundCwd, a.Cwd), a.WorkspaceID))
		ags = append(ags, agSnap{a.PaneID, a.WorkspaceID, a.Agent.Agent, a.AgentStatus, repo})
	}
	detail, _ := json.Marshal(map[string]any{"workspaces": wss, "agents": ags})
	ev := c.base("snapshot")
	ev.Detail = string(detail)
	c.out.Enqueue(ev)
}

// ── lifecycle event stream ───────────────────────────────────────────

// lifecycle payloads share this shape: the entity object under its own key.
type lifecycleData struct {
	Type      string              `json:"type"`
	Workspace *herdrapi.Workspace `json:"workspace"`
	Pane      *herdrapi.Agent     `json:"pane"` // pane payloads carry the same fields we need
	Tab       *struct {
		TabID       string `json:"tab_id"`
		WorkspaceID string `json:"workspace_id"`
		Label       string `json:"label"`
	} `json:"tab"`
	WorkspaceID string `json:"workspace_id"`
	PaneID      string `json:"pane_id"`
	TabID       string `json:"tab_id"`
	Path        string `json:"path"`
}

func (c *Collector) subscriptions() []herdrapi.Subscription {
	var subs []herdrapi.Subscription
	add := func(types ...string) {
		for _, t := range types {
			subs = append(subs, herdrapi.Subscription{Type: t})
		}
	}
	if c.cfg.Collect.Workspaces {
		add("workspace.created", "workspace.renamed", "workspace.closed")
		if c.cfg.Collect.FocusEvents {
			add("workspace.focused")
		}
	}
	if c.cfg.Collect.Tabs {
		add("tab.created", "tab.closed", "tab.renamed")
	}
	if c.cfg.Collect.Panes {
		add("pane.created", "pane.closed", "pane.exited")
	}
	if c.cfg.Collect.Worktrees {
		add("worktree.created", "worktree.opened", "worktree.removed")
	}
	return subs
}

func (c *Collector) streamLifecycle(ctx context.Context) {
	subs := c.subscriptions()
	if len(subs) == 0 {
		return
	}
	backoff := time.Second
	for ctx.Err() == nil {
		var connectedAt time.Time
		err := c.api.Stream(ctx, subs,
			func(t time.Time) { connectedAt = t; backoff = time.Second },
			func(f herdrapi.EventFrame) {
				// herdr replays current state on subscribe — swallow the burst
				if time.Since(connectedAt) < c.cfg.ReplayGrace() {
					return
				}
				c.handleLifecycle(f)
			})
		if ctx.Err() != nil {
			return
		}
		log.Printf("event stream: %v (reconnecting in %s)", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Collector) handleLifecycle(f herdrapi.EventFrame) {
	var d lifecycleData
	if err := json.Unmarshal(f.Data, &d); err != nil {
		return
	}
	// frame names are underscored (workspace_created) — normalize to dots
	kind := strings.Replace(f.Event, "_", ".", 1)
	// a worktree add/remove can change a checkout's branch or worktree
	// status; drop the whole (small) repo cache so the next poll re-resolves
	if strings.HasPrefix(f.Event, "worktree") {
		c.repoMu.Lock()
		c.repoCache = map[string]repoInfo{}
		c.repoMu.Unlock()
	}
	ev := c.base(kind)
	switch {
	case d.Workspace != nil:
		ev.WorkspaceID = d.Workspace.WorkspaceID
		ev.WorkspaceLabel = d.Workspace.Label
		ev.WorkspaceNumber = d.Workspace.Number
	case d.Pane != nil:
		ev.PaneID, ev.TabID = d.Pane.PaneID, d.Pane.TabID
		ev.WorkspaceID = d.Pane.WorkspaceID
		ev.Cwd = d.Pane.Cwd
	case d.Tab != nil:
		ev.TabID, ev.WorkspaceID = d.Tab.TabID, d.Tab.WorkspaceID
		ev.TabLabel = d.Tab.Label
	default:
		ev.WorkspaceID, ev.PaneID, ev.TabID = d.WorkspaceID, d.PaneID, d.TabID
		ev.Cwd = d.Path // worktree events carry a path
	}
	if ev.WorkspaceLabel == "" {
		c.wsMu.RLock()
		if w, ok := c.wsMeta[ev.WorkspaceID]; ok {
			ev.WorkspaceLabel, ev.WorkspaceNumber = w.Label, w.Number
		}
		c.wsMu.RUnlock()
	}
	c.emit(ev)
}
