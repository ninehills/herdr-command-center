// Package harness extracts token COUNTERS and the model id from coding-agent
// session files (Claude Code, Pi). This is the one collector that touches
// harness-owned files, so its contract is strict:
//
//   - only structural fields are decoded: usage counters, model id, cwd.
//     Message content is never inspected, stored, or forwarded.
//   - Claude readers attach at the CURRENT END of the session file and count
//     forward — historical lines are never parsed, and per-run deltas (the
//     only thing telemetry reports) are exact either way.
//   - every read is bounded: Claude/Pi read only bytes appended since the
//     last poll (Pi additionally parses the file once at attach).
//
// Gated by collect.session_usage.
package harness

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Usage is a monotonic counter snapshot. Deltas between two snapshots give
// per-run numbers.
type Usage struct {
	In         int64
	Out        int64
	CacheRead  int64
	CacheWrite int64
}

func (u Usage) Sub(v Usage) Usage {
	d := Usage{u.In - v.In, u.Out - v.Out, u.CacheRead - v.CacheRead, u.CacheWrite - v.CacheWrite}
	// counters can regress if a session is replaced mid-run — clamp
	if d.In < 0 || d.Out < 0 {
		return Usage{}
	}
	return d
}

func (u Usage) IsZero() bool { return u == Usage{} }

// SessionStats is a session-cumulative snapshot for live display, mirroring
// what the harness's own status bar shows (e.g. pi's footer):
// ↑input ↓output R<cache-read> W<cache-write> CH<latest-request hit rate>
// $<cost> <context%>/<window>. Fields a harness file can't provide stay zero.
type SessionStats struct {
	In            int64   `json:"in"`
	Out           int64   `json:"out"`
	CacheRead     int64   `json:"cache_read"`
	CacheWrite    int64   `json:"cache_write"`
	CostUSD       float64 `json:"cost_usd"`
	CacheHit      float64 `json:"cache_hit"` // latest request, percent
	ContextTokens int64   `json:"context_tokens"`
	ContextWindow int64   `json:"context_window"`
}

// Tracker follows one agent's session file.
type Tracker interface {
	// Refresh advances the counters; cheap enough for every poll tick.
	Refresh()
	// Usage returns the current counter snapshot (counters since attach for
	// append-style trackers; run deltas are measured off this).
	Usage() Usage
	// Session returns session-cumulative stats for display.
	Session() SessionStats
	// Model returns the last-seen model id ("" if unknown yet).
	Model() string
	// Path returns the tracked session file (for claim bookkeeping).
	Path() string
}

// Attach locates the live session file for a harness+cwd and returns a
// tracker, or nil if no fresh session file could be found. skip lists
// session files already claimed by other agents (two agents can share a
// cwd; each must read its own file).
func Attach(harnessName, cwd string, home string, skip map[string]bool) Tracker {
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	switch harnessName {
	case "claude":
		if p := locateClaude(home, cwd, skip); p != "" {
			return newClaudeTracker(p)
		}
	case "pi":
		if p := locatePi(home, cwd, skip); p != "" {
			return newPiTracker(p, home)
		}
	}
	return nil
}

// AttachPath binds a tracker to an explicit session file (reported by
// herdr's agent integration) — an exact match, no locating or claims.
func AttachPath(harnessName, path, home string) Tracker {
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if _, err := os.Stat(path); err != nil {
		return nil // herdr can report a session file that doesn't exist (yet/anymore)
	}
	switch harnessName {
	case "claude":
		return newClaudeTracker(path)
	case "pi":
		return newPiTracker(path, home)
	}
	return nil
}

// staleness window: a session file this old isn't the live session
const freshWindow = 15 * time.Minute

// ── claude ───────────────────────────────────────────────────────────

var nonAlnum = regexp.MustCompile(`[^a-zA-Z0-9]`)

// claudeProjectDir munges a cwd the way Claude Code names its project dirs
// (every non-alphanumeric byte becomes "-").
func claudeProjectDir(home, cwd string) string {
	return filepath.Join(home, ".claude", "projects", nonAlnum.ReplaceAllString(cwd, "-"))
}

func locateClaude(home, cwd string, skip map[string]bool) string {
	dir := claudeProjectDir(home, cwd)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var best string
	var bestMod time.Time
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if info.ModTime().After(bestMod) && !skip[p] {
			bestMod = info.ModTime()
			best = p
		}
	}
	if best == "" || time.Since(bestMod) > freshWindow {
		return ""
	}
	return best
}

type claudeTracker struct {
	path    string
	offset  int64
	partial []byte // trailing incomplete line carried between reads
	usage   Usage
	model   string
	seen    map[string]bool // message-id dedupe (streamed updates repeat ids)
	seenQ   []string
}

func newClaudeTracker(path string) *claudeTracker {
	t := &claudeTracker{path: path, seen: map[string]bool{}}
	// attach at EOF: history is never parsed; runs measure forward deltas
	if info, err := os.Stat(path); err == nil {
		t.offset = info.Size()
	}
	return t
}

// claudeLine decodes only the structural fields we need.
type claudeLine struct {
	Type    string `json:"type"`
	Message struct {
		ID    string `json:"id"`
		Model string `json:"model"`
		Usage struct {
			In         int64 `json:"input_tokens"`
			CacheWrite int64 `json:"cache_creation_input_tokens"`
			CacheRead  int64 `json:"cache_read_input_tokens"`
			Out        int64 `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

func (t *claudeTracker) Refresh() {
	f, err := os.Open(t.path)
	if err != nil {
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() <= t.offset {
		return
	}
	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, 32*1024*1024)) // bound one read
	if err != nil {
		return
	}
	t.offset += int64(len(data))
	data = append(t.partial, data...)
	lines := bytes.Split(data, []byte{'\n'})
	t.partial = append([]byte(nil), lines[len(lines)-1]...) // may be incomplete
	for _, line := range lines[:len(lines)-1] {
		if len(line) == 0 {
			continue
		}
		var cl claudeLine
		if json.Unmarshal(line, &cl) != nil || cl.Type != "assistant" {
			continue
		}
		if cl.Message.Model != "" {
			t.model = cl.Message.Model
		}
		id := cl.Message.ID
		if id != "" {
			if t.seen[id] {
				continue
			}
			t.seen[id] = true
			t.seenQ = append(t.seenQ, id)
			if len(t.seenQ) > 512 {
				delete(t.seen, t.seenQ[0])
				t.seenQ = t.seenQ[1:]
			}
		}
		u := cl.Message.Usage
		t.usage.In += u.In
		t.usage.Out += u.Out
		t.usage.CacheRead += u.CacheRead
		t.usage.CacheWrite += u.CacheWrite
	}
}

func (t *claudeTracker) Usage() Usage  { return t.usage }
func (t *claudeTracker) Model() string { return t.model }
func (t *claudeTracker) Path() string  { return t.path }

// Session: claude attaches at EOF, so only post-attach counters are known.
func (t *claudeTracker) Session() SessionStats {
	return SessionStats{In: t.usage.In, Out: t.usage.Out, CacheRead: t.usage.CacheRead, CacheWrite: t.usage.CacheWrite}
}

// ── pi ───────────────────────────────────────────────────────────────

// Pi sessions live in ~/.pi/agent/sessions/<munged-cwd>/<ts>_<uuid>.jsonl.
// The munging is not relied on: the newest fresh file of every session dir
// is probed for a first-line cwd match instead.
func locatePi(home, cwd string, skip map[string]bool) string {
	base := filepath.Join(home, ".pi", "agent", "sessions")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	type cand struct {
		path string
		mod  time.Time
	}
	var fresh []cand
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(base, e.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		var best string
		var bestMod time.Time
		for _, fe := range files {
			if fe.IsDir() || filepath.Ext(fe.Name()) != ".jsonl" {
				continue
			}
			info, err := fe.Info()
			if err != nil {
				continue
			}
			if info.ModTime().After(bestMod) {
				bestMod = info.ModTime()
				best = filepath.Join(dir, fe.Name())
			}
		}
		if best != "" && !skip[best] && time.Since(bestMod) <= freshWindow {
			fresh = append(fresh, cand{best, bestMod})
		}
	}
	sort.Slice(fresh, func(i, j int) bool { return fresh[i].mod.After(fresh[j].mod) })
	// newest fresh session whose first-line cwd matches (cap the probes)
	for i, c := range fresh {
		if i >= 8 {
			break
		}
		if piSessionCwd(c.path) == cwd {
			return c.path
		}
	}
	return ""
}

// piSessionCwd reads only the first line's cwd.
func piSessionCwd(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256*1024)
	line, err := r.ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return ""
	}
	var meta struct {
		Type string `json:"type"`
		Cwd  string `json:"cwd"`
	}
	if json.Unmarshal(line, &meta) != nil || meta.Type != "session" {
		return ""
	}
	return meta.Cwd
}

type piTracker struct {
	home    string
	path    string
	offset  int64
	partial []byte // trailing incomplete line carried between reads
	usage   Usage  // forward counters since attach (run deltas)
	sess    SessionStats
	model   string
	seen    map[string]bool // message-id dedupe
	seenQ   []string
}

func newPiTracker(path, home string) *piTracker {
	t := &piTracker{path: path, home: home, seen: map[string]bool{}}
	t.seed()
	return t
}

// piUsage decodes only the structural fields we need.
type piUsage struct {
	In         int64 `json:"input"`
	Out        int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Total      int64 `json:"totalTokens"`
	Cost       struct {
		Total float64 `json:"total"`
	} `json:"cost"`
}

// piLine decodes only the structural fields we need.
type piLine struct {
	Type     string   `json:"type"`
	ID       string   `json:"id"`
	ModelID  string   `json:"modelId"`  // model_change
	Provider string   `json:"provider"` // model_change
	Usage    *piUsage `json:"usage"`    // compaction / branch_summary
	Message  struct {
		Role  string   `json:"role"`
		Model string   `json:"model"`
		Usage *piUsage `json:"usage"`
	} `json:"message"`
}

// seed parses the whole session file once: session-cumulative stats (like
// the pi footer's totals) come from history; run deltas stay forward-only.
func (t *piTracker) seed() {
	f, err := os.Open(t.path)
	if err != nil {
		return
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 64*1024*1024)) // bound one read
	if err != nil {
		return
	}
	lines := bytes.Split(data, []byte{'\n'})
	for _, line := range lines[:len(lines)-1] {
		t.apply(line, false)
	}
	// runs measure forward deltas from here
	info, err := f.Stat()
	if err == nil {
		t.offset = info.Size()
	}
}

// apply folds one session line into the session stats, and into the forward
// counters too when count is true (post-attach lines).
func (t *piTracker) apply(line []byte, count bool) {
	if len(line) == 0 {
		return
	}
	var u *piUsage
	var role, msgModel, msgID string
	switch {
	case bytes.Contains(line, []byte(`"model_change"`)):
		var pl piLine
		if json.Unmarshal(line, &pl) == nil && pl.ModelID != "" {
			t.model = pl.ModelID
			if pl.Provider != "" {
				t.sess.ContextWindow = piContextWindow(t.home, pl.Provider, pl.ModelID)
			}
		}
		return
	case bytes.Contains(line, []byte(`"usage"`)):
		var pl piLine
		if json.Unmarshal(line, &pl) != nil {
			return
		}
		switch pl.Type {
		case "message":
			u, role, msgModel, msgID = pl.Message.Usage, pl.Message.Role, pl.Message.Model, pl.ID
		case "compaction", "branch_summary":
			u = pl.Usage
		}
	default:
		return
	}
	if u == nil {
		return
	}
	if msgModel != "" {
		t.model = msgModel
	}
	if msgID != "" { // dedupe streamed updates (same message id repeats)
		if t.seen[msgID] {
			return
		}
		t.seen[msgID] = true
		t.seenQ = append(t.seenQ, msgID)
		if len(t.seenQ) > 2048 { // seeded history can be long
			delete(t.seen, t.seenQ[0])
			t.seenQ = t.seenQ[1:]
		}
	}
	if count {
		t.usage.In += u.In
		t.usage.Out += u.Out
		t.usage.CacheRead += u.CacheRead
		t.usage.CacheWrite += u.CacheWrite
	}
	t.sess.In += u.In
	t.sess.Out += u.Out
	t.sess.CacheRead += u.CacheRead
	t.sess.CacheWrite += u.CacheWrite
	t.sess.CostUSD += u.Cost.Total
	if role == "assistant" { // pi footer: hit rate and context track the LATEST request
		if prompt := u.In + u.CacheRead + u.CacheWrite; prompt > 0 {
			t.sess.CacheHit = 100 * float64(u.CacheRead) / float64(prompt)
		}
		if total := u.Total; total > 0 {
			t.sess.ContextTokens = total
		} else {
			t.sess.ContextTokens = u.In + u.Out + u.CacheRead + u.CacheWrite
		}
	}
}

// piContextWindow resolves a model's context window from pi's own
// models-store.json (provider -> models[] -> id -> contextWindow).
func piContextWindow(home, provider, modelID string) int64 {
	data, err := os.ReadFile(filepath.Join(home, ".pi", "agent", "models-store.json"))
	if err != nil {
		return 0
	}
	var store map[string]json.RawMessage
	if json.Unmarshal(data, &store) != nil {
		return 0
	}
	raw, ok := store[provider]
	if !ok {
		return 0
	}
	var found int64
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if id, _ := x["id"].(string); id == modelID {
				if cw, ok := x["contextWindow"].(float64); ok {
					found = int64(cw)
				}
			}
			for _, v2 := range x {
				walk(v2)
			}
		case []any:
			for _, v2 := range x {
				walk(v2)
			}
		}
	}
	var v any
	if json.Unmarshal(raw, &v) == nil {
		walk(v)
	}
	return found
}

func (t *piTracker) Refresh() {
	f, err := os.Open(t.path)
	if err != nil {
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() <= t.offset {
		return
	}
	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, 32*1024*1024)) // bound one read
	if err != nil {
		return
	}
	t.offset += int64(len(data))
	data = append(t.partial, data...)
	lines := bytes.Split(data, []byte{'\n'})
	t.partial = append([]byte(nil), lines[len(lines)-1]...) // may be incomplete
	for _, line := range lines[:len(lines)-1] {
		t.apply(line, true)
	}
}

func (t *piTracker) Usage() Usage          { return t.usage }
func (t *piTracker) Session() SessionStats { return t.sess }
func (t *piTracker) Model() string         { return t.model }
func (t *piTracker) Path() string          { return t.path }
