// Package harness extracts token COUNTERS and the model id from coding-agent
// session files (Claude Code, Codex). This is the one collector that touches
// harness-owned files, so its contract is strict:
//
//   - only structural fields are decoded: usage counters, model id, cwd.
//     Message content is never inspected, stored, or forwarded.
//   - Claude readers attach at the CURRENT END of the session file and count
//     forward — historical lines are never parsed, and per-run deltas (the
//     only thing telemetry reports) are exact either way.
//   - every read is bounded: Codex reads a fixed-size tail; Claude reads only
//     bytes appended since the last poll.
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

// Tracker follows one agent's session file.
type Tracker interface {
	// Refresh advances the counters; cheap enough for every poll tick.
	Refresh()
	// Usage returns the current counter snapshot.
	Usage() Usage
	// Model returns the last-seen model id ("" if unknown yet).
	Model() string
}

// Attach locates the live session file for a harness+cwd and returns a
// tracker, or nil if no fresh session file could be found.
func Attach(harnessName, cwd string, home string) Tracker {
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	switch harnessName {
	case "claude":
		if p := locateClaude(home, cwd); p != "" {
			return newClaudeTracker(p)
		}
	case "codex":
		if p := locateCodex(home, cwd); p != "" {
			return newCodexTracker(p)
		}
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

func locateClaude(home, cwd string) string {
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
		if info.ModTime().After(bestMod) {
			bestMod = info.ModTime()
			best = filepath.Join(dir, e.Name())
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

// ── codex ────────────────────────────────────────────────────────────

func locateCodex(home, cwd string) string {
	base := filepath.Join(home, ".codex", "sessions")
	// today and yesterday (rollouts are date-bucketed local time)
	days := []time.Time{time.Now(), time.Now().AddDate(0, 0, -1)}
	var candidates []string
	for _, d := range days {
		dir := filepath.Join(base, d.Format("2006"), d.Format("01"), d.Format("02"))
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".jsonl" {
				candidates = append(candidates, filepath.Join(dir, e.Name()))
			}
		}
	}
	type cand struct {
		path string
		mod  time.Time
	}
	var fresh []cand
	for _, p := range candidates {
		if info, err := os.Stat(p); err == nil && time.Since(info.ModTime()) <= freshWindow {
			fresh = append(fresh, cand{p, info.ModTime()})
		}
	}
	sort.Slice(fresh, func(i, j int) bool { return fresh[i].mod.After(fresh[j].mod) })
	// newest fresh rollout whose session_meta cwd matches (cap the probes)
	for i, c := range fresh {
		if i >= 8 {
			break
		}
		if codexSessionCwd(c.path) == cwd {
			return c.path
		}
	}
	return ""
}

// codexSessionCwd reads only the first line's session_meta.payload.cwd.
func codexSessionCwd(path string) string {
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
		Type    string `json:"type"`
		Payload struct {
			Cwd string `json:"cwd"`
		} `json:"payload"`
	}
	if json.Unmarshal(line, &meta) != nil || meta.Type != "session_meta" {
		return ""
	}
	return meta.Payload.Cwd
}

type codexTracker struct {
	path  string
	base  Usage // cumulative counters at attach time (run deltas start here)
	cur   Usage
	model string
	first bool
}

func newCodexTracker(path string) *codexTracker {
	t := &codexTracker{path: path, first: true}
	t.Refresh()
	return t
}

const codexTailBytes = 256 * 1024

func (t *codexTracker) Refresh() {
	f, err := os.Open(t.path)
	if err != nil {
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return
	}
	start := info.Size() - codexTailBytes
	if start < 0 {
		start = 0
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, codexTailBytes+1))
	if err != nil {
		return
	}
	lines := bytes.Split(data, []byte{'\n'})
	if start > 0 && len(lines) > 1 {
		lines = lines[1:] // first tail line is likely partial
	}
	var latest Usage
	found := false
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		// cheap pre-filters keep full JSON decoding off most lines
		switch {
		case bytes.Contains(line, []byte(`"token_count"`)):
			var ev struct {
				Payload struct {
					Info struct {
						Total struct {
							In     int64 `json:"input_tokens"`
							Cached int64 `json:"cached_input_tokens"`
							Out    int64 `json:"output_tokens"`
						} `json:"total_token_usage"`
					} `json:"info"`
				} `json:"payload"`
			}
			if json.Unmarshal(line, &ev) == nil {
				latest = Usage{
					In:        ev.Payload.Info.Total.In,
					Out:       ev.Payload.Info.Total.Out,
					CacheRead: ev.Payload.Info.Total.Cached,
				}
				found = true
			}
		case bytes.Contains(line, []byte(`"turn_context"`)):
			var tc struct {
				Payload struct {
					Model string `json:"model"`
				} `json:"payload"`
			}
			if json.Unmarshal(line, &tc) == nil && tc.Payload.Model != "" {
				t.model = tc.Payload.Model
			}
		}
	}
	if found {
		if t.first {
			// counters are cumulative for the whole session — everything
			// before attach belongs to history we don't report
			t.base = latest
			t.first = false
		}
		t.cur = latest
	}
}

func (t *codexTracker) Usage() Usage  { return t.cur.Sub(t.base) }
func (t *codexTracker) Model() string { return t.model }
