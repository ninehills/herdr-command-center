package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClaudeProjectDirMunge(t *testing.T) {
	got := claudeProjectDir("/home/u", "/Users/x/code/github/diodide/PortfolioBuilder")
	want := filepath.Join("/home/u", ".claude", "projects", "-Users-x-code-github-diodide-PortfolioBuilder")
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	got = claudeProjectDir("/home/u", "/a/b.c/d_e")
	if filepath.Base(got) != "-a-b-c-d-e" {
		t.Fatalf("dots/underscores should munge: %q", got)
	}
}

func TestClaudeTrackerCountsForwardOnly(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	line := func(id string, in, out int64) string {
		return fmt.Sprintf(`{"type":"assistant","message":{"id":%q,"model":"claude-fable-5","usage":{"input_tokens":%d,"cache_creation_input_tokens":5,"cache_read_input_tokens":100,"output_tokens":%d}}}`, id, in, out)
	}
	// history that must NOT be counted (attach at EOF)
	os.WriteFile(p, []byte(line("old", 999, 999)+"\n"), 0o644)
	tr := newClaudeTracker(p)
	tr.Refresh()
	if !tr.Usage().IsZero() {
		t.Fatalf("history counted: %+v", tr.Usage())
	}

	// append two messages + a duplicate id + a partial line
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(line("m1", 10, 20) + "\n")
	f.WriteString(line("m1", 10, 20) + "\n") // dup id — ignored
	f.WriteString(line("m2", 1, 2) + "\n")
	f.WriteString(`{"type":"assistant","message":{"id":"m3"`) // incomplete
	f.Close()
	tr.Refresh()
	u := tr.Usage()
	if u.In != 11 || u.Out != 22 || u.CacheRead != 200 || u.CacheWrite != 10 {
		t.Fatalf("unexpected usage: %+v", u)
	}
	if tr.Model() != "claude-fable-5" {
		t.Fatalf("model: %q", tr.Model())
	}

	// complete the partial line — it must be counted exactly once
	f, _ = os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`,"model":"claude-fable-5","usage":{"input_tokens":7,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":3}}}` + "\n")
	f.Close()
	tr.Refresh()
	u = tr.Usage()
	if u.In != 18 || u.Out != 25 {
		t.Fatalf("partial-line handling wrong: %+v", u)
	}
}

func TestPiContextWindowLookup(t *testing.T) {
	home := t.TempDir()
	os.MkdirAll(filepath.Join(home, ".pi", "agent"), 0o755)
	store := `{"kimi-coding":{"models":[{"id":"k3","contextWindow":1048576},{"id":"k2","contextWindow":262144}]}}`
	os.WriteFile(filepath.Join(home, ".pi", "agent", "models-store.json"), []byte(store), 0o644)
	if got := piContextWindow(home, "kimi-coding", "k3"); got != 1048576 {
		t.Fatalf("contextWindow: %d", got)
	}
	if got := piContextWindow(home, "kimi-coding", "nope"); got != 0 {
		t.Fatalf("unknown model must be 0: %d", got)
	}
	if got := piContextWindow(home, "no-provider", "k3"); got != 0 {
		t.Fatalf("unknown provider must be 0: %d", got)
	}
}

func TestPiSessionStats(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	os.WriteFile(p, []byte(`{"type":"session","version":3,"id":"x","cwd":"/w"}`+"\n"+`{"type":"model_change","id":"mc","modelId":"k3","provider":"kimi-coding"}`+"\n"), 0o644)
	tr := newPiTracker(p, dir)
	msg := func(id string, in, out, cr int64, cost float64) string {
		return fmt.Sprintf(`{"type":"message","id":%q,"message":{"role":"assistant","model":"k3","usage":{"input":%d,"output":%d,"cacheRead":%d,"cacheWrite":0,"totalTokens":%d,"cost":{"total":%v}}}}`, id, in, out, cr, in+out+cr, cost)
	}
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(msg("m1", 1000, 200, 9000, 0.05) + "\n")
	f.WriteString(msg("m2", 500, 100, 9500, 0.03) + "\n")
	f.Close()
	tr.Refresh()
	s := tr.Session()
	if s.In != 1500 || s.Out != 300 || s.CacheRead != 18500 {
		t.Fatalf("session totals: %+v", s)
	}
	// CH tracks the LATEST request: 9500/(500+9500) = 95%%
	if s.CacheHit < 94.9 || s.CacheHit > 95.1 {
		t.Fatalf("cache hit: %v", s.CacheHit)
	}
	if s.ContextTokens != 10100 {
		t.Fatalf("context tokens: %d", s.ContextTokens)
	}
	if s.CostUSD < 0.079 || s.CostUSD > 0.081 {
		t.Fatalf("cost: %v", s.CostUSD)
	}
}

func TestUsageSubClamp(t *testing.T) {
	a := Usage{In: 5, Out: 5}
	b := Usage{In: 10, Out: 10}
	if d := a.Sub(b); !d.IsZero() {
		t.Fatalf("regressed counters must clamp to zero: %+v", d)
	}
}

func TestPiLocateAndSessionCwd(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".pi", "agent", "sessions", "--home-u-code--")
	os.MkdirAll(dir, 0o755)
	good := filepath.Join(dir, "s1.jsonl")
	os.WriteFile(good, []byte(`{"type":"session","version":3,"id":"x","cwd":"/home/u/code"}`+"\n"), 0o644)
	// a newer session in another dir whose cwd does not match — s1 must win
	dir2 := filepath.Join(home, ".pi", "agent", "sessions", "--elsewhere--")
	os.MkdirAll(dir2, 0o755)
	other := filepath.Join(dir2, "s2.jsonl")
	os.WriteFile(other, []byte(`{"type":"session","version":3,"id":"y","cwd":"/elsewhere"}`+"\n"), 0o644)
	os.Chtimes(other, time.Now(), time.Now())
	if got := locatePi(home, "/home/u/code", nil); got != good {
		t.Fatalf("locatePi got %q want %q", got, good)
	}
	if got := locatePi(home, "/nobody", nil); got != "" {
		t.Fatalf("locatePi should miss, got %q", got)
	}
}

func TestPiTrackerCountsForwardOnly(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	msg := func(id string, in, out, cr, cw int64) string {
		return fmt.Sprintf(`{"type":"message","id":%q,"message":{"role":"assistant","model":"k3","usage":{"input":%d,"output":%d,"cacheRead":%d,"cacheWrite":%d}}}`, id, in, out, cr, cw)
	}
	// history: model must be seeded, counters must NOT be counted
	os.WriteFile(p, []byte(`{"type":"session","version":3,"id":"x","cwd":"/home/u/code"}`+"\n"+
		`{"type":"model_change","id":"mc","modelId":"k3"}`+"\n"+
		msg("old", 999, 999, 999, 999)+"\n"), 0o644)
	tr := newPiTracker(p, dir)
	if tr.Model() != "k3" {
		t.Fatalf("model not seeded from history: %q", tr.Model())
	}
	tr.Refresh()
	if !tr.Usage().IsZero() {
		t.Fatalf("history counted: %+v", tr.Usage())
	}
	// session stats DO include history (pi-footer parity)
	if s := tr.Session(); s.In != 999 || s.CacheRead != 999 || s.CacheHit <= 0 {
		t.Fatalf("session not seeded from history: %+v", s)
	}

	// append two messages + a duplicate id + a partial line
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(msg("m1", 10, 20, 30, 5) + "\n")
	f.WriteString(msg("m1", 10, 20, 30, 5) + "\n") // dup id — ignored
	f.WriteString(msg("m2", 1, 2, 3, 0) + "\n")
	f.WriteString(`{"type":"message","id":"m3"`) // incomplete
	f.Close()
	tr.Refresh()
	u := tr.Usage()
	if u.In != 11 || u.Out != 22 || u.CacheRead != 33 || u.CacheWrite != 5 {
		t.Fatalf("unexpected usage: %+v", u)
	}

	// complete the partial line — counted exactly once
	f, _ = os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`,"message":{"role":"assistant","model":"k3","usage":{"input":7,"output":3,"cacheRead":0,"cacheWrite":0}}}` + "\n")
	f.Close()
	tr.Refresh()
	u = tr.Usage()
	if u.In != 18 || u.Out != 25 {
		t.Fatalf("partial-line handling wrong: %+v", u)
	}
}
