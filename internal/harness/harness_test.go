package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
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

func TestCodexTrackerDeltasFromAttach(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "rollout.jsonl")
	meta := `{"type":"session_meta","payload":{"cwd":"/work/proj"}}`
	tc := `{"type":"turn_context","payload":{"model":"gpt-5.5","effort":"medium"}}`
	count := func(in, cached, out int64) string {
		return fmt.Sprintf(`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":0,"total_tokens":%d}}}}`, in, cached, out, in+out)
	}
	os.WriteFile(p, []byte(meta+"\n"+tc+"\n"+count(1000, 400, 500)+"\n"), 0o644)

	if cwd := codexSessionCwd(p); cwd != "/work/proj" {
		t.Fatalf("session cwd: %q", cwd)
	}
	tr := newCodexTracker(p)
	if !tr.Usage().IsZero() {
		t.Fatalf("cumulative history counted at attach: %+v", tr.Usage())
	}
	if tr.Model() != "gpt-5.5" {
		t.Fatalf("model: %q", tr.Model())
	}
	// progress: cumulative grows
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(count(1500, 600, 800) + "\n")
	f.Close()
	tr.Refresh()
	u := tr.Usage()
	if u.In != 500 || u.Out != 300 || u.CacheRead != 200 {
		t.Fatalf("delta wrong: %+v", u)
	}
}

func TestUsageSubClamp(t *testing.T) {
	a := Usage{In: 5, Out: 5}
	b := Usage{In: 10, Out: 10}
	if d := a.Sub(b); !d.IsZero() {
		t.Fatalf("regressed counters must clamp to zero: %+v", d)
	}
}
