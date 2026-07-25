package harness

import (
	"os"
	"testing"
	"time"
)

// TestLiveAttach is a manual live check against real session files on this
// machine. Guarded by an env var so CI never runs it.
func TestLiveAttach(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_CHECK") == "" {
		t.Skip("set HARNESS_LIVE_CHECK=1 to run against live session files")
	}
	tr := Attach("claude", os.Getenv("LIVE_CWD"), "", nil)
	if tr == nil {
		t.Fatal("claude: no fresh session found")
	}
	tr.Refresh()
	t.Logf("claude attach: usage=%+v model=%q", tr.Usage(), tr.Model())
	time.Sleep(25 * time.Second)
	tr.Refresh()
	t.Logf("claude +25s:   usage=%+v model=%q", tr.Usage(), tr.Model())
}
