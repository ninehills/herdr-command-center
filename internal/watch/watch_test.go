package watch

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		err  error
	}{
		{"token after URL", []string{"ws://localhost/events", "--token", "x"}, nil},
		{"token before URL", []string{"--token", "x", "wss://localhost/events"}, nil},
		{"token equals", []string{"--token=x", "ws://localhost/events"}, nil},
		{"missing URL", []string{"--token", "x"}, errors.New("bad")},
		{"missing token", []string{"ws://localhost/events"}, errors.New("bad")},
		{"multiple URLs", []string{"ws://a/x", "ws://b/x", "--token=x"}, errors.New("bad")},
		{"scheme", []string{"http://localhost/events", "--token=x"}, errors.New("bad")},
		{"help", []string{"--help"}, flag.ErrHelp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseArgs(tt.args)
			if tt.err == nil && (err != nil || opts.Token != "x") {
				t.Fatalf("options=%+v err=%v", opts, err)
			}
			if tt.err != nil && err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(tt.err, flag.ErrHelp) && !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected help, got %v", err)
			}
		})
	}
}

func TestConnectLoopStopsWhenUpdateQueueIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	updates := make(chan update, 1)
	updates <- update{}
	done := make(chan struct{})
	go func() {
		_ = connectLoop(ctx, options{URL: "ws://localhost/events", Token: "x"}, updates)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connectLoop remained blocked after cancellation")
	}
}

func TestStateDeleteAndRenderDoesNotLeakToken(t *testing.T) {
	agents := map[string]agent{}
	applyMessage(agents, message{Type: "agent.added", AgentID: "p:a", Agent: agent{Name: "claude", Status: "working"}})
	if len(agents) != 1 {
		t.Fatal("agent was not added")
	}
	applyMessage(agents, message{Type: "agent.deleted", AgentID: "p:a"})
	if len(agents) != 0 {
		t.Fatal("agent was not deleted")
	}
	var out bytes.Buffer
	events := []string{formatEvent(message{Type: "agent.updated", Timestamp: "2026-07-25T08:22:09Z", AgentID: "p:a", SourceKind: "agent.status_changed", Agent: agent{Status: "working\x1b[2J", PreviousStatus: "idle"}})}
	render(&out, "ws://localhost/events", "connected", time.Now(), map[string]agent{"p:a": {Name: "claude\x1b]52;c;stolen\a", Status: "working\x1b[2J", WorkspaceLabel: "bad\u0080label"}}, events)
	if strings.ContainsAny(out.String(), "\a\u0080") || strings.Contains(out.String(), "claude\x1b") || strings.Contains(out.String(), "working\x1b[2J") {
		t.Fatalf("render contains untrusted control characters: %q", out.String())
	}
	if strings.Contains(out.String(), "secret-token") || !strings.Contains(out.String(), "claude") {
		t.Fatalf("unexpected render: %q", out.String())
	}
	if !strings.Contains(out.String(), "EVENTS") || !strings.Contains(out.String(), "status=idle->working") || !strings.Contains(out.String(), "[agent.status_changed]") {
		t.Fatalf("event log missing from render: %q", out.String())
	}
}

func TestAppendEventRingBuffer(t *testing.T) {
	var events []string
	for i := 0; i < maxEventLog+50; i++ {
		events = appendEvent(events, fmt.Sprintf("event-%d", i))
	}
	if len(events) != maxEventLog || events[0] != "event-50" || events[len(events)-1] != fmt.Sprintf("event-%d", maxEventLog+49) {
		t.Fatalf("ring buffer wrong: len=%d first=%q last=%q", len(events), events[0], events[len(events)-1])
	}
}
