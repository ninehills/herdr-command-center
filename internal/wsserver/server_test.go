package wsserver

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ninehills/herdr-command-center/internal/collector"
	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/harness"
)

func TestWebSocketAuthSnapshotAndDelete(t *testing.T) {
	if _, err := New(config.WebSocket{Enabled: true, Listen: "127.0.0.1:0", Path: "/events"}); err == nil {
		t.Fatal("enabled server accepted an empty token")
	}
	s, err := New(config.WebSocket{Enabled: true, Listen: "127.0.0.1:0", Path: "/events", AuthToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	url := "ws://" + s.Addr() + "/events"
	if resp, err := http.Get("http://" + s.Addr() + "/wrong"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong path: err=%v response=%v", err, resp)
	} else {
		resp.Body.Close()
	}

	if _, resp, err := websocket.Dial(ctx, url, nil); err == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token: err=%v response=%v", err, resp)
	}
	bad := http.Header{"Authorization": {"Bearer wrong"}}
	if _, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: bad}); err == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token: err=%v response=%v", err, resp)
	}

	s.Publish(collector.Event{V: 1, Kind: "agent.seen", TS: time.Now().Format(time.RFC3339Nano), PaneID: "p1", Harness: "claude", Status: "working"})
	time.Sleep(10 * time.Millisecond)
	good := http.Header{"Authorization": {"Bearer secret"}}
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: good})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var msg Message
	if err := readJSON(ctx, conn, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "agent.added" || msg.AgentID != "p1:claude" || msg.Agent.Status != "working" {
		t.Fatalf("unexpected snapshot: %+v", msg)
	}

	s.Publish(collector.Event{V: 1, Kind: "agent.gone", TS: time.Now().Format(time.RFC3339Nano), PaneID: "p1", Harness: "claude", Status: "unknown"})
	if err := readJSON(ctx, conn, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "agent.deleted" {
		t.Fatalf("unexpected delete: %+v", msg)
	}
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := s.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
	defer readCancel()
	if _, _, err := conn.Read(readCtx); websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("expected normal server closure, got %v", err)
	}
}

func TestPublishDoesNotBlockOrLoseStateWhenHubIsFull(t *testing.T) {
	s, err := New(config.WebSocket{Enabled: true, Listen: "127.0.0.1:0", Path: "/events", AuthToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for i := range cap(s.publish) + 1 {
		s.Publish(collector.Event{Kind: "agent.seen", PaneID: fmt.Sprintf("p%d", i), Harness: "codex"})
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("Publish blocked on a full hub")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(ctx, "ws://"+s.Addr()+"/events", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	want := fmt.Sprintf("p%d:codex", cap(s.publish))
	for range cap(s.publish) + 1 {
		var msg Message
		if err := readJSON(ctx, conn, &msg); err != nil {
			t.Fatal(err)
		}
		if msg.AgentID == want {
			return
		}
	}
	t.Fatalf("snapshot lost latest agent %s", want)
}

func TestSnapshotLargerThanClientBuffer(t *testing.T) {
	s, _ := New(config.WebSocket{Enabled: true, Listen: "127.0.0.1:0", Path: "/events", AuthToken: "secret"})
	for i := range 65 {
		s.Publish(collector.Event{Kind: "agent.seen", PaneID: fmt.Sprintf("p%d", i), Harness: "codex"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(ctx, "ws://"+s.Addr()+"/events", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	for range 65 {
		var msg Message
		if err := readJSON(ctx, conn, &msg); err != nil {
			t.Fatal(err)
		}
	}
}

func TestApplyMergesAndDoesNotResurrectDeletedAgent(t *testing.T) {
	agents := map[string]Agent{}
	added, ok := apply(agents, collector.Event{Kind: "agent.seen", PaneID: "p", Harness: "codex", Status: "working"})
	if !ok || added.Type != "agent.added" {
		t.Fatalf("unexpected add: %+v", added)
	}
	updated, ok := apply(agents, collector.Event{Kind: "run.finished", PaneID: "p", Harness: "codex", RunDurationMS: 5, TokensIn: 12, TokensOut: 6, TokensCacheR: 40, TokensCacheW: 3})
	if !ok || updated.Type != "agent.updated" || updated.Agent.TokensIn != 12 || updated.Agent.TokensCacheR != 40 || updated.Agent.TokensCacheW != 3 {
		t.Fatalf("unexpected update: %+v", updated)
	}
	started, ok := apply(agents, collector.Event{Kind: "run.started", PaneID: "p", Harness: "codex"})
	if !ok || started.Agent.RunDurationMS != 0 || started.Agent.TokensIn != 0 || started.Agent.TokensOut != 0 || started.Agent.TokensCacheR != 0 || started.Agent.TokensCacheW != 0 {
		t.Fatalf("run start retained previous metrics: %+v", started)
	}
	finished, ok := apply(agents, collector.Event{Kind: "run.finished", PaneID: "p", Harness: "codex"})
	if !ok || finished.Agent.RunDurationMS != 0 || finished.Agent.TokensIn != 0 || finished.Agent.TokensOut != 0 {
		t.Fatalf("zero run metrics were not applied: %+v", finished)
	}
	deleted, _ := apply(agents, collector.Event{Kind: "agent.gone", PaneID: "p", Harness: "codex"})
	if deleted.Type != "agent.deleted" || deleted.Agent.Status != "unknown" {
		t.Fatalf("unexpected delete: %+v", deleted)
	}
	if _, ok := apply(agents, collector.Event{Kind: "run.finished", PaneID: "p", Harness: "codex"}); ok {
		t.Fatal("run event resurrected deleted agent")
	}
}

func readJSON(ctx context.Context, conn *websocket.Conn, dst any) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return wsjson.Read(ctx, conn, dst)
}

func TestApplyRunUsage(t *testing.T) {
	agents := map[string]Agent{}
	apply(agents, collector.Event{Kind: "agent.seen", PaneID: "p", Harness: "codex", Status: "working"})
	apply(agents, collector.Event{Kind: "run.started", PaneID: "p", Harness: "codex", Model: "gpt-5.5"})

	// live delta during the run: tokens grow, duration advances
	live, ok := apply(agents, collector.Event{Kind: "run.usage", PaneID: "p", Harness: "codex", Model: "gpt-5.5", RunDurationMS: 8000, TokensIn: 100, TokensOut: 20, TokensCacheR: 300})
	if !ok || live.Type != "agent.updated" || live.Agent.TokensIn != 100 || live.Agent.TokensCacheR != 300 || live.Agent.RunDurationMS != 8000 {
		t.Fatalf("live run.usage not applied: %+v", live)
	}

	apply(agents, collector.Event{Kind: "run.finished", PaneID: "p", Harness: "codex", RunDurationMS: 9000, TokensIn: 120, TokensOut: 25, TokensCacheR: 320})

	// trailing delta after a late session-file flush: tokens update, the
	// finished duration is kept (trailing events carry no duration)
	trailing, ok := apply(agents, collector.Event{Kind: "run.usage", PaneID: "p", Harness: "codex", TokensIn: 150, TokensOut: 30, TokensCacheR: 350})
	if !ok || trailing.Agent.TokensIn != 150 || trailing.Agent.TokensCacheR != 350 {
		t.Fatalf("trailing run.usage not applied: %+v", trailing)
	}
	if trailing.Agent.RunDurationMS != 9000 {
		t.Fatalf("trailing run.usage wiped duration: %+v", trailing.Agent)
	}

	// run.usage for an unknown agent is dropped like other run.* events
	if _, ok := apply(agents, collector.Event{Kind: "run.usage", PaneID: "ghost", Harness: "pi", TokensIn: 1}); ok {
		t.Fatal("run.usage resurrected an unknown agent")
	}
}

func TestApplyRunUsageSession(t *testing.T) {
	agents := map[string]Agent{}
	apply(agents, collector.Event{Kind: "agent.seen", PaneID: "p", Harness: "pi", Status: "working"})
	stats := harness.SessionStats{In: 323000, Out: 61000, CacheRead: 6800000, CostUSD: 3.913, CacheHit: 99.7, ContextTokens: 109000, ContextWindow: 1000000}
	msg, ok := apply(agents, collector.Event{Kind: "run.usage", PaneID: "p", Harness: "pi", TokensIn: 5, Session: &stats})
	if !ok || msg.Agent.Session == nil {
		t.Fatalf("session not merged: %+v", msg.Agent)
	}
	if msg.Agent.Session.CacheHit != 99.7 || msg.Agent.Session.ContextWindow != 1000000 || msg.Agent.Session.CostUSD != 3.913 {
		t.Fatalf("session fields wrong: %+v", msg.Agent.Session)
	}
	// a later event without session must keep the last one
	msg2, _ := apply(agents, collector.Event{Kind: "agent.status_changed", PaneID: "p", Harness: "pi", Status: "idle"})
	if msg2.Agent.Session == nil || msg2.Agent.Session.In != 323000 {
		t.Fatalf("session lost: %+v", msg2.Agent)
	}
}
