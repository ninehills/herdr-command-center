// Package wsserver exposes privacy-shaped collector agent events over WebSocket.
package wsserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/ninehills/herdr-command-center/internal/collector"
	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/harness"
)

type Agent struct {
	Name           string `json:"name"`
	Harness        string `json:"harness"`
	Status         string `json:"status"`
	PreviousStatus string `json:"previous_status,omitempty"`
	WorkspaceID    string `json:"workspace_id,omitempty"`
	WorkspaceLabel string `json:"workspace_label,omitempty"`
	TabID          string `json:"tab_id,omitempty"`
	TabLabel       string `json:"tab_label,omitempty"`
	PaneID         string `json:"pane_id,omitempty"`
	TerminalID     string `json:"terminal_id,omitempty"`
	Model          string `json:"model,omitempty"`
	Repo           string `json:"repo,omitempty"`
	Branch         string `json:"branch,omitempty"`
	RunStartedAt   string `json:"run_started_at,omitempty"`
	RunDurationMS  int64  `json:"run_duration_ms,omitempty"`
	TokensIn       int64  `json:"tokens_in,omitempty"`
	TokensOut      int64  `json:"tokens_out,omitempty"`
	TokensCacheR   int64  `json:"tokens_cache_read,omitempty"`
	TokensCacheW   int64  `json:"tokens_cache_write,omitempty"`
	// Session-cumulative stats for the live panel (pi-status-bar parity)
	Session *harness.SessionStats `json:"session,omitempty"`
}

type Message struct {
	V          int    `json:"v"`
	Type       string `json:"type"`
	Timestamp  string `json:"timestamp"`
	SourceKind string `json:"source_kind"`
	AgentID    string `json:"agent_id"`
	Agent      Agent  `json:"agent"`
}

type client struct{ send chan []byte }
type registration struct {
	client   *client
	snapshot chan [][]byte
}

type Server struct {
	cfg        config.WebSocket
	publish    chan []byte
	mu         sync.Mutex
	agents     map[string]Agent
	register   chan registration
	unregister chan *client
	ctx        context.Context
	cancel     context.CancelFunc
	http       *http.Server
	addr       string
	done       chan struct{}
}

func New(cfg config.WebSocket) (*Server, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.AuthToken == "" {
		return nil, fmt.Errorf("websocket.auth_token is required when websocket is enabled")
	}
	if cfg.Listen == "" || cfg.Path == "" || cfg.Path[0] != '/' {
		return nil, fmt.Errorf("invalid websocket listen/path configuration")
	}
	return &Server{cfg: cfg, publish: make(chan []byte, 256), agents: make(map[string]Agent), register: make(chan registration), unregister: make(chan *client), done: make(chan struct{})}, nil
}

func (s *Server) Start(parent context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("websocket listen %s: %w", s.cfg.Listen, err)
	}
	s.ctx, s.cancel = context.WithCancel(parent)
	s.addr = ln.Addr().String()
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.Path, s.handle)
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go s.run()
	go func() {
		if err := s.http.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("websocket server: %v", err)
		}
	}()
	return nil
}

func (s *Server) Addr() string { return s.addr }

// Publish preserves the latest state and never waits for a saturated broadcast queue.
func (s *Server) Publish(ev collector.Event) {
	if s == nil {
		return
	}
	s.mu.Lock()
	msg, ok := apply(s.agents, ev)
	s.mu.Unlock()
	if !ok {
		return
	}
	b, _ := json.Marshal(msg)
	select {
	case s.publish <- b:
	default:
	}
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil || s.cancel == nil {
		return nil
	}
	s.cancel()
	err := s.http.Shutdown(ctx)
	select {
	case <-s.done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}
	return err
}

func (s *Server) run() {
	defer close(s.done)
	clients := map[*client]struct{}{}
	for {
		select {
		case <-s.ctx.Done():
			for c := range clients {
				close(c.send)
			}
			return
		case r := <-s.register:
			clients[r.client] = struct{}{}
			r.snapshot <- s.snapshot()
		case c := <-s.unregister:
			if _, ok := clients[c]; ok {
				delete(clients, c)
				close(c.send)
			}
		case b := <-s.publish:
			for c := range clients {
				select {
				case c.send <- b:
				default:
					delete(clients, c)
					close(c.send)
				}
			}
		}
	}
}

func (s *Server) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]byte, 0, len(s.agents))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for id, agent := range s.agents {
		b, _ := json.Marshal(Message{V: 1, Type: "agent.added", Timestamp: now, SourceKind: "snapshot", AgentID: id, Agent: agent})
		result = append(result, b)
	}
	return result
}

func apply(agents map[string]Agent, ev collector.Event) (Message, bool) {
	if ev.PaneID == "" || ev.Harness == "" || !(strings.HasPrefix(ev.Kind, "agent.") || strings.HasPrefix(ev.Kind, "run.")) {
		return Message{}, false
	}
	id := ev.PaneID + ":" + ev.Harness
	a, exists := agents[id]
	// A trailing run.finished after agent.gone must not resurrect a deleted row.
	if !exists && strings.HasPrefix(ev.Kind, "run.") {
		return Message{}, false
	}
	if !exists {
		a = Agent{Name: ev.Harness, Harness: ev.Harness, Status: "unknown"}
	}
	merge := func(dst *string, value string) {
		if value != "" {
			*dst = value
		}
	}
	merge(&a.Status, ev.Status)
	merge(&a.PreviousStatus, ev.PrevStatus)
	merge(&a.WorkspaceID, ev.WorkspaceID)
	merge(&a.WorkspaceLabel, ev.WorkspaceLabel)
	merge(&a.TabID, ev.TabID)
	merge(&a.TabLabel, ev.TabLabel)
	merge(&a.PaneID, ev.PaneID)
	merge(&a.TerminalID, ev.TerminalID)
	merge(&a.Model, ev.Model)
	merge(&a.RunStartedAt, ev.RunStartedAt)
	if ev.Repo != nil {
		merge(&a.Repo, ev.Repo.Remote)
		if a.Repo == "" {
			merge(&a.Repo, ev.Repo.Name)
		}
		merge(&a.Branch, ev.Repo.Branch)
	}
	switch ev.Kind {
	case "run.started":
		a.Model = ev.Model
		a.RunStartedAt = ev.RunStartedAt
		a.RunDurationMS, a.TokensIn, a.TokensOut, a.TokensCacheR, a.TokensCacheW = 0, 0, 0, 0, 0
	case "run.finished":
		a.Model = ev.Model
		a.RunStartedAt = ev.RunStartedAt
		a.RunDurationMS, a.TokensIn, a.TokensOut = ev.RunDurationMS, ev.TokensIn, ev.TokensOut
		a.TokensCacheR, a.TokensCacheW = ev.TokensCacheR, ev.TokensCacheW
		if ev.Session != nil {
			s := *ev.Session
			a.Session = &s
		}
	case "run.usage":
		// live/trailing deltas: trailing updates carry no duration — keep
		// the one run.finished recorded
		merge(&a.Model, ev.Model)
		merge(&a.RunStartedAt, ev.RunStartedAt)
		if ev.RunDurationMS > 0 {
			a.RunDurationMS = ev.RunDurationMS
		}
		a.TokensIn, a.TokensOut = ev.TokensIn, ev.TokensOut
		a.TokensCacheR, a.TokensCacheW = ev.TokensCacheR, ev.TokensCacheW
		if ev.Session != nil {
			s := *ev.Session
			a.Session = &s
		}
	}
	typ := "agent.updated"
	if !exists {
		typ = "agent.added"
	}
	if ev.Kind == "agent.gone" {
		typ = "agent.deleted"
		a.Status = "unknown"
		delete(agents, id)
	} else {
		agents[id] = a
	}
	return Message{V: 1, Type: typ, Timestamp: ev.TS, SourceKind: ev.Kind, AgentID: id, Agent: a}, true
}

func (s *Server) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) || len(header) == len(prefix) {
		return false
	}
	got := []byte(strings.TrimPrefix(header, prefix))
	want := []byte(s.cfg.AuthToken)
	return len(got) == len(want) && subtle.ConstantTimeCompare(got, want) == 1
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r.Header.Get("Authorization")) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "server closing")
	c := &client{send: make(chan []byte, 64)}
	reg := registration{client: c, snapshot: make(chan [][]byte, 1)}
	select {
	case s.register <- reg:
	case <-s.ctx.Done():
		return
	}
	var snapshot [][]byte
	select {
	case snapshot = <-reg.snapshot:
	case <-s.ctx.Done():
		return
	}
	// Give fragile clients a beat to finish parsing the 101 upgrade response
	// before snapshot frames arrive; frames coalesced with the handshake can
	// be dropped by clients without a transport pushback buffer (e.g.
	// esp_websocket_client on ESP32).
	timer := time.NewTimer(50 * time.Millisecond)
	select {
	case <-timer.C:
	case <-s.ctx.Done():
		timer.Stop()
		return
	}
	for _, b := range snapshot {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		err := conn.Write(ctx, websocket.MessageText, b)
		cancel()
		if err != nil {
			return
		}
	}
	defer func() {
		select {
		case s.unregister <- c:
		case <-s.ctx.Done():
		}
	}()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-readDone:
			return
		case <-ping.C:
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			err := conn.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		case b, ok := <-c.send:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			err := conn.Write(ctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
