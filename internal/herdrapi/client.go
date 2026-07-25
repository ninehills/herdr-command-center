// Package herdrapi is a minimal client for herdr's newline-delimited JSON
// socket API (https://herdr.dev/docs/socket-api/).
//
// Two access patterns:
//   - Call: one-shot request/response on a short-lived connection
//   - Stream: a long-lived connection carrying events.subscribe traffic
//
// Response frames carry an "id"; event frames carry "event" + "data".
// Note: on subscribe, herdr REPLAYS current state as synthetic *_created
// events — Stream reports the connection time so callers can treat the
// initial burst as backfill.
package herdrapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// SocketPath resolves the herdr socket (herdr-injected env first).
func SocketPath() string {
	if p := os.Getenv("HERDR_SOCKET_PATH"); p != "" {
		return p
	}
	base, err := os.UserConfigDir()
	if err != nil {
		base = filepath.Join(os.Getenv("HOME"), ".config")
	}
	return filepath.Join(base, "herdr", "herdr.sock")
}

type Client struct {
	Path string
}

func New() *Client { return &Client{Path: SocketPath()} }

type request struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

type response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *APIError       `json:"error"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Call performs one request on a fresh connection.
func (c *Client) Call(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	conn, err := net.DialTimeout("unix", c.Path, timeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.Path, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if params == nil {
		params = map[string]any{}
	}
	line, err := json.Marshal(request{ID: "req", Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		var resp response
		if err := json.Unmarshal(sc.Bytes(), &resp); err != nil {
			continue
		}
		// skip any stray event frames; we want our response
		if resp.ID == "" {
			continue
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%s: connection closed without response", method)
}

// ── snapshot types (fields observed from herdr 0.7.x) ───────────────

type Agent struct {
	Agent         string `json:"agent"`
	AgentStatus   string `json:"agent_status"`
	Cwd           string `json:"cwd"`
	ForegroundCwd string `json:"foreground_cwd"`
	Focused       bool   `json:"focused"`
	PaneID        string `json:"pane_id"`
	TabID         string `json:"tab_id"`
	TerminalID    string `json:"terminal_id"`
	WorkspaceID   string `json:"workspace_id"`
	// AgentSession is reported by herdr's agent integrations: the exact
	// session file this agent is writing (kind="path").
	AgentSession *AgentSession `json:"agent_session,omitempty"`
}

type AgentSession struct {
	Agent  string `json:"agent"`
	Kind   string `json:"kind"`
	Source string `json:"source"`
	Value  string `json:"value"`
}

type Workspace struct {
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
	Number      int    `json:"number"`
	Focused     bool   `json:"focused"`
	AgentStatus string `json:"agent_status"`
	PaneCount   int    `json:"pane_count"`
	TabCount    int    `json:"tab_count"`
	ActiveTabID string `json:"active_tab_id"`
}

func (c *Client) AgentList(timeout time.Duration) ([]Agent, error) {
	raw, err := c.Call("agent.list", nil, timeout)
	if err != nil {
		return nil, err
	}
	var out struct {
		Agents []Agent `json:"agents"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Agents, nil
}

func (c *Client) WorkspaceList(timeout time.Duration) ([]Workspace, error) {
	raw, err := c.Call("workspace.list", nil, timeout)
	if err != nil {
		return nil, err
	}
	var out struct {
		Workspaces []Workspace `json:"workspaces"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Workspaces, nil
}

// Tab is one entry from tab.list (unfiltered = every tab in the session).
// Labels default to the tab's number as a string ("1", "2", …) until the
// user renames them — consumers treat purely-numeric labels as unnamed.
type Tab struct {
	TabID       string `json:"tab_id"`
	WorkspaceID string `json:"workspace_id"`
	Label       string `json:"label"`
	Number      int    `json:"number"`
}

// Pane is one entry from pane.list. Unlike agent.list it enumerates ALL
// panes — including plain shells with no detected agent — so it is the
// authoritative source for which pane the user has focused. Exactly one
// pane carries Focused == true, and that boolean is stable (it tracks UI
// focus, NOT the rapid pane.focused *event* stream, which is herdr's
// internal detection scan cursor cycling every pane ~10×/second).
type Pane struct {
	PaneID        string `json:"pane_id"`
	TerminalID    string `json:"terminal_id"`
	WorkspaceID   string `json:"workspace_id"`
	TabID         string `json:"tab_id"`
	Focused       bool   `json:"focused"`
	Cwd           string `json:"cwd"`
	ForegroundCwd string `json:"foreground_cwd"`
	Agent         string `json:"agent"`
	AgentStatus   string `json:"agent_status"`
}

func (c *Client) TabList(timeout time.Duration) ([]Tab, error) {
	raw, err := c.Call("tab.list", nil, timeout)
	if err != nil {
		return nil, err
	}
	var out struct {
		Tabs []Tab `json:"tabs"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Tabs, nil
}

func (c *Client) PaneList(timeout time.Duration) ([]Pane, error) {
	raw, err := c.Call("pane.list", nil, timeout)
	if err != nil {
		return nil, err
	}
	var out struct {
		Panes []Pane `json:"panes"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Panes, nil
}

// Notify shows a herdr toast (notification.show). Whether it renders depends
// on herdr's own [ui.toast] delivery setting; a disabled setting returns
// {shown:false} rather than an error.
func (c *Client) Notify(title, body, position string, timeout time.Duration) error {
	params := map[string]any{"title": title}
	if body != "" {
		params["body"] = body
	}
	if position != "" {
		params["position"] = position
	}
	_, err := c.Call("notification.show", params, timeout)
	return err
}

// ── git worktree mapping ─────────────────────────────────────────────
//
// worktree.list resolves the git repository enclosing a WORKSPACE's source
// checkout: repo root, human name, and the per-checkout branch / linked-
// worktree flag. The selector is workspace_id (a path/pane_id param is
// ignored; with no selector herdr answers for the focused workspace). A
// workspace whose checkout is not inside a work tree returns a
// not_git_worktree APIError — callers fall back to a raw git call.

type Worktree struct {
	Path             string `json:"path"`
	Branch           string `json:"branch"`
	IsLinkedWorktree bool   `json:"is_linked_worktree"`
	Label            string `json:"label"`
}

type WorktreeSource struct {
	RepoRoot           string `json:"repo_root"`
	RepoName           string `json:"repo_name"`
	SourceCheckoutPath string `json:"source_checkout_path"`
}

type WorktreeListResult struct {
	Source    WorktreeSource `json:"source"`
	Worktrees []Worktree     `json:"worktrees"`
}

// WorktreeList maps the given workspace's checkout to its repository. A
// non-git workspace surfaces as an APIError (code "not_git_worktree").
func (c *Client) WorktreeList(workspaceID string, timeout time.Duration) (*WorktreeListResult, error) {
	raw, err := c.Call("worktree.list", map[string]any{"workspace_id": workspaceID}, timeout)
	if err != nil {
		return nil, err
	}
	var out WorktreeListResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── event streaming ──────────────────────────────────────────────────

type Subscription struct {
	Type   string `json:"type"`
	PaneID string `json:"pane_id,omitempty"`
}

// EventFrame is a pushed event: {"event":"workspace_focused","data":{...}}.
type EventFrame struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

// Stream opens a long-lived connection, subscribes, and delivers events to
// onEvent until ctx is done or the connection drops (returned error). The
// onConnect callback receives the subscribe time — events arriving within
// the caller's chosen grace window after it are herdr's state replay.
func (c *Client) Stream(ctx context.Context, subs []Subscription, onConnect func(time.Time), onEvent func(EventFrame)) error {
	conn, err := net.DialTimeout("unix", c.Path, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.Path, err)
	}
	defer conn.Close()
	// the closer must exit when Stream returns naturally, not only on ctx
	// cancellation — otherwise every reconnect leaks a goroutine
	streamDone := make(chan struct{})
	defer close(streamDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-streamDone:
		}
	}()

	line, err := json.Marshal(request{
		ID:     "sub",
		Method: "events.subscribe",
		Params: map[string]any{"subscriptions": subs},
	})
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return err
	}
	onConnect(time.Now())

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		var frame EventFrame
		if err := json.Unmarshal(sc.Bytes(), &frame); err != nil || frame.Event == "" {
			// subscribe ack or unparseable frame — check for a subscribe error
			var resp response
			if err := json.Unmarshal(sc.Bytes(), &resp); err == nil && resp.Error != nil {
				return fmt.Errorf("events.subscribe: %w", resp.Error)
			}
			continue
		}
		onEvent(frame)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return fmt.Errorf("event stream closed")
}
