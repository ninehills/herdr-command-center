// Package watch implements the read-only ANSI WebSocket agent dashboard.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

type options struct {
	URL   string
	Token string
}

type agent struct {
	Name           string `json:"name"`
	Harness        string `json:"harness"`
	Status         string `json:"status"`
	PreviousStatus string `json:"previous_status"`
	WorkspaceLabel string `json:"workspace_label"`
	PaneID         string `json:"pane_id"`
	Model          string `json:"model"`
	RunDurationMS  int64  `json:"run_duration_ms"`
	TokensIn       int64  `json:"tokens_in"`
	TokensOut      int64  `json:"tokens_out"`
	TokensCacheR   int64  `json:"tokens_cache_read"`
	TokensCacheW   int64  `json:"tokens_cache_write"`
	Session        *struct {
		In            int64   `json:"in"`
		Out           int64   `json:"out"`
		CacheRead     int64   `json:"cache_read"`
		CacheWrite    int64   `json:"cache_write"`
		CostUSD       float64 `json:"cost_usd"`
		CacheHit      float64 `json:"cache_hit"`
		ContextTokens int64   `json:"context_tokens"`
		ContextWindow int64   `json:"context_window"`
	} `json:"session"`
}

type message struct {
	V          int    `json:"v"`
	Type       string `json:"type"`
	Timestamp  string `json:"timestamp"`
	AgentID    string `json:"agent_id"`
	SourceKind string `json:"source_kind"`
	Agent      agent  `json:"agent"`
}

type update struct {
	message *message
	state   string
	reset   bool
}

var errAuth = errors.New("authentication failed")

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: herdr-command-center watch ws://host:port/path --token TOKEN

Options:
  --token string  WebSocket Bearer token (required)
  -h, --help      show this help

The dashboard reconnects automatically after network failures. Press Ctrl-C to quit.`)
}

func parseArgs(args []string) (options, error) {
	var normalized, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--token":
			if i+1 >= len(args) {
				return options{}, errors.New("--token requires a value")
			}
			normalized = append(normalized, a, args[i+1])
			i++
		case strings.HasPrefix(a, "--token="):
			normalized = append(normalized, a)
		case a == "-h" || a == "--help":
			return options{}, flag.ErrHelp
		case strings.HasPrefix(a, "-"):
			return options{}, fmt.Errorf("unknown option %s", a)
		default:
			positional = append(positional, a)
		}
	}
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	token := fs.String("token", "", "")
	if err := fs.Parse(normalized); err != nil {
		return options{}, err
	}
	if len(positional) != 1 {
		return options{}, errors.New("exactly one WebSocket URL is required")
	}
	if *token == "" {
		return options{}, errors.New("--token is required")
	}
	u, err := url.Parse(positional[0])
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return options{}, errors.New("URL must use ws:// or wss://")
	}
	return options{URL: u.String(), Token: *token}, nil
}

func Run(args []string) error {
	opts, err := parseArgs(args)
	if errors.Is(err, flag.ErrHelp) {
		usage(os.Stdout)
		return nil
	}
	if err != nil {
		return err
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(signalCtx)
	updates := make(chan update, 128)
	drawDone := make(chan struct{})
	fmt.Print("\x1b[?1049h\x1b[?25l")
	go func() {
		defer close(drawDone)
		drawLoop(ctx, os.Stdout, opts.URL, updates)
	}()
	defer func() {
		cancel()
		<-drawDone
		fmt.Print("\x1b[?25h\x1b[?1049l")
		stop()
	}()
	return connectLoop(ctx, opts, updates)
}

func connectLoop(ctx context.Context, opts options, updates chan<- update) error {
	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		if !sendUpdate(ctx, updates, update{state: "connecting"}) {
			break
		}
		header := http.Header{"Authorization": {"Bearer " + opts.Token}}
		conn, resp, err := websocket.Dial(ctx, opts.URL, &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
				return errAuth
			}
			if !sendUpdate(ctx, updates, update{state: "reconnecting"}) {
				break
			}
			if !sleep(ctx, jitter(backoff)) {
				break
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		if !sendUpdate(ctx, updates, update{state: "connected", reset: true}) {
			_ = conn.Close(websocket.StatusNormalClosure, "stopping")
			break
		}
		received := false
		for {
			typ, data, readErr := conn.Read(ctx)
			if readErr != nil {
				break
			}
			if typ != websocket.MessageText {
				continue
			}
			var msg message
			if json.Unmarshal(data, &msg) != nil || msg.V != 1 || msg.AgentID == "" || (msg.Type != "agent.added" && msg.Type != "agent.updated" && msg.Type != "agent.deleted") {
				continue
			}
			received = true
			if !sendUpdate(ctx, updates, update{message: &msg}) {
				break
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "reconnecting")
		if received {
			backoff = 500 * time.Millisecond
		}
		if !sendUpdate(ctx, updates, update{state: "reconnecting"}) {
			break
		}
		if !sleep(ctx, jitter(backoff)) {
			break
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	return nil
}

func sendUpdate(ctx context.Context, updates chan<- update, u update) bool {
	select {
	case updates <- u:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d/5)+1))
}

// maxEventLog caps the in-memory event ring buffer; only the tail that
// fits the terminal is rendered.
const maxEventLog = 200

func drawLoop(ctx context.Context, w io.Writer, endpoint string, updates <-chan update) {
	agents := map[string]agent{}
	events := make([]string, 0, maxEventLog)
	state, updated := "connecting", time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	dirty := true
	for {
		select {
		case <-ctx.Done():
			return
		case u := <-updates:
			if u.state != "" {
				state = u.state
				events = appendEvent(events, time.Now().Format("15:04:05")+"  -- "+u.state+" --")
			}
			if u.reset {
				agents = map[string]agent{}
			}
			if u.message != nil {
				applyMessage(agents, *u.message)
				if u.message.SourceKind != "run.usage" { // too frequent for the log; table still updates
					events = appendEvent(events, formatEvent(*u.message))
				}
				updated = time.Now()
			}
			dirty = true
		case <-ticker.C:
			if dirty {
				render(w, endpoint, state, updated, agents, events)
				dirty = false
			}
		}
	}
}

func appendEvent(events []string, line string) []string {
	events = append(events, line)
	if len(events) > maxEventLog {
		events = append(events[:0], events[len(events)-maxEventLog:]...)
	}
	return events
}

// formatEvent renders one received message as a compact debug log line.
func formatEvent(msg message) string {
	ts := msg.Timestamp
	if t, err := time.Parse(time.RFC3339Nano, msg.Timestamp); err == nil {
		ts = t.Local().Format("15:04:05")
	}
	line := fmt.Sprintf("%s  %-13s  %s", ts, msg.Type, msg.AgentID)
	var parts []string
	if msg.Agent.Status != "" {
		s := msg.Agent.Status
		if msg.Agent.PreviousStatus != "" {
			s = msg.Agent.PreviousStatus + "->" + s
		}
		parts = append(parts, "status="+s)
	}
	if msg.Agent.Model != "" {
		parts = append(parts, "model="+msg.Agent.Model)
	}
	if totalIn(msg.Agent) != 0 || msg.Agent.TokensOut != 0 {
		parts = append(parts, fmt.Sprintf("tokens=%d/%d", totalIn(msg.Agent), msg.Agent.TokensOut))
	}

	if len(parts) > 0 {
		line += "  " + strings.Join(parts, " ")
	}
	if msg.SourceKind != "" {
		line += "  [" + msg.SourceKind + "]"
	}
	return safeText(line)
}

func applyMessage(agents map[string]agent, msg message) {
	if msg.Type == "agent.deleted" {
		delete(agents, msg.AgentID)
	} else {
		agents[msg.AgentID] = msg.Agent
	}
}

func render(w io.Writer, endpoint, state string, updated time.Time, agents map[string]agent, events []string) {
	width := 100
	if n, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && n >= 40 {
		width = n
	}
	height := 24
	if n, err := strconv.Atoi(os.Getenv("LINES")); err == nil && n >= 10 {
		height = n
	}
	fmt.Fprint(w, "\x1b[H\x1b[2J")
	fmt.Fprintf(w, "herdr-command-center watch  %s  %s  agents: %d  updated: %s\n\n", clip(endpoint, max(16, width-62)), state, len(agents), updated.Format("15:04:05"))
	if width < 70 {
		fmt.Fprintln(w, "NAME             STATUS      WORKSPACE")
	} else {
		fmt.Fprintln(w, "NAME             STATUS      WORKSPACE          PANE          MODEL          INPUT   OUTPUT  CACHE READ  HIT      COST      CONTEXT")
	}
	rows := make([]agent, 0, len(agents))
	for _, a := range agents {
		rows = append(rows, a)
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return a.WorkspaceLabel+a.Name+a.PaneID < b.WorkspaceLabel+b.Name+b.PaneID
	})
	for _, a := range rows {
		name := a.Name
		if name == "" {
			name = a.Harness
		}
		if width < 70 {
			fmt.Fprintf(w, "%-16s %-11s %s\n", clip(name, 15), colored(a.Status), clip(a.WorkspaceLabel, max(8, width-30)))
		} else {
			in, out, cacheRead, hit, cost, ctx := sessionStats(a)
			fmt.Fprintf(w, "%-16s %-20s %-18s %-13s %-14s %-7s %-7s %-10s %-8s %-9s %s\n", clip(name, 15), colored(a.Status), clip(a.WorkspaceLabel, 17), clip(a.PaneID, 12), clip(a.Model, 13), in, out, cacheRead, hit, cost, ctx)
		}
	}
	// Event log fills the remaining terminal rows (header 2 + table header 1
	// + rows + section title/blank 2 + footer 2 are already spoken for).
	if avail := height - len(rows) - 7; avail > 0 {
		fmt.Fprintln(w, "\nEVENTS")
		if len(events) > avail {
			events = events[len(events)-avail:]
		}
		for _, e := range events {
			fmt.Fprintln(w, clip(e, width-1))
		}
	}
	fmt.Fprintln(w, "\nCtrl-C quit")
}

func colored(status string) string {
	status = safeText(status)
	color := map[string]string{"working": "32", "blocked": "33", "done": "34", "idle": "90", "unknown": "31"}[status]
	if color == "" {
		color = "31"
	}
	return "\x1b[" + color + "m" + fmt.Sprintf("%-10s", status) + "\x1b[0m"
}

func clip(s string, n int) string {
	r := []rune(safeText(s))
	if len(r) <= n {
		return string(r)
	}
	if n <= 1 {
		return "…"
	}
	return string(r[:n-1]) + "…"
}

func safeText(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return -1
		}
		return r
	}, s)
}

// totalIn counts every input token: fresh input plus tokens served from or
// written to the prompt cache (providers report the three separately).
func totalIn(a agent) int64 { return a.TokensIn + a.TokensCacheR + a.TokensCacheW }

// sessionStats renders session-cumulative stats as plain-English columns:
// total input, output, cache read, latest-request cache hit rate, cost,
// and context window usage. Missing data shows as "-".
func sessionStats(a agent) (in, out, cacheRead, hit, cost, ctx string) {
	in, out, cacheRead, hit, cost, ctx = "-", "-", "-", "-", "-", "-"
	s := a.Session
	if s == nil {
		return
	}
	if s.In > 0 {
		in = formatTokens(s.In)
	}
	if s.Out > 0 {
		out = formatTokens(s.Out)
	}
	if s.CacheRead > 0 {
		cacheRead = formatTokens(s.CacheRead)
	}
	if s.CacheHit > 0 {
		hit = fmt.Sprintf("%.1f%%", s.CacheHit)
	}
	if s.CostUSD > 0 {
		cost = fmt.Sprintf("$%.3f", s.CostUSD)
	}
	if s.ContextWindow > 0 {
		ctx = fmt.Sprintf("%.1f%%/%s", 100*float64(s.ContextTokens)/float64(s.ContextWindow), formatTokens(s.ContextWindow))
	}
	return
}

// formatTokens matches pi's compact footer format.
func formatTokens(n int64) string {
	switch {
	case n < 1000:
		return strconv.FormatInt(n, 10)
	case n < 10000:
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	case n < 1000000:
		return fmt.Sprintf("%dk", (n+500)/1000)
	case n < 10000000:
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	default:
		return fmt.Sprintf("%dM", (n+500000)/1000000)
	}
}

func count(n int64) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	}
	return strconv.FormatInt(n, 10)
}
