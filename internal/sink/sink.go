// Package sink batches telemetry events and delivers them to the configured
// HTTP endpoint as newline-delimited JSON (optionally gzipped). Failed
// batches spool to disk (size-capped) and drain on the next success.
package sink

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ninehills/herdr-command-center/internal/config"
)

type Stats struct {
	Enqueued   atomic.Int64
	Sent       atomic.Int64
	Dropped    atomic.Int64
	Spooled    atomic.Int64
	SendErrors atomic.Int64
	LastFlush  atomic.Int64 // unix ms
	LastError  atomic.Value // string
}

type Sink struct {
	cfg      config.Config
	client   *http.Client
	spool    string
	mu       sync.Mutex
	buf      []json.RawMessage
	wake     chan struct{}
	done     chan struct{}
	finished chan struct{}
	down     atomic.Bool
	onState  func(down bool, detail string)
	Stats    Stats
}

func New(cfg config.Config) *Sink {
	s := &Sink{
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.HTTPTimeout()},
		spool:    filepath.Join(config.StateDir(), "spool.ndjson"),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	s.Stats.LastError.Store("")
	return s
}

// SetOnState registers a callback fired when delivery flips down↔up. Set
// before Run starts.
func (s *Sink) SetOnState(fn func(down bool, detail string)) { s.onState = fn }

func (s *Sink) setDown(down bool, detail string) {
	if s.down.Swap(down) != down && s.onState != nil {
		s.onState(down, detail)
	}
}

// Enqueue adds one event; oldest events drop if the buffer is full so the
// collector never blocks on a slow endpoint.
func (s *Sink) Enqueue(ev any) {
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.Lock()
	if len(s.buf) >= s.cfg.Send.MaxBufferEvents {
		s.buf = s.buf[1:]
		s.Stats.Dropped.Add(1)
	}
	s.buf = append(s.buf, raw)
	full := len(s.buf) >= s.cfg.Send.MaxBatch
	s.mu.Unlock()
	s.Stats.Enqueued.Add(1)
	if full {
		s.Kick()
	}
}

// Kick requests an immediate flush (used by SIGUSR1 and batch-full).
func (s *Sink) Kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run flushes on an interval until Stop; final flush on shutdown.
func (s *Sink) Run() {
	t := time.NewTicker(s.cfg.FlushInterval())
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.flush()
		case <-s.wake:
			s.flush()
		case <-s.done:
			s.flush()
			close(s.finished)
			return
		}
	}
}

func (s *Sink) Stop() {
	close(s.done)
	<-s.finished
}

func (s *Sink) take() []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.buf)
	if n == 0 {
		return nil
	}
	if n > s.cfg.Send.MaxBatch {
		n = s.cfg.Send.MaxBatch
	}
	batch := s.buf[:n]
	s.buf = append([]json.RawMessage(nil), s.buf[n:]...)
	return batch
}

func (s *Sink) flush() {
	for {
		batch := s.take()
		if batch == nil {
			break
		}
		if err := s.post(batch); err != nil {
			s.Stats.SendErrors.Add(1)
			s.Stats.LastError.Store(err.Error())
			s.spoolBatch(batch)
			s.setDown(true, err.Error())
			return // endpoint is down; stop hammering until next tick
		}
		s.Stats.Sent.Add(int64(len(batch)))
		s.setDown(false, "")
	}
	s.drainSpool()
	s.Stats.LastFlush.Store(time.Now().UnixMilli())
}

func (s *Sink) post(batch []json.RawMessage) error {
	if s.cfg.Endpoint.URL == "" {
		return fmt.Errorf("no endpoint.url configured")
	}
	var body bytes.Buffer
	for _, raw := range batch {
		body.Write(raw)
		body.WriteByte('\n')
	}
	payload := body.Bytes()

	var buf bytes.Buffer
	headers := map[string]string{"Content-Type": "application/x-ndjson"}
	if s.cfg.Send.Gzip {
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(payload); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		payload = buf.Bytes()
		headers["Content-Encoding"] = "gzip"
	}

	req, err := http.NewRequest(http.MethodPost, s.cfg.Endpoint.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if s.cfg.Endpoint.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Endpoint.AuthToken)
	}
	for k, v := range s.cfg.Endpoint.Headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("endpoint returned %s", resp.Status)
	}
	return nil
}

// spoolBatch appends failed events to the on-disk spool; if the spool
// exceeds its cap the whole file is dropped (oldest data loses).
func (s *Sink) spoolBatch(batch []json.RawMessage) {
	_ = os.MkdirAll(filepath.Dir(s.spool), 0o755)
	if st, err := os.Stat(s.spool); err == nil &&
		st.Size() > int64(s.cfg.Send.MaxSpoolMB)*1024*1024 {
		// count what we're throwing away, not just the fact of it
		if data, rerr := os.ReadFile(s.spool); rerr == nil {
			s.Stats.Dropped.Add(int64(bytes.Count(data, []byte{'\n'})))
		} else {
			s.Stats.Dropped.Add(1)
		}
		_ = os.Remove(s.spool)
	}
	f, err := os.OpenFile(s.spool, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	for _, raw := range batch {
		f.Write(raw)
		f.Write([]byte{'\n'})
	}
	s.Stats.Spooled.Add(int64(len(batch)))
}

// drainSpool retries previously failed events after a successful flush.
// The file is atomically claimed first (rename) so a mid-drain failure
// re-spools only the UNSENT remainder — delivered batches are never
// re-POSTed on the next drain.
func (s *Sink) drainSpool() {
	claimed := s.spool + ".draining"
	if err := os.Rename(s.spool, claimed); err != nil {
		return // no spool (common case) or already claimed
	}
	data, err := os.ReadFile(claimed)
	if err != nil {
		return
	}
	lines := bytes.Split(data, []byte{'\n'})
	var batch []json.RawMessage
	requeueFrom := func(i int, pending []json.RawMessage) {
		f, ferr := os.OpenFile(s.spool, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if ferr != nil {
			return
		}
		defer f.Close()
		for _, raw := range pending {
			f.Write(raw)
			f.Write([]byte{'\n'})
		}
		for ; i < len(lines); i++ {
			if len(bytes.TrimSpace(lines[i])) == 0 {
				continue
			}
			f.Write(lines[i])
			f.Write([]byte{'\n'})
		}
	}
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		batch = append(batch, json.RawMessage(line))
		if len(batch) >= s.cfg.Send.MaxBatch {
			if s.post(batch) != nil {
				requeueFrom(i+1, batch)
				_ = os.Remove(claimed)
				return
			}
			s.Stats.Sent.Add(int64(len(batch)))
			batch = nil
		}
	}
	if len(batch) > 0 {
		if s.post(batch) != nil {
			requeueFrom(len(lines), batch)
			_ = os.Remove(claimed)
			return
		}
		s.Stats.Sent.Add(int64(len(batch)))
	}
	_ = os.Remove(claimed)
}

// SpoolBytes reports the on-disk backlog size for status output.
func (s *Sink) SpoolBytes() int64 {
	st, err := os.Stat(s.spool)
	if err != nil {
		return 0
	}
	return st.Size()
}

// Buffered reports the in-memory queue length for status output.
func (s *Sink) Buffered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}
