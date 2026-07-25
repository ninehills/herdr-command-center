// Package notify surfaces daemon-health events as herdr toast notifications
// (the socket notification.show method). Every notification is gated by
// config so the user chooses which classes fire; a nil Notifier is a no-op,
// and delivery failures never block the caller.
//
// Note: whether a toast actually appears also depends on herdr's own
// [ui.toast] delivery setting — if that is "off", notification.show returns
// {shown:false, reason:"disabled"} and we simply move on.
package notify

import (
	"time"

	"github.com/ninehills/herdr-command-center/internal/config"
	"github.com/ninehills/herdr-command-center/internal/herdrapi"
)

// Event classes; each maps to a config toggle in [notify].
const (
	Start        = "start"
	Degraded     = "degraded"
	Recovered    = "recovered"
	EndpointDown = "endpoint_down"
	EndpointUp   = "endpoint_up"
	Update       = "update"
)

type Notifier struct {
	cfg config.Notify
	api *herdrapi.Client
}

func New(cfg config.Notify) *Notifier {
	return &Notifier{cfg: cfg, api: herdrapi.New()}
}

func (n *Notifier) enabled(event string) bool {
	if n == nil || !n.cfg.Enabled {
		return false
	}
	switch event {
	case Start:
		return n.cfg.OnStart
	case Degraded:
		return n.cfg.OnDegraded
	case Recovered:
		return n.cfg.OnRecovered
	case EndpointDown, EndpointUp:
		return n.cfg.OnEndpoint
	case Update:
		return n.cfg.OnUpdate
	}
	return false
}

// Notify shows a toast for the given event class if enabled. Best-effort:
// errors (including herdr's toasts being disabled) are swallowed.
func (n *Notifier) Notify(event, title, body string) {
	if !n.enabled(event) {
		return
	}
	_ = n.api.Notify(title, body, n.cfg.Position, 3*time.Second)
}
