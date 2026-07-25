# herdr-command-center

> A [herdr](https://herdr.dev) plugin that streams your workspace & agent
> activity to an endpoint you control.

[![ci](https://github.com/ninehills/herdr-command-center/actions/workflows/ci.yml/badge.svg)](https://github.com/ninehills/herdr-command-center/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

You run agents all day — Claude Code in one pane, Codex in another, herdr
orchestrating the lot. `herdr-command-center` watches that session and turns it
into a clean event stream: which workspaces exist, which agents are running
where, when a work run started, how long it took. POST it at a webhook,
a serverless function, a KV store, your own dashboard — it's newline-
delimited JSON to a URL you choose.

Built for things like a live *"what am I working on right now"* panel on a
personal site, but generic by design: it's a telemetry pipe, not a product.

## How it works

```
             ┌──────────────────────── herdr server ───────────────────────┐
             │  socket API (ndjson over a local unix socket)               │
             └──────┬──────────────────────────────┬───────────────────────┘
                    │ events.subscribe             │ agent.list / workspace.list
                    │ (lifecycle stream)           │ (adaptive polling diff)
             ┌──────▼──────────────────────────────▼──────┐
             │            herdr-command-center daemon          │
             │  · lifecycle events (created/renamed/…)    │
             │  · agent status transitions                │
             │  · derived runs (working → idle, + timing) │
             │  · privacy shaping (labels, cwd, filters)  │
             └──────────────────┬─────────────────────────┘
                                │ batched ndjson POSTs (gzip, bearer auth)
                                │ offline → disk spool, drains on recovery
                        ┌───────▼────────┐
                        │  your endpoint │
                        └────────────────┘
```

- **Lifecycle** (workspaces, tabs, panes, worktrees) comes from a single
  long-lived `events.subscribe` connection. Herdr replays current state on
  subscribe; the daemon uses that replay to warm its cache without emitting.
- **Agent activity** is derived by diffing cheap `agent.list` snapshots —
  herdr has no wildcard pane subscriptions, and polling one local socket
  call every few seconds is lighter than per-pane subscription churn.
  Polling adapts: 5s while any agent is working, 30s when everything idles.
- **Runs** are derived from status transitions: `working` starts a run,
  `idle`/`done` ends it (with duration); `blocked` pauses inside a run —
  an agent waiting on your input is still mid-task. Sub-30s blips are
  ignored (configurable).

## What gets captured

| Event kind | Source | Payload highlights |
|---|---|---|
| `run.started` / `run.finished` | derived | workspace, pane, harness, started_at, duration_ms, `model` + token counters‡ |
| `agent.seen` / `agent.gone` / `agent.status_changed` | poll diff | harness (`claude`, `codex`, …), status, prev_status, `repo`† |
| `focus.interval` / `focus.changed` | poll diff | which pane the **user** focused, and for how long (opt-in) |
| `workspace.created/renamed/closed` | event stream | id, label, number |
| `tab.created/renamed/closed` | event stream | id, label, workspace |
| `pane.created/closed/exited` | event stream | id, tab, workspace |
| `worktree.created/opened/removed` | event stream | workspace, path* |
| `snapshot` | timer (15m) | compact full state for reconciliation (with `repo`†) |
| `daemon.started/stopped`, `test` | plugin | — |

\* paths and cwds are only sent when `privacy.include_cwd = true`.

‡ `model` (e.g. `claude-fable-5`, `gpt-5.5`) and per-run token deltas
(`tokens_in/out/cache_read/cache_write`) come from the harness session files —
counters and model id only, see the privacy section. Gated by
`collect.session_usage`.

† `repo` is a nested object — `{repo_root, repo_name, branch, remote, is_worktree}` — `remote` is `owner/repo` when origin points at github.com —
resolved from herdr's native git worktree mapping (a cached `git rev-parse`
covers directories herdr doesn't track). It is the concrete git identity
enclosing a pane's foreground cwd and a stable grouping key for
**time-by-repo** analytics. Repo identity inherits cwd privacy: with
`include_cwd = false` (the default) `repo_root` is emitted as a stable
`sha256:` hash and `repo_name`/`branch` are suppressed; with `include_cwd =
true` the root is home-relative and name/branch pass through.

**`focus.interval`** closes when focus leaves a pane (or on shutdown /
socket-degraded) and carries `{started_at, ended_at, duration_seconds,
reason, pane_id, tab_id, terminal_id, workspace_id, harness, repo}` — the
core primitive for time-by-repo / time-by-workspace / time-by-agent usage.
`focus.changed` is a lightweight now-focused pointer for a live panel. Both
are opt-in (`collect.focus_intervals`) because attention timing is a
behavioral signal. Focus is derived from the poll tick, so its resolution is
the poll cadence; sub-poll flickers are not attention and are not reported.

Example event:

```json
{"v":1,"kind":"run.finished","ts":"2026-07-08T21:14:03Z","host":"a1b2c3",
 "workspace_id":"w6","workspace_label":"PortfolioBuilder","workspace_number":3,
 "tab_id":"w6:t1","pane_id":"w6:p1","terminal_id":"term_65606224645204",
 "harness":"claude","status":"idle","prev_status":"working",
 "run_started_at":"2026-07-08T20:41:12Z","run_duration_ms":1971000}
```

## What does NOT get captured

- **No pane content.** The plugin never calls `pane.read` / `agent.read`, and
  never reads herdr's `agent.explain` region previews (on herdr 0.7.1 those
  scrape live terminal text — your prompt, transcript excerpts, and the model
  status line). Your prompts, agent output, and code never leave the machine.
- **No keystrokes, no commands, no file contents.**
- **No transcripts — counters only.** herdr 0.7.1 exposes no model or token
  data over its socket, so the optional session-usage collector
  (`collect.session_usage`, on by default) reads the harness's own session
  files directly — and decodes **only structural fields**: token counters
  (`usage` / `total_token_usage`) and the model id. Message content is never
  inspected, stored, or forwarded; Claude readers attach at the current end
  of the file so historical lines are never parsed at all, and Codex reads a
  bounded tail. If even that is too close, `collect.session_usage = false`
  turns the whole collector off and the plugin goes back to never opening
  harness files.
- **No user-focus firehose.** herdr's `pane.focused` *event* fires ~10×/sec
  cycling every pane (it's an internal detection-scan cursor, not attention),
  so we never subscribe to it — `focus.*` events derive from the stable
  focused-pane flag instead.
- Working directories are **off by default**; hostname is hashed by default;
  workspace labels can be disabled or allow/deny-listed per label; repo
  name/branch inherit the cwd redaction level.

## Install

Requires herdr ≥ 0.7. The install step downloads a prebuilt binary for your
platform (darwin/linux × amd64/arm64) — no Go toolchain needed; it falls back
to `go build` only if Go is present and no release asset matches.

```console
$ herdr plugin install ninehills/herdr-command-center
```

Configure your endpoint:

```console
$ $EDITOR "$(herdr plugin config-dir herdr-command-center)/config.toml"
```

```toml
[endpoint]
url = "https://your-endpoint.example/ingest"
auth_token = "…"
```

Then start it (it also autostarts on the next workspace/pane/worktree
creation via manifest hooks):

```console
$ herdr plugin action invoke herdr-command-center.start
$ herdr plugin action invoke herdr-command-center.test-endpoint
ok — sent 1 event to https://your-endpoint.example/ingest
$ herdr plugin action invoke herdr-command-center.status
daemon:   running (pid 41337)
endpoint: https://your-endpoint.example/ingest
sent:     212 events (214 enqueued, 2 buffered, 0 spooled, 0 dropped)
```

For quick experiments, environment variables beat editing config:

```console
$ HERDR_COMMAND_CENTER_ENDPOINT=http://localhost:8787/ingest herdr-command-center daemon
```

## Configuration

Full reference in [`config.example.toml`](config.example.toml). The short
version:

| Key | Default | Meaning |
|---|---|---|
| `endpoint.url` | *(required)* | where ndjson batches POST |
| `endpoint.auth_token` | `""` | sent as `Authorization: Bearer` |
| `websocket.enabled` | `false` | serve the authenticated live agent stream |
| `websocket.listen` | `127.0.0.1:9745` | WebSocket listen address |
| `websocket.path` | `/events` | WebSocket path |
| `websocket.auth_token` | `""` | required Bearer token when enabled |
| `send.flush_interval_ms` | `15000` | batch cadence |
| `send.max_spool_mb` | `5` | offline disk backlog cap |
| `collect.poll_interval_ms` | `5000` | agent poll while working |
| `collect.idle_poll_interval_ms` | `30000` | agent poll while idle |
| `collect.runs` | `true` | derive `run.*` events |
| `collect.min_run_ms` | `30000` | ignore shorter runs |
| `collect.repo` | `true` | attach git `repo` (root/name/branch/worktree) |
| `collect.focus_events` | `false` | `workspace.focused` (noisy) |
| `collect.focus_intervals` | `false` | `focus.interval` / `focus.changed` (user attention; opt-in) |
| `privacy.include_cwd` | `false` | send working directories (also un-redacts repo name/branch) |
| `privacy.include_labels` | `true` | send workspace/tab labels |
| `privacy.workspace_denylist` | `[]` | labels to never report |
| `update.enabled` | `true` | check GitHub for newer releases |
| `update.auto_apply` | `true` | re-install + restart on a newer release |
| `update.check_interval_hours` | `6` | how often to check |
| `notify.enabled` | `true` | herdr toast notifications for daemon health |
| `notify.on_*` | `true` | per-class toggles (start, degraded, recovered, endpoint, update) |
| `collect.session_usage` | `true` | model id + per-run token counters from harness session files |

Every collector (`workspaces`, `tabs`, `panes`, `agents`, `runs`,
`worktrees`, `repo`, `focus_intervals`) toggles independently.

## Self-update & notifications

The daemon checks GitHub for newer releases every `update.check_interval_hours`
and, when one exists, re-installs itself (`herdr plugin install … --yes`) and
restarts — no manual step, and no Go toolchain required on the host because
releases ship **prebuilt binaries** (darwin/linux × amd64/arm64) that the
install step downloads and checksum-verifies. If an update fails, or the
explicit restart doesn't land, the current daemon keeps running and the next
herdr lifecycle hook starts the freshly-installed binary — updates never take
telemetry down. Set `update.auto_apply = false` to be notified but apply
manually, or `update.enabled = false` to pin.

Daemon-health **notifications** surface as herdr toasts: daemon start, herdr
socket lost/recovered, telemetry endpoint down/up, and update available/applied.
Each class toggles under `[notify]`. Toasts only render if herdr's own toast
delivery is on — enable it in herdr's `config.toml`:

```toml
[ui.toast]
delivery = "herdr"   # in-app toasts (also: "terminal", "system", or "off")
```

## Receiving the stream

Batches arrive as `POST` with `Content-Type: application/x-ndjson`
(gzipped unless `send.gzip = false`), one event per line. A complete
receiver in ~10 lines of node:

```js
import http from "node:http";
import zlib from "node:zlib";

http.createServer((req, res) => {
  let stream = req.headers["content-encoding"] === "gzip"
    ? req.pipe(zlib.createGunzip()) : req;
  let body = "";
  stream.on("data", (c) => (body += c));
  stream.on("end", () => {
    body.trim().split("\n").forEach((l) => console.log(JSON.parse(l)));
    res.writeHead(204).end();
  });
}).listen(8787);
```

Delivery is at-least-once: if your endpoint is down, events spool to disk
(capped) and drain when it recovers. Consumers should treat `(host, ts,
kind, pane_id)` as an idempotency hint and use `snapshot` events to
reconcile drift.

## Live WebSocket and watch

Enable the optional loopback WebSocket service with a separate inbound token:

```toml
[websocket]
enabled = true
listen = "127.0.0.1:9745"
path = "/events"
auth_token = "change-me"
```

`HERDR_COMMAND_CENTER_WS_TOKEN` and `HERDR_COMMAND_CENTER_WS_LISTEN` override the token
and address. Clients must send `Authorization: Bearer <token>`; query-string
tokens are not accepted. For example:

```console
$ wscat -c ws://127.0.0.1:9745/events -H "Authorization: Bearer change-me"
$ herdr-command-center watch ws://127.0.0.1:9745/events --token change-me
```

Each text frame is protocol v1 JSON with type `agent.added`, `agent.updated`,
or `agent.deleted`, plus `source_kind`, `agent_id`, and the privacy-shaped
agent state. New clients first receive the hub's current agents as
`agent.added` snapshots. Updates follow the collector poll cadence (5 seconds
while working and 30 seconds while idle by default), rather than bypassing the
collector. While a run is open, `source_kind: run.usage` frames stream the
live per-run token counters (input/output/cache read/cache write) and model
on every poll tick they change; these frames are observer-only and are never
sent to the remote endpoint. `watch` uses a zero-dependency ANSI table with a
cache-hit-rate column, reconnects after network failures, and exits with
`Ctrl-C`.

## CLI

The plugin binary doubles as a CLI (also exposed as herdr actions):

```
herdr-command-center daemon         run the collector in the foreground
herdr-command-center ensure-daemon  start the daemon if not already running
herdr-command-center status         show daemon + delivery status
herdr-command-center flush          flush the batch now
herdr-command-center stop           stop the daemon
herdr-command-center test           send a single test event
herdr-command-center check-update   check GitHub for a newer release
herdr-command-center notify-test    send a test herdr notification
herdr-command-center print-config   print the effective configuration
herdr-command-center watch URL --token TOKEN  live ANSI agent dashboard
```

## Development

```console
$ git clone https://github.com/ninehills/herdr-command-center && cd herdr-command-center
$ go build -o bin/herdr-command-center .
$ herdr plugin link "$PWD"       # no build step on linked plugins
```

State lives in `HERDR_PLUGIN_STATE_DIR` (daemon pid, `status.json`,
`spool.ndjson`, `telemetry.log`); config in `HERDR_PLUGIN_CONFIG_DIR`.
Outside plugin context both fall back to XDG paths, so `go run . daemon`
works in any terminal inside a herdr session.

## Roadmap

- [x] git `repo` context (root/name/branch/worktree) on agent + snapshot events
- [x] `focus.interval` / `focus.changed` — time-by-repo/workspace/agent usage
- [ ] `agent.model` + `agent.session_usage` — **blocked on herdr:** requires a
  sanctioned agent-session journal path from the socket API (herdr 0.7.1
  exposes none, and its `agent.explain` previews scrape live terminal text we
  refuse to read). Revisit if a future herdr surfaces a model/backend field or
  a path we can parse counters-only from.
- [x] per-run model + token counters (counters-only session-file reader)
- [ ] richer run context via `pane report-metadata` titles (opt-in)
- [ ] `events.wait`-based low-latency mode behind a flag
- [x] prebuilt release binaries so Go isn't required at install
- [ ] Windows support (named-pipe socket)

## License

[MIT](LICENSE)
