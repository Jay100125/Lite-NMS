# Discovery Realtime Redesign — Design

**Date:** 2026-07-07
**Repos:** `Lite-NMS` (Java Vert.x backend) + `NMSLITE_UI` (React UI)
**Reference (architecture inspiration only — IP boundary, no code copied):** Motadata UI/backend discovery-progress flow (SockJS event-bus bridge, staged per-IP progress at 33/66/100).

## Goal

Replace the drawer-based, poll-only discovery UX with a full-screen flow that shows
live per-IP discovery progress over a SockJS event-bus bridge — ping check (33%),
port check (66%), plugin response (100% success/fail) — accepting IP / IP-range /
CIDR targets. Give each provisioned device a drilldown with metric charts plus a
raw polled-data grid. Remove the fleet-average availability dashboard (keep the
per-device availability panel and the whole backend availability feature).

## Decisions (locked with user)

1. **Screen flow:** separate routes — `/discovery` list → `/discovery/:id` detail
   → Run → `/discovery/:id/progress` (live) → done → `/discovery/:id/result`.
2. **Target inputs:** IP, IP range (`192.168.1.10-120`), CIDR (`192.168.1.0/24`).
   No CSV upload.
3. **Availability:** remove the dashboard avg-uptime KPI + per-device uptime table;
   keep the per-device `AvailabilityPanel` and the entire backend feature.
4. **Home route `/`:** slim dashboard — count cards (Total devices, Up, Down) +
   quick links. No averaging.
5. **Drilldown (`/provisioning/:id`):** metric config, Highcharts time-series,
   **new raw polled-data grid**, availability panel — stacked, no toggle.
6. **Transport:** Vert.x SockJS event-bus bridge (option A), reference-faithful.

## Backend (Lite-NMS)

### SockJS bridge

- Mount `SockJSHandler` bridge at `/eventbus/*` in `Server.java`. No new
  dependency (`vertx-web` 4.5.x ships it).
- `SockJSBridgeOptions`: **outbound-only**, permitted address regex
  `nms\.discovery\.[0-9]+` (one address per run, e.g. `nms.discovery.42`).
  No inbound permitted addresses.
- **Auth:** SockJS cannot send an `Authorization` header. A handler on the
  `/eventbus` route validates a JWT passed as `?access_token=<jwt>` using the
  same `JWTAuth` provider as the REST API; invalid/missing token → 401, socket
  never upgrades.

### Staged progress events

`Utility.checkReachability` is split into two pure steps so the `Discovery`
verticle can publish between stages:

- `pingCheck(ips)` — fping batch, returns alive set.
- `portCheck(aliveIps, port)` — TCP connect (1s timeout), returns open set.

All events publish to `nms.discovery.<discoveryId>`, discriminated by `type`:

| Trigger | Payload |
|---|---|
| Run starts (status → RUNNING) | `{type:"state", status:"RUNNING"}` |
| Targets expanded | `{type:"targets", total:<n>, ips:[...]}` |
| fping done, per IP alive | `{type:"progress", ip, stage:"PING", progress:33.33, status:"ok"}` |
| fping done, per IP dead | `{type:"progress", ip, stage:"PING", progress:100, status:"failed", message:"ping failed"}` |
| port check, per IP open | `{type:"progress", ip, stage:"PORT", progress:66.66, status:"ok"}` |
| port check, per IP closed | `{type:"progress", ip, stage:"PORT", progress:100, status:"failed", message:"port not reachable"}` |
| each plugin result line (`ResponseProcessor`) | `{type:"progress", ip, stage:"PLUGIN", progress:100, status:"COMPLETED"\|"FAILED", message}` |
| profile completes (`EVENT_COMPLETION`) | `{type:"state", status:"COMPLETED"}` |
| run aborts on internal error | `{type:"state", status:"FAILED", message}` |

Notes:

- **No in-memory progress cache.** Ping/port failures and each plugin result are
  already persisted to `discovery_result` as they happen (COMPLETED-wins upsert,
  unique per `(discovery_id, ip)`), so a page reload rebuilds state from
  `GET /api/discovery/:id` (status) + `GET /api/discovery/:id/result`; SockJS is
  a live overlay only.
- **Aggregate %** (top bar, Total/Discovered/Failed tiles) is computed
  client-side from the `targets` event + per-IP events.
- **Multi-credential wrinkle:** the engine emits one result line per
  `(ip, credential)`. The UI treats the first `COMPLETED` per IP as terminal;
  persisted upsert remains source of truth.
- Publishes are fire-and-forget; event publishing must never fail a run.
- Existing behavior unchanged: `plugin_type` still hardcoded `LINUX`; dead/closed
  IPs never reach the plugin; run endpoint still returns 200 immediately.

## UI (NMSLITE_UI)

### Routes

| Route | Screen |
|---|---|
| `/` | Slim dashboard: Total devices / Up / Down count cards (from per-job availability `is_up`; no averaging) + quick links. |
| `/discovery` | List (unchanged columns). "New" navigates to `/discovery/new` (drawer deleted). |
| `/discovery/new`, `/discovery/:id/edit` | One full-page form: name; target **type selector (IP / IP Range / CIDR)** with per-type zod validation; port; credential multi-select. Wire format unchanged (`ip.address` dotted key — backend parses all three shapes). |
| `/discovery/:id` | Profile detail: field summary, Edit / Delete / **Run**. Run → navigate to progress. |
| `/discovery/:id/progress` | Full-page live view: overall progress bar + %, stat tiles (Total / Discovered / Failed), per-IP table — stage chip (Ping → Port → Plugin), per-row 33/66/100 bar, failure message. Auto-navigate to result on `COMPLETED` state event. |
| `/discovery/:id/result` | Persisted result table (moved from old detail page), keeps checkbox → Provision-selected flow. |
| `/provisioning/:id` | MetricConfigPanel → MetricCharts → **new PolledDataGrid** (timestamp, metric type, values; paginated, newest first) → AvailabilityPanel. |

### Eventbus client

- New deps: `sockjs-client`, `@vertx/eventbus-bridge-client.js`.
- `src/lib/eventbus.ts`: lazy singleton connection to `/eventbus?access_token=<jwt>`,
  auto-reconnect; exposes `subscribe(address, handler) → unsubscribe`. Connects on
  demand (only the progress page uses it), closes when unused.
- `useDiscoveryProgress(id)`: subscribes to `nms.discovery.<id>`, reduces events
  into `{rows, tiles, overallPct, state}`; seeded on mount from persisted results.

### Fallback / degraded mode

Existing 3s TanStack `refetchInterval` polling of results stays. If the socket is
not connected, rows still advance from polled `discovery_result` (without
intermediate ping/port stages) and the page shows a "live updates unavailable"
indicator. On reconnect, state re-seeds from persisted results (no missed-event
drift).

## Error handling

- Socket drop mid-run → auto-reconnect + polling fallback + re-seed on reconnect.
- Reload mid-run → seed from status + results; live events overlay.
- fping unavailable / target-resolution error → profile `FAILED` (existing) plus
  `{type:"state", status:"FAILED", message}` so the page shows the reason.
- Bad JWT on `/eventbus` → 401, UI degrades to polling; REST 401 still logs out.
- Zero alive IPs → run completes normally (state event + empty/failed results).

## Testing

**Backend (JUnit, existing patterns; keep current 23 green):**
- `/eventbus` rejects missing/invalid token, accepts valid.
- Discovery run publishes the expected event sequence on `nms.discovery.<id>`
  (assert by consuming the event bus in-process; no SockJS client needed).
- `pingCheck` / `portCheck` unit tests for stage outputs.

**UI (Vitest + RTL + MSW; eventbus module mocked):**
- Per-type target validation schemas.
- Progress reducer: event sequence → correct rows/tiles/percentages.
- Seeding from persisted results; completion event navigates to result.
- PolledDataGrid rendering; slim dashboard counts without avg.

## Out of scope

CSV target upload, abort-discovery, per-user event addresses, SNMP/WinRM
discovery (plugin_type stays LINUX), removing the backend availability feature.
