# Lite-NMS v2 — Design Spec

**Date:** 2026-07-04
**Author:** Jay Patel
**Status:** Approved (design), pending implementation plan
**Repos:** `Lite-NMS` (Java Vert.x backend) · `NMSLITE_PLUGIN` (Go plugin engine)

---

## 1. Purpose & goals

Rearchitect the existing internship-grade Lite-NMS into a credible, resume-worthy
Network Monitoring System that:

- Monitors devices over **three real protocols**: **Linux (SSH)**, **SNMP**, **WinRM**.
- Keeps the **two-process architecture** that mirrors the production Motadata design:
  a reactive **Java Vert.x** backend + a **Go plugin engine** spawned as a subprocess.
- Fixes the correctness, schema, and security defects found in the v1 review.
- Adds **availability/uptime monitoring** and **system self-observability**
  (health, Prometheus metrics, structured logging).
- Is demonstrably the author's own work — Motadata repos are used only as
  **architectural inspiration**, never copied.

**Non-goals for this cycle:** the UI (separate design/plan cycle), alerting/thresholds,
and a dedicated time-series datastore. Metric history stays in Postgres.

### Scope & sequencing
This is **cycle 1 of 2**. Cycle 1 = backend + 3-protocol plugin engine (this spec).
Cycle 2 = UI on top of the finished, stable API (future spec).

---

## 2. Reference & IP boundary

The Motadata backend, UI, and Go Plugin Engine are proprietary employer code. They are
used here **only** as pattern/architecture reference. Nothing proprietary — code,
schemas, OID tables, assets — is copied into these repos. Patterns adopted (all common,
non-proprietary engineering practice):

- Subprocess-per-batch plugin invocation with a base64-JSON envelope in / base64 results out.
- A `Discover(ctx, out) / Collect(ctx, out)` plugin method pair.
- A context/envelope map carrying an `event_type` discriminator.

Patterns **improved** vs. the reference: a `map`-based plugin **registry** instead of a
hand-written if/else dispatch chain, and a **bounded worker pool with a parent deadline**
instead of unbounded goroutines.

---

## 3. Architecture

```
┌─────────────────────────── Java Vert.x backend (Lite-NMS) ─────────────────┐
│  Server            HTTP/REST + JWT auth                                     │
│    handlers:       Auth · Credential · Discovery · Provision · Metric/Hist  │
│  Database          event-bus DB layer → Postgres (vertx-pg-client, pooled)  │
│  Discovery         fping reachability (bounded) → engine discovery run      │
│  Scheduler         per-metric interval countdown (in-memory cache)          │
│  Polling           builds due-job batches → PluginRunner                    │
│  Availability      up/down state + rolling uptime %                         │
│  PluginRunner      (WORKER verticle) spawns Go engine subprocess            │
│  ResponseProcessor batches results → Database                               │
│  Observability     /health · /metrics (Micrometer→Prometheus) · JSON logs   │
└───────────────────────────────┬────────────────────────────────────────────┘
                base64-JSON envelope file in / base64 result lines on stdout
                                 ▼
┌──────────────────── Go plugin engine (NMSLITE_PLUGIN, spawned per batch) ───┐
│  main         decode envelope → registry.dispatch(plugin_type, event_type)  │
│  registry     map[Key]Plugin        (replaces if/else chain)                │
│  Plugin iface Discover(ctx, out) / Collect(ctx, out)                        │
│  pool         bounded worker pool (semaphore) + parent context deadline     │
│  collectors   linux/ (SSH)   snmp/ (gosnmp)   winrm/ (WinRM lib)            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component boundaries (each independently testable)
Registry, each collector, the envelope codec, the scheduler decrement logic, and the
availability state machine are pure/isolated units with defined interfaces — unit-testable
without a live device.

---

## 4. Plugin-engine contract (the centerpiece rework)

Keep subprocess spawn; make it typed and robust.

### 4.1 Envelope (backend → engine)
A single versioned JSON envelope, written by the backend to a temp file the engine reads
and **deletes** (credentials off the pipe/argv quickly):

```json
{
  "version": 1,
  "event_type": "discovery | poll",
  "targets": [
    {
      "request_id": "uuid",
      "job_id": 123,
      "plugin_type": "LINUX | SNMP | WINRM",
      "ip": "10.0.0.5",
      "port": 22,
      "credential": { "...": "protocol-specific" },
      "metrics": ["CPU", "MEMORY", "DISK", "NETWORK", "UPTIME"]
    }
  ]
}
```

### 4.2 Result line (engine → backend, one base64-JSON line per target)
```json
{
  "request_id": "uuid",
  "event_type": "poll",
  "plugin_type": "LINUX",
  "job_id": 123,
  "status": "success | failed",
  "result": { "CPU": { "...": 0 } },
  "error": null
}
```

`request_id` / `event_type` are **always echoed**. This is a first-class discriminator —
it removes v1's `request.type`-never-set routing bug where every result was dropped.

### 4.3 Robustness rules
- **Concurrent stdout+stderr draining** in `PluginRunner` (separate reads) → fixes the
  v1 pipe-buffer **deadlock**.
- Hard **per-batch timeout** via `process.waitFor(timeout)`; per-collection
  `context.WithTimeout` inside each Go plugin.
- **Registry** dispatch: `map[typeKey]Plugin`; adding a protocol = register one struct.
- **Bounded worker pool** (semaphore sized to CPU) in both discovery and polling → fixes
  v1's unbounded-goroutine polling and the batch-abort `return` bug (→ per-target error + `continue`).
- **No secrets in logs**; SSH `known_hosts` verification (not `InsecureIgnoreHostKey`),
  SNMP community/v3 creds, WinRM auth — all configurable.

---

## 5. Collectors (all three real)

| Protocol | Library | Metrics (initial) | Notes |
|----------|---------|-------------------|-------|
| Linux    | `golang.org/x/crypto/ssh` | CPU, MEMORY, DISK, NETWORK, UPTIME | `known_hosts` verification; commands emit JSON |
| SNMP     | `gosnmp` | sysUpTime, CPU, MEMORY, DISK, interface counters | v2c + v3; GET/WALK standard OIDs |
| WinRM    | `masterzen/winrm` | CPU, MEMORY, DISK | PowerShell/CIM queries |

Collection uses the author's real devices for integration verification.

---

## 6. Data model (reconcile the schema — single source of truth)

Fix v1's schema/query drift and standardize **one** status vocabulary across
schema CHECK ↔ Java constants ↔ Go output.

- `credential_profile.cred_data` — **encrypted at rest** (app-level AES envelope, key from env).
- `discovery_profiles.status` — enum/varchar `PENDING | RUNNING | COMPLETED | FAILED`.
- `discovery_result.result` — `COMPLETED | FAILED` (matched by both sides).
- `metrics` — add `is_enabled BOOLEAN NOT NULL DEFAULT TRUE`; define `metric_name` as a
  real Postgres enum (`CREATE TYPE metric_name AS ENUM (...)`) so the `::metric_name` casts
  resolve; `plugin_type IN (LINUX, SNMP, WINRM)`.
- `polled_data` — timestamped rows keyed by `job_id` + `metric_type`; history endpoints read here.
- **New** `device_availability` — current up/down state + `availability_pct` rollup per device.

DDL runs **sequentially** (FK-safe), replacing v1's concurrent `CompositeFuture.all` DDL.

---

## 7. Runtime flows

- **Discovery:** create profile → bounded `fping` reachability → engine `discovery` run per
  protocol → store per-IP `COMPLETED/FAILED` (failure branch now handled, not swallowed) →
  set profile status.
- **Polling:** Scheduler decrements per-metric interval counters in the in-memory cache
  (all mutation confined to one verticle → fixes the shared-`JsonObject` race) → due jobs
  batched → `PluginRunner` spawns engine → results routed by the always-present
  `event_type` → batched insert.
- **Availability:** lightweight reachability (ping + protocol connect) updates each device's
  up/down state and rolling uptime %; exposed via API.
- **Observability:** `/health` (liveness/readiness), `/metrics` (Micrometer→Prometheus:
  poll latency, engine exit codes, batch sizes, queue depth), structured JSON logs with
  request IDs, credentials redacted.

---

## 8. Security fixes (baked in)

- Externalize `DB_PASSWORD` / `JWT_SECRET` to env/config (Vert.x `ConfigRetriever`);
  rotate to a long random JWT key.
- Encrypt device credentials at rest (§6).
- Redact secrets from all logs (v1 logged full cred payloads at INFO).
- SSH `known_hosts` verification in the Linux collector.
- Fix double HTTP response in `Credential.create` (missing `return`).

---

## 9. Testing

- **Backend:** Testcontainers-Postgres integration tests exercising the real schema (would
  have caught the v1 drift); unit tests for `Validator`, IP resolution, scheduler decrement,
  availability state machine; WebClient API tests covering auth + error branches (incl. the
  double-response path).
- **Engine:** Go table-driven unit tests for envelope codec, registry dispatch, worker-pool
  bounding, and each collector's output parsing (fixtures, not live devices). Optional
  `-tags integration` tests against the author's real devices.
- **CI:** GitHub Actions — `mvn verify` + `go test ./...`; status badge in README.

---

## 10. Delivery / git strategy

- Personal GitHub account **`Jay100125`**; commit identity **Jay Patel
  `<jaypatel100125@gmail.com>`** (already configured on both repos).
- Work on a **`v2` feature branch** in each repo; incremental, well-scoped commits (the
  commit history is part of the resume narrative).
- Rewrite READMEs into architecture docs (diagram, protocol matrix, run instructions;
  UI screenshots added in cycle 2).
- Remove committed cruft (`output.txt`, `jayP`, `.idea/`, stray binaries) via `.gitignore`.
- CI workflow (§9) included in this cycle.

---

## 11. Defects fixed from v1 review (traceability)

| v1 finding | Fix location in this design |
|------------|------------------------------|
| `request.type` routing drops all results | §4.2 always-echoed discriminator |
| ProcessBuilder stdout/stderr deadlock | §4.3 concurrent draining |
| schema ↔ query drift (`is_enabled`, `metric_name`) | §6 |
| status/result vocabulary mismatch | §6 single vocabulary |
| hardcoded `DB_PASSWORD` / weak `JWT_SECRET` | §8 externalize + rotate |
| device creds plaintext + logged | §6 encryption, §8 redaction |
| unbounded polling goroutines / batch-abort `return` | §4.3 bounded pool + continue |
| swallowed discovery failure | §7 discovery failure branch |
| double HTTP response in Credential.create | §8 |
| concurrent DDL vs FK order | §6 sequential DDL |
| no real tests | §9 |
