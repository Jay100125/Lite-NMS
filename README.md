# Lite-NMS

A lightweight network monitoring system: a **Java / Vert.x** backend that discovers
devices, provisions metric-collection jobs, and polls them on a schedule by spawning a
**Go plugin engine** ([`NMSLITE_PLUGIN`](https://github.com/Jay100125/NMSLITE_PLUGIN)) that
speaks three real protocols — **Linux (SSH)**, **SNMP**, and **WinRM**.

This is the **v2** rearchitecture: a typed engine contract, a reconciled schema, credentials
encrypted at rest, availability/uptime tracking, and self-observability — all covered by
integration tests against a real PostgreSQL.

## Architecture

```
┌─────────────────────── Java Vert.x backend (this repo) ─────────────────────┐
│  Server            HTTP/REST + JWT auth · /health · /metrics                 │
│    handlers:       Auth · Credential · Discovery · Provision · Metric/Hist   │
│  Database          event-bus DB layer → Postgres (vertx-pg-client, pooled)   │
│  Discovery         reachability → engine discovery run (failure branch handled)│
│  Scheduler         per-metric interval countdown (in-memory cache, one owner)│
│  Polling           builds due-job batches → v2 envelope → Plugin             │
│  Availability      up/down state + rolling uptime %                          │
│  Plugin            (WORKER verticle) spawns Go engine; drains stdout+stderr   │
│  ResponseProcessor routes results by event_type → batches → Database         │
└───────────────────────────────┬─────────────────────────────────────────────┘
              base64-JSON envelope file in / base64 result lines on stdout
                                 ▼
┌──────────────── Go plugin engine (NMSLITE_PLUGIN, spawned per batch) ────────┐
│  registry dispatch (plugin_type × event_type) → collectors:                  │
│  linux/ (SSH)   ·   snmp/ (gosnmp)   ·   winrm/ (WinRM)                       │
└──────────────────────────────────────────────────────────────────────────────┘
```

The backend and engine are **two separate repositories** communicating over a versioned
envelope contract — the engine is spawned as a subprocess per batch.

## Protocol matrix

| Protocol | Library | Metrics (initial) | Default port |
|----------|---------|-------------------|--------------|
| Linux    | `golang.org/x/crypto/ssh` | CPU, MEMORY, DISK, NETWORK, UPTIME | 22 |
| SNMP     | `gosnmp` | sysUpTime, CPU, MEMORY, DISK, interface counters | 161 |
| WinRM    | `masterzen/winrm` | CPU, MEMORY, DISK | 5985 |

## Engine contract

**Envelope** (backend → engine), written to a temp file the engine reads and deletes:

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
      "credential": { "username": "...", "password": "..." },
      "metrics": ["CPU", "MEMORY", "DISK"]
    }
  ]
}
```

**Result line** (engine → backend, one base64-JSON line per target on stdout):

```json
{ "request_id": "uuid", "event_type": "poll", "plugin_type": "LINUX",
  "job_id": 123, "status": "success | failed", "result": { "CPU": {} }, "error": null }
```

`request_id` and `event_type` are always echoed — `event_type` is the first-class
discriminator used to route every result (fixing v1's dropped-result routing bug).

## Configuration

All secrets come from the environment (never source or logs). Host/port/name have
dev-safe defaults; **override the secrets in every real deployment**.

| Variable | Purpose | Default |
|----------|---------|---------|
| `NMS_DB_HOST` / `NMS_DB_PORT` / `NMS_DB_NAME` | Postgres connection | `localhost` / `5432` / `nms` |
| `NMS_DB_USER` / `NMS_DB_PASSWORD` | Postgres credentials | `nms` / `nms` |
| `NMS_JWT_SECRET` | JWT signing key (32+ chars) | dev-only placeholder |
| `NMS_CRED_KEY` | base64 AES-256 key (32 bytes) for credential encryption at rest | dev-only placeholder |

## Build & run

**Engine** (separate repo):

```bash
git clone https://github.com/Jay100125/NMSLITE_PLUGIN
cd NMSLITE_PLUGIN && go build -o Lite_NMS_Plugin .
# place/symlink the binary at ./plugin/Lite_NMS_Plugin under the backend
```

**Backend:**

```bash
./mvnw -DskipTests package
NMS_DB_PASSWORD=... NMS_JWT_SECRET=... NMS_CRED_KEY=... \
  java -jar target/*-fat.jar
```

## Tests

Integration tests run against a **local PostgreSQL** (no Docker): each run creates and
drops a uniquely-named throwaway database and applies `schema.sql`. Provision the test role
once as the postgres superuser:

```bash
sudo -u postgres psql -f scripts/test-db-setup.sql
./mvnw verify
```

Override the test connection with `NMS_TEST_DB_HOST` / `NMS_TEST_DB_PORT` /
`NMS_TEST_DB_USER` / `NMS_TEST_DB_PASSWORD` / `NMS_TEST_DB_ADMIN`.

## Observability

- `GET /health` → `200 {"status":"UP"}`
- `GET /metrics` → Prometheus text exposition (Micrometer)
- Structured JSON logs with credentials redacted

## CI

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the backend
`mvn verify` against a Postgres service container and the engine's `go test ./...`.

## Roadmap

- **Cycle 1 (current):** backend + 3-protocol engine, availability, observability,
  credential encryption, integration tests, CI.
- **Cycle 2:** web UI over the finished API. _(screenshots to follow)_
