# Counter Expansion — Motadata-style Metrics

**Date:** 2026-07-10
**Status:** Approved, in implementation
**Scope:** Expand the metrics collected by the NMS from a thin per-category set (~9 Linux keys) to a rich Motadata-style set: **host-level counters + per-instance tables** (per-core, per-volume, per-interface, top-N processes), across **LINUX, SNMP, and WINRM** collectors. Sequencing: **counters first**, chart/UI restyle last.

Spans three repos:
- `NMSLITE_PLUGIN` (Go collector engine) — `/home/jay-patel/personal/NMSLITE_PLUGIN`
- `Lite-NMS` (Java/Vert.x backend) — `/home/jay-patel/personal/Lite-NMS`
- `NMSLITE_UI` (React frontend) — `/home/jay-patel/personal/NMSLITE_UI`

Reference (read-only): Motadata engine `/home/jay-patel/workspace/PluginEngine` (metric catalog `plugin-metrics/*.json`) and Motadata UI `/home/jay-patel/workspace/UI` (Highcharts theme).

---

## 1. Problem

Today the engine returns `RESULT = {CPU:{…}, MEMORY:{…}}` and `ResponseProcessor` writes **one `polled_data` row per category** (`metric_type`=category, `data`=JSON blob). Only one scalar per category is typically present, and there is no notion of *instances* (multiple disks, interfaces, cores, processes). We want Motadata-like breadth while keeping Lite-NMS small.

## 2. Data model

Single additive schema change — `polled_data.instance`:

```sql
ALTER TABLE polled_data ADD COLUMN IF NOT EXISTS instance VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_polled_data_job_metric_instance
  ON polled_data (job_id, metric_type, instance, polled_at);
```

- **Host counters** → row with `instance = NULL` (unchanged behavior).
- **Instance counters** → one row per instance, same `metric_type` (category), `instance` = e.g. `"sda"`, `"eth0"`, `"cpu0"`, or a PID/name.

The `metric_name` enum (`CPU,MEMORY,DISK,NETWORK,PROCESS,UPTIME`) and per-job metric config (`metrics` table) are **unchanged** — instances are collected as part of their parent category. Enabling `DISK` polls the host disk aggregate **and** its per-volume instances.

## 3. Engine output contract (backward-compatible)

Each collector emits, per requested category key, a JSON object of host counters plus an **optional** `instances` array:

```json
"DISK": {
  "system_disk_capacity_bytes": 501809635328,
  "system_disk_used_bytes": 319708364800,
  "system_disk_used_percent": 63.71,
  "instances": [
    {"instance": "/",     "system_disk_volume_capacity_bytes": 0, "system_disk_volume_used_percent": 0},
    {"instance": "/home"}
  ]
}
```

Rules for `ResponseProcessor`:
- The category object **minus** the `instances` key → one host row (`instance = NULL`).
- Each element of `instances` → one row; `instance` = element's `instance` field; `data` = the element (the `instance` field may remain in the blob).
- A category object with no `instances` key → host row only (exactly today's behavior).

Result envelope struct (`protocol.Result.Result map[string]any`) is unchanged; the `instances` array lives inside each category value.

## 4. Backend changes (Lite-NMS)

- `src/main/resources/schema.sql` — add the `instance` column + index (idempotent).
- `QueryConstant.INSERT_POLLED_DATA` — add `instance`:
  `INSERT INTO polled_data (job_id, metric_type, instance, data, polled_at) VALUES ($1,$2,$3,$4::jsonb, to_timestamp($5/1000.0)) returning id`
- `QueryConstant.GET_ALL_POLLED_DATA` / `GET_POLLED_DATA_BY_JOB_ID` — add `instance` to the SELECT list.
- `ResponseProcessor.storePollResults` — for each category value, split out `instances`: emit a host `batchParams` row (`instance=null`, data minus `instances`) and one row per instance (`instance=<id>`). Batch param arity goes 4 → 5.
- Polled-data API DTO / UI type gains `instance` (nullable).

No change to scheduler, availability, discovery, or metric-config flows.

## 5. Counter catalog

Keys keep the existing `snake_case` convention (mirroring Motadata's *coverage*, not its dotted names). `bytes` for sizes, `percent` for ratios, `per_sec` for rates. Rates (cpu %, net bytes/sec) are computed in-collector from two samples ~1s apart.

### LINUX (SSH) — Phase 1
- **CPU** host: `system_cpu_percent`, `system_cpu_user_percent`, `system_cpu_kernel_percent`, `system_cpu_idle_percent`, `system_cpu_io_percent`, `system_cpu_cores`, `system_load_avg_1min`, `system_load_avg_5min`, `system_load_avg_15min`. **instances** (per core): `instance` (`cpu0`…), `system_cpu_core_percent`.
- **MEMORY** host: `system_memory_total_bytes`, `system_memory_used_bytes`, `system_memory_free_bytes`, `system_memory_available_bytes`, `system_memory_cached_bytes`, `system_memory_buffer_bytes`, `system_memory_used_percent`, `system_memory_free_percent`, `system_swap_total_bytes`, `system_swap_used_bytes`, `system_swap_free_bytes`, `system_swap_used_percent`.
- **DISK** host (root fs): `system_disk_capacity_bytes`, `system_disk_used_bytes`, `system_disk_free_bytes`, `system_disk_used_percent`, `system_disk_free_percent`. **instances** (per mounted volume): `instance` (mount path), `system_disk_volume_capacity_bytes`, `system_disk_volume_used_bytes`, `system_disk_volume_free_bytes`, `system_disk_volume_used_percent`.
- **NETWORK** host: `system_network_in_bytes_per_sec`, `system_network_out_bytes_per_sec`, `system_network_tcp_connections`, `system_network_udp_connections`. **instances** (per interface): `instance` (`eth0`…), `system_network_interface_in_bytes_per_sec`, `system_network_interface_out_bytes_per_sec`.
- **PROCESS** host: `system_process_count`, `system_running_processes`, `system_blocked_processes`. **instances** (top-N by CPU, N=10): `instance` (pid), `system_process_name`, `system_process_cpu_percent`, `system_process_memory_used_percent`.
- **UPTIME** host: `system_uptime_seconds`.

### SNMP — Phase 2
- **CPU** host: `system_cpu_percent` (avg of `hrProcessorLoad` `.1.3.6.1.2.1.25.3.3.1.2`). **instances**: per-processor load.
- **MEMORY**/**DISK** host + instances: Host-Resources storage table `.1.3.6.1.2.1.25.2.3.1` (types Physical/Virtual memory, FixedDisk) → capacity/used/free/percent; per-storage-entry instances.
- **NETWORK** instances: IF-MIB (`ifDescr`, `ifOperStatus`, `ifInOctets`/`ifOutOctets` → byte rates from two walks) per interface.
- **UPTIME**: `sysUpTime` `.1.3.6.1.2.1.1.3.0`. **SYSTEM** (discovery): `sysDescr`, `sysName`.

### WINRM — Phase 3
- **CPU** host: `Win32_Processor.LoadPercentage` avg + per-core instances. **MEMORY**: `Win32_OperatingSystem` total/free + `Win32_PageFileUsage` swap. **DISK**: `Win32_LogicalDisk` per-drive instances + aggregate. **NETWORK**: `Win32_PerfFormattedData_Tcpip_NetworkInterface` per-nic instances + tcp/udp conns. **PROCESS**: top-N via `Get-Process`. **UPTIME**: `LastBootUpTime`.

## 6. UI implications (Phase 4)

- `PolledData` type gains `instance: string | null`.
- Host counters → multi-series time-series charts, restyled to the Motadata Highcharts theme (JetBrains Mono, vivid Tailwind-600 palette + brighter dark variants, `areaspline` with vertical color→transparent gradient fill, hidden-until-hover markers, frosted custom HTML tooltip, subtle gridlines, no chart chrome).
- Instance counters → per-category grids (rows = instances) with the latest sample, and an instance picker to chart a chosen instance over time.

## 7. Phasing

1. **Backend + Linux end-to-end** (this phase): schema/instance column, `ResponseProcessor` split, and the full Linux host+instance collector with Go unit tests. Proves the whole pipeline against real Linux devices.
2. **SNMP** collector (Host-Resources + IF-MIB).
3. **WinRM** collector.
4. **UI**: Motadata chart restyle + instance grids.

## 8. Testing

- Plugin: Go unit tests over captured raw `/proc` / `df` / `ps` / `ss` sample outputs → assert parsed host maps and instance arrays (no live SSH needed).
- Backend: existing suite must stay green; `ResponseProcessor` host/instance split covered against real Linux devices (12 provisioned jobs exist) and by inspecting `polled_data` rows (`instance` populated for DISK/NETWORK/CPU/PROCESS).
- End-to-end: rebuild the fat jar + engine binary, poll a real Linux device, confirm `instance`-keyed rows land in `polled_data`.

## 9. Non-goals

Full Motadata parity (496 device types / thousands of counters), new device types beyond the three, config/reporting/topology/compliance plugins, alerting. Instance cardinality is bounded (processes capped to top-N).
