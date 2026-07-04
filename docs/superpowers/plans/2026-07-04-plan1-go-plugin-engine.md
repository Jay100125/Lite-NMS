# Go Plugin Engine v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the `NMSLITE_PLUGIN` Go engine into a clean, typed, testable subprocess that collects metrics over Linux(SSH)/SNMP/WinRM and speaks the versioned envelope contract from the design spec.

**Architecture:** The backend spawns the compiled binary per batch, passing a temp-file path as `os.Args[1]` (falls back to stdin). The engine reads a base64-JSON **envelope**, dispatches each target through a `map`-based **registry** to a protocol **Plugin**, runs collection inside a **bounded worker pool** with per-target context deadlines and panic recovery, and writes one base64-JSON **result line** per target to stdout. Discriminator fields (`request_id`, `event_type`, `plugin_type`, `job_id`) are stamped by the engine before the plugin runs, so they are *always* present — even on plugin failure.

**Tech Stack:** Go 1.24, `golang.org/x/crypto/ssh`, `github.com/gosnmp/gosnmp`, `github.com/masterzen/winrm`, standard `testing`.

**Repo:** `/home/jay-patel/personal/NMSLITE_PLUGIN` — execute all tasks here on a `v2` branch.

## Global Constraints

- Module path stays `ssh_plugin` (existing `go.mod`); Go version `1.24`.
- Envelope contract is spec §4 — do not change field names; `request_id`/`event_type`/`plugin_type`/`job_id` are always echoed on every result.
- `plugin_type` values are exactly `LINUX`, `SNMP`, `WINRM`. `event_type` values are exactly `discovery`, `poll`. `status` values are exactly `success`, `failed`.
- No secrets in logs: never log credential fields or full envelope bodies. Logs go to **stderr** only (stdout is the result channel).
- SSH uses `known_hosts` verification, never `ssh.InsecureIgnoreHostKey()`.
- Every collection runs under a `context.Context` with a deadline; the engine bounds concurrency with a semaphore sized to `runtime.NumCPU()`.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

```
NMSLITE_PLUGIN/
  go.mod / go.sum
  main.go                         # entrypoint: read envelope (file arg or stdin) -> engine.Run
  internal/
    protocol/
      envelope.go                 # Envelope, Target, Result structs + Decode/Encode
      envelope_test.go
    registry/
      registry.go                 # Plugin interface, Register, Get
      registry_test.go
    engine/
      engine.go                   # Run: worker pool, dispatch, panic recovery, discriminator stamping
      engine_test.go
    sshclient/
      sshclient.go                # known_hosts-verified dial + run-command with timeout
    plugins/
      linux/
        linux.go                  # Plugin impl (Discover/Collect) over SSH
        commands.go               # metric -> shell command (JSON-emitting)
        parse.go                  # raw command output -> map[string]any
        parse_test.go
      snmp/
        snmp.go                   # Plugin impl over gosnmp
        oids.go                   # OID constants + metric mapping
        snmp_test.go
      winrm/
        winrm.go                  # Plugin impl over masterzen/winrm
        winrm_test.go
  scripts/build.sh                # go build -> ../Lite-NMS/plugin/Lite_NMS_Plugin
  README.md
```

The legacy `ssh/` package (`utils.go`, `polling.go`, `discovery.go`) and the old `main.go` are replaced. Task 10 deletes them once the new path is green.

---

### Task 1: Branch, dependencies, and package skeleton

**Files:**
- Modify: `go.mod`
- Create: `internal/protocol/envelope.go` (empty package stub), `internal/registry/registry.go` (stub), `internal/engine/engine.go` (stub)

**Interfaces:**
- Produces: three compilable empty packages `ssh_plugin/internal/protocol`, `.../registry`, `.../engine`.

- [ ] **Step 1: Create the v2 branch**

```bash
cd /home/jay-patel/personal/NMSLITE_PLUGIN
git checkout -b v2
```

- [ ] **Step 2: Add dependencies**

```bash
go get github.com/gosnmp/gosnmp@latest
go get github.com/masterzen/winrm@latest
go get golang.org/x/crypto/ssh@latest
```

- [ ] **Step 3: Create package stubs**

`internal/protocol/envelope.go`:
```go
// Package protocol defines the wire contract between the Java backend and this engine.
package protocol
```

`internal/registry/registry.go`:
```go
// Package registry maps a plugin_type to its Plugin implementation.
package registry
```

`internal/engine/engine.go`:
```go
// Package engine runs a batch of targets through the registry with bounded concurrency.
package engine
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./...`
Expected: exits 0, no output.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/
git commit -m "chore: scaffold v2 engine packages and add snmp/winrm deps

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Envelope + Result types and codec

**Files:**
- Modify: `internal/protocol/envelope.go`
- Test: `internal/protocol/envelope_test.go`

**Interfaces:**
- Produces:
  - `type Envelope struct { Version int; EventType string; Targets []Target }`
  - `type Target struct { RequestID string; JobID int64; PluginType string; IP string; Port int; Credential json.RawMessage; Metrics []string }`
  - `type Result struct { RequestID string; EventType string; PluginType string; JobID int64; Status string; Result map[string]any; Error string }`
  - `func DecodeEnvelope(line string) (Envelope, error)` — base64 → JSON → Envelope
  - `func EncodeResult(r Result) string` — Result → JSON → base64 (single line, no newline)
  - `func NewResult(t Target, eventType string) Result` — pre-stamps the four discriminators + `Status:"failed"`
  - Constants: `StatusSuccess = "success"`, `StatusFailed = "failed"`, `EventDiscovery = "discovery"`, `EventPoll = "poll"`, `TypeLinux = "LINUX"`, `TypeSNMP = "SNMP"`, `TypeWinRM = "WINRM"`

- [ ] **Step 1: Write the failing test**

`internal/protocol/envelope_test.go`:
```go
package protocol

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestDecodeEnvelope(t *testing.T) {
	raw := `{"version":1,"event_type":"poll","targets":[{"request_id":"r1","job_id":7,"plugin_type":"LINUX","ip":"10.0.0.5","port":22,"credential":{"username":"u"},"metrics":["CPU"]}]}`
	line := base64.StdEncoding.EncodeToString([]byte(raw))

	env, err := DecodeEnvelope(line)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Version != 1 || env.EventType != "poll" || len(env.Targets) != 1 {
		t.Fatalf("bad envelope: %+v", env)
	}
	tg := env.Targets[0]
	if tg.RequestID != "r1" || tg.JobID != 7 || tg.PluginType != "LINUX" || tg.IP != "10.0.0.5" || tg.Port != 22 {
		t.Fatalf("bad target: %+v", tg)
	}
	if len(tg.Metrics) != 1 || tg.Metrics[0] != "CPU" {
		t.Fatalf("bad metrics: %+v", tg.Metrics)
	}
}

func TestNewResultStampsDiscriminators(t *testing.T) {
	tg := Target{RequestID: "r9", JobID: 3, PluginType: "SNMP"}
	r := NewResult(tg, EventPoll)
	if r.RequestID != "r9" || r.JobID != 3 || r.PluginType != "SNMP" || r.EventType != EventPoll {
		t.Fatalf("discriminators not stamped: %+v", r)
	}
	if r.Status != StatusFailed {
		t.Fatalf("default status should be failed, got %q", r.Status)
	}
}

func TestEncodeResultRoundTrip(t *testing.T) {
	r := Result{RequestID: "r1", EventType: "poll", PluginType: "LINUX", JobID: 7, Status: "success", Result: map[string]any{"CPU": 1.0}}
	line := EncodeResult(r)
	decoded, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		t.Fatalf("not base64: %v", err)
	}
	var back Result
	if err := json.Unmarshal(decoded, &back); err != nil {
		t.Fatalf("json: %v", err)
	}
	if back.RequestID != "r1" || back.Status != "success" {
		t.Fatalf("round trip lost data: %+v", back)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/protocol/`
Expected: FAIL — `undefined: DecodeEnvelope` etc.

- [ ] **Step 3: Write the implementation**

`internal/protocol/envelope.go`:
```go
package protocol

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

const (
	StatusSuccess = "success"
	StatusFailed  = "failed"

	EventDiscovery = "discovery"
	EventPoll      = "poll"

	TypeLinux = "LINUX"
	TypeSNMP  = "SNMP"
	TypeWinRM = "WINRM"
)

// Envelope is the single JSON document the backend sends per batch.
type Envelope struct {
	Version   int      `json:"version"`
	EventType string   `json:"event_type"`
	Targets   []Target `json:"targets"`
}

// Target is one device/metric-set to collect.
type Target struct {
	RequestID  string          `json:"request_id"`
	JobID      int64           `json:"job_id"`
	PluginType string          `json:"plugin_type"`
	IP         string          `json:"ip"`
	Port       int             `json:"port"`
	Credential json.RawMessage `json:"credential"`
	Metrics    []string        `json:"metrics"`
}

// Result is one line of engine output per target.
type Result struct {
	RequestID  string         `json:"request_id"`
	EventType  string         `json:"event_type"`
	PluginType string         `json:"plugin_type"`
	JobID      int64          `json:"job_id"`
	Status     string         `json:"status"`
	Result     map[string]any `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
}

// DecodeEnvelope parses one base64-encoded JSON envelope line.
func DecodeEnvelope(line string) (Envelope, error) {
	var env Envelope
	jsonBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(line))
	if err != nil {
		return env, err
	}
	err = json.Unmarshal(jsonBytes, &env)
	return env, err
}

// EncodeResult renders a Result as a single base64(JSON) string (no trailing newline).
func EncodeResult(r Result) string {
	b, err := json.Marshal(r)
	if err != nil {
		// Marshal of a Result cannot realistically fail; emit a minimal failed line.
		b = []byte(`{"status":"failed","error":"marshal error"}`)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// NewResult pre-stamps discriminators so every result routes correctly even on failure.
func NewResult(t Target, eventType string) Result {
	return Result{
		RequestID:  t.RequestID,
		EventType:  eventType,
		PluginType: t.PluginType,
		JobID:      t.JobID,
		Status:     StatusFailed,
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/protocol/`
Expected: PASS (ok).

- [ ] **Step 5: Commit**

```bash
git add internal/protocol/
git commit -m "feat(protocol): envelope/result types and base64 codec

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Plugin registry

**Files:**
- Modify: `internal/registry/registry.go`
- Test: `internal/registry/registry_test.go`

**Interfaces:**
- Consumes: `protocol.Target`, `protocol.Result` (Task 2).
- Produces:
  - `type Plugin interface { Discover(ctx context.Context, t protocol.Target) protocol.Result; Collect(ctx context.Context, t protocol.Target) protocol.Result }`
  - `func Register(pluginType string, p Plugin)`
  - `func Get(pluginType string) (Plugin, bool)`

- [ ] **Step 1: Write the failing test**

`internal/registry/registry_test.go`:
```go
package registry

import (
	"context"
	"testing"

	"ssh_plugin/internal/protocol"
)

type fakePlugin struct{}

func (fakePlugin) Discover(_ context.Context, t protocol.Target) protocol.Result {
	r := protocol.NewResult(t, protocol.EventDiscovery)
	r.Status = protocol.StatusSuccess
	return r
}
func (fakePlugin) Collect(_ context.Context, t protocol.Target) protocol.Result {
	r := protocol.NewResult(t, protocol.EventPoll)
	r.Status = protocol.StatusSuccess
	return r
}

func TestRegisterAndGet(t *testing.T) {
	Register("FAKE", fakePlugin{})
	p, ok := Get("FAKE")
	if !ok {
		t.Fatal("expected plugin registered")
	}
	res := p.Collect(context.Background(), protocol.Target{PluginType: "FAKE"})
	if res.Status != protocol.StatusSuccess {
		t.Fatalf("unexpected: %+v", res)
	}
	if _, ok := Get("MISSING"); ok {
		t.Fatal("expected missing plugin to be absent")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/registry/`
Expected: FAIL — `undefined: Register`.

- [ ] **Step 3: Write the implementation**

`internal/registry/registry.go`:
```go
package registry

import (
	"context"

	"ssh_plugin/internal/protocol"
)

// Plugin is implemented by every protocol collector.
type Plugin interface {
	Discover(ctx context.Context, t protocol.Target) protocol.Result
	Collect(ctx context.Context, t protocol.Target) protocol.Result
}

var plugins = map[string]Plugin{}

// Register adds a plugin for a plugin_type. Call from package init().
func Register(pluginType string, p Plugin) {
	plugins[pluginType] = p
}

// Get returns the plugin for a plugin_type.
func Get(pluginType string) (Plugin, bool) {
	p, ok := plugins[pluginType]
	return p, ok
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/registry/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/registry/
git commit -m "feat(registry): plugin interface and type->plugin map

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Engine — bounded worker pool, dispatch, panic recovery

**Files:**
- Modify: `internal/engine/engine.go`
- Test: `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `protocol` (Task 2), `registry.Get` (Task 3).
- Produces:
  - `func Run(ctx context.Context, env protocol.Envelope, out io.Writer, perTargetTimeout time.Duration)` — dispatches every target concurrently (bounded by `runtime.NumCPU()`), writes one base64 result line per target to `out`, serialized by a mutex.
  - Behavior: unknown `plugin_type` → a failed result with the discriminators set and `Error` explaining it; a plugin that panics → a failed result, not a crash.

- [ ] **Step 1: Write the failing test**

`internal/engine/engine_test.go`:
```go
package engine

import (
	"bufio"
	"context"
	"strings"
	"testing"
	"time"

	"ssh_plugin/internal/protocol"
	"ssh_plugin/internal/registry"
)

type okPlugin struct{}

func (okPlugin) Discover(_ context.Context, t protocol.Target) protocol.Result {
	r := protocol.NewResult(t, protocol.EventDiscovery)
	r.Status = protocol.StatusSuccess
	return r
}
func (okPlugin) Collect(_ context.Context, t protocol.Target) protocol.Result {
	r := protocol.NewResult(t, protocol.EventPoll)
	r.Status = protocol.StatusSuccess
	r.Result = map[string]any{"CPU": 1.0}
	return r
}

type panicPlugin struct{}

func (panicPlugin) Discover(_ context.Context, _ protocol.Target) protocol.Result { panic("boom") }
func (panicPlugin) Collect(_ context.Context, _ protocol.Target) protocol.Result  { panic("boom") }

func decodeLines(t *testing.T, s string) []protocol.Result {
	t.Helper()
	var out []protocol.Result
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// EncodeResult -> decode by reusing DecodeEnvelope's base64 step through Result
		r, err := decodeResult(line)
		if err != nil {
			t.Fatalf("decode result: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func TestRunPollSuccess(t *testing.T) {
	registry.Register("OKP", okPlugin{})
	env := protocol.Envelope{Version: 1, EventType: protocol.EventPoll, Targets: []protocol.Target{
		{RequestID: "r1", JobID: 1, PluginType: "OKP", Metrics: []string{"CPU"}},
	}}
	var sb strings.Builder
	Run(context.Background(), env, &sb, time.Second)

	results := decodeLines(t, sb.String())
	if len(results) != 1 || results[0].Status != protocol.StatusSuccess || results[0].RequestID != "r1" {
		t.Fatalf("bad results: %+v", results)
	}
}

func TestRunUnknownPluginType(t *testing.T) {
	env := protocol.Envelope{Version: 1, EventType: protocol.EventPoll, Targets: []protocol.Target{
		{RequestID: "r2", JobID: 2, PluginType: "NOPE"},
	}}
	var sb strings.Builder
	Run(context.Background(), env, &sb, time.Second)
	results := decodeLines(t, sb.String())
	if len(results) != 1 || results[0].Status != protocol.StatusFailed || results[0].RequestID != "r2" {
		t.Fatalf("unknown type should fail with discriminators: %+v", results)
	}
}

func TestRunPanicIsContained(t *testing.T) {
	registry.Register("PANIC", panicPlugin{})
	env := protocol.Envelope{Version: 1, EventType: protocol.EventPoll, Targets: []protocol.Target{
		{RequestID: "r3", JobID: 3, PluginType: "PANIC"},
	}}
	var sb strings.Builder
	Run(context.Background(), env, &sb, time.Second) // must not panic
	results := decodeLines(t, sb.String())
	if len(results) != 1 || results[0].Status != protocol.StatusFailed {
		t.Fatalf("panic should be contained: %+v", results)
	}
}
```

Add the small helper `internal/engine/decode_test.go` (test-only decoder):
```go
package engine

import (
	"encoding/base64"
	"encoding/json"

	"ssh_plugin/internal/protocol"
)

func decodeResult(line string) (protocol.Result, error) {
	var r protocol.Result
	b, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/engine/`
Expected: FAIL — `undefined: Run`.

- [ ] **Step 3: Write the implementation**

`internal/engine/engine.go`:
```go
package engine

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"ssh_plugin/internal/protocol"
	"ssh_plugin/internal/registry"
)

// Run dispatches every target concurrently with bounded parallelism and writes
// one base64(JSON) result line per target to out.
func Run(ctx context.Context, env protocol.Envelope, out io.Writer, perTargetTimeout time.Duration) {
	sem := make(chan struct{}, max(1, runtime.NumCPU()))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, t := range env.Targets {
		wg.Add(1)
		go func(t protocol.Target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			res := dispatch(ctx, env.EventType, t, perTargetTimeout)

			mu.Lock()
			fmt.Fprintln(out, protocol.EncodeResult(res))
			mu.Unlock()
		}(t)
	}
	wg.Wait()
}

// dispatch resolves the plugin, applies a per-target deadline, and contains panics.
func dispatch(parent context.Context, eventType string, t protocol.Target, timeout time.Duration) (res protocol.Result) {
	res = protocol.NewResult(t, eventType)

	defer func() {
		if r := recover(); r != nil {
			res.Status = protocol.StatusFailed
			res.Error = fmt.Sprintf("plugin panic: %v", r)
		}
	}()

	plugin, ok := registry.Get(t.PluginType)
	if !ok {
		res.Error = fmt.Sprintf("unknown plugin_type %q", t.PluginType)
		return res
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	if eventType == protocol.EventDiscovery {
		return plugin.Discover(ctx, t)
	}
	return plugin.Collect(ctx, t)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/engine/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/engine/
git commit -m "feat(engine): bounded worker pool, timeout dispatch, panic containment

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Entrypoint — read envelope from file arg or stdin

**Files:**
- Replace: `main.go`
- Test: `main_test.go`

**Interfaces:**
- Consumes: `protocol.DecodeEnvelope` (Task 2), `engine.Run` (Task 4).
- Produces: `func readEnvelopeSource(args []string, stdin io.Reader) (string, error)` — returns the raw base64 line: if `len(args) > 1`, read+delete `args[1]`; else read all of stdin. `main()` wires it to `engine.Run(..., os.Stdout, 90s)`.

- [ ] **Step 1: Write the failing test**

`main_test.go`:
```go
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadEnvelopeSourceFromFileDeletes(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "ctx")
	if err := os.WriteFile(f, []byte("QUJD"), 0o600); err != nil { // "ABC"
		t.Fatal(err)
	}
	got, err := readEnvelopeSource([]string{"engine", f}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if strings.TrimSpace(got) != "QUJD" {
		t.Fatalf("got %q", got)
	}
	if _, err := os.Stat(f); !os.IsNotExist(err) {
		t.Fatal("envelope file should be deleted after read")
	}
}

func TestReadEnvelopeSourceFromStdin(t *testing.T) {
	got, err := readEnvelopeSource([]string{"engine"}, strings.NewReader("WFla"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if strings.TrimSpace(got) != "WFla" {
		t.Fatalf("got %q", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test .`
Expected: FAIL — `undefined: readEnvelopeSource` (and old `main.go` symbols may conflict; that is fixed in Step 3 by replacing the file).

- [ ] **Step 3: Replace main.go**

`main.go` (full replacement):
```go
package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"ssh_plugin/internal/engine"
	"ssh_plugin/internal/protocol"

	// register plugins via their init()
	_ "ssh_plugin/internal/plugins/linux"
	_ "ssh_plugin/internal/plugins/snmp"
	_ "ssh_plugin/internal/plugins/winrm"
)

const perTargetTimeout = 90 * time.Second

func main() {
	log.SetOutput(os.Stderr) // stdout is reserved for result lines

	line, err := readEnvelopeSource(os.Args, os.Stdin)
	if err != nil {
		log.Printf("[engine] read envelope: %v", err)
		os.Exit(1)
	}

	env, err := protocol.DecodeEnvelope(line)
	if err != nil {
		log.Printf("[engine] decode envelope: %v", err)
		os.Exit(1)
	}

	engine.Run(context.Background(), env, os.Stdout, perTargetTimeout)
}

// readEnvelopeSource returns the raw base64 envelope line. When a file path is
// passed as args[1], it is read and then deleted; otherwise stdin is read.
func readEnvelopeSource(args []string, stdin io.Reader) (string, error) {
	if len(args) > 1 {
		path := args[1]
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		_ = os.Remove(path)
		return string(b), nil
	}
	b, err := io.ReadAll(stdin)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
```

Note: this will not compile until Tasks 6-8 create the three `plugins/*` packages. To keep Step 4 runnable now, temporarily comment the three `_ "ssh_plugin/internal/plugins/..."` imports, run the test, then uncomment in Task 9. Record this in the commit message.

- [ ] **Step 4: Run test to verify it passes (with plugin imports temporarily commented)**

Run: `go test . -run TestReadEnvelopeSource`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat(main): envelope-file/stdin entrypoint wired to engine (plugin imports pending)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: SSH client helper + Linux collector

**Files:**
- Create: `internal/sshclient/sshclient.go`
- Create: `internal/plugins/linux/commands.go`, `internal/plugins/linux/parse.go`, `internal/plugins/linux/linux.go`
- Test: `internal/plugins/linux/parse_test.go`

**Interfaces:**
- Consumes: `protocol`, `registry`.
- Produces:
  - `sshclient.Run(ctx context.Context, cfg sshclient.Config, cmd string) (string, error)` where `Config{ Host string; Port int; User string; Password string; KnownHostsPath string }`.
  - `linux.Plugin` registered as `protocol.TypeLinux` in `init()`.
  - `linux.commandFor(metric string) (string, bool)` and `linux.parseMetric(metric, raw string) (map[string]any, error)`.

- [ ] **Step 1: Write the failing test (parser — pure, no device)**

`internal/plugins/linux/parse_test.go`:
```go
package linux

import "testing"

func TestParseMetricJSON(t *testing.T) {
	raw := `{"system_cpu_percent": 12.5, "system_cpu_cores": 4}`
	got, err := parseMetric("CPU", raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["system_cpu_cores"].(float64) != 4 {
		t.Fatalf("bad cores: %v", got["system_cpu_cores"])
	}
}

func TestParseMetricRejectsGarbage(t *testing.T) {
	if _, err := parseMetric("CPU", "not json"); err == nil {
		t.Fatal("expected error on non-JSON output")
	}
}

func TestCommandForKnownMetric(t *testing.T) {
	if _, ok := commandFor("CPU"); !ok {
		t.Fatal("CPU command should exist")
	}
	if _, ok := commandFor("BOGUS"); ok {
		t.Fatal("unknown metric must not have a command")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/plugins/linux/`
Expected: FAIL — package/functions undefined.

- [ ] **Step 3: Implement sshclient**

`internal/sshclient/sshclient.go`:
```go
package sshclient

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Config describes one SSH connection.
type Config struct {
	Host           string
	Port           int
	User           string
	Password       string
	KnownHostsPath string // e.g. $HOME/.ssh/known_hosts
	DialTimeout    time.Duration
}

// Run dials the host (verifying the host key against known_hosts), runs cmd, and
// returns trimmed stdout. The context deadline bounds the whole operation.
func Run(ctx context.Context, cfg Config, cmd string) (string, error) {
	hostKeyCallback, err := knownhosts.New(cfg.KnownHostsPath)
	if err != nil {
		return "", fmt.Errorf("known_hosts: %w", err)
	}
	timeout := cfg.DialTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	clientCfg := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: hostKeyCallback,
		Timeout:         timeout,
	}
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))

	type dialResult struct {
		client *ssh.Client
		err    error
	}
	ch := make(chan dialResult, 1)
	go func() {
		c, err := ssh.Dial("tcp", addr, clientCfg)
		ch <- dialResult{c, err}
	}()

	var client *ssh.Client
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case dr := <-ch:
		if dr.err != nil {
			return "", dr.err
		}
		client = dr.client
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	// Enforce the context deadline on command execution.
	done := make(chan struct{})
	var out []byte
	var runErr error
	go func() {
		out, runErr = session.Output(cmd)
		close(done)
	}()
	select {
	case <-ctx.Done():
		_ = session.Close()
		return "", ctx.Err()
	case <-done:
		if runErr != nil {
			return "", runErr
		}
		return trim(string(out)), nil
	}
}

func trim(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}

// DefaultKnownHosts returns $HOME/.ssh/known_hosts.
func DefaultKnownHosts() string {
	return os.Getenv("HOME") + "/.ssh/known_hosts"
}
```

- [ ] **Step 4: Implement the Linux commands, parser, and plugin**

`internal/plugins/linux/commands.go`:
```go
package linux

// commands maps a metric name to a shell command that prints a JSON object.
var commands = map[string]string{
	"CPU":     `printf '{"system_cpu_cores": %s, "system_cpu_percent": %s}\n' "$(nproc)" "$(top -bn1 | awk '/%Cpu/ {printf \"%.2f\", 100 - $8}')"`,
	"MEMORY":  `printf '{"system_memory_used_bytes": %s, "system_memory_total_bytes": %s}\n' "$(free -b | awk '/Mem:/ {print $3}')" "$(free -b | awk '/Mem:/ {print $2}')"`,
	"DISK":    `printf '{"system_disk_used_bytes": %s, "system_disk_total_bytes": %s}\n' "$(df -B1 / | awk 'NR==2 {print $3}')" "$(df -B1 / | awk 'NR==2 {print $2}')"`,
	"NETWORK": `printf '{"system_tcp_connections": %s}\n' "$(ss -t | wc -l)"`,
	"UPTIME":  `printf '{"system_uptime_seconds": %s}\n' "$(awk '{print $1}' /proc/uptime)"`,
}

func commandFor(metric string) (string, bool) {
	c, ok := commands[metric]
	return c, ok
}
```

`internal/plugins/linux/parse.go`:
```go
package linux

import (
	"encoding/json"
	"fmt"
)

// parseMetric parses a command's JSON output into a metric map.
func parseMetric(metric, raw string) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("metric %s: invalid JSON output: %w", metric, err)
	}
	return m, nil
}
```

`internal/plugins/linux/linux.go`:
```go
package linux

import (
	"context"
	"encoding/json"

	"ssh_plugin/internal/protocol"
	"ssh_plugin/internal/registry"
	"ssh_plugin/internal/sshclient"
)

func init() { registry.Register(protocol.TypeLinux, Plugin{}) }

// Plugin collects Linux metrics over SSH.
type Plugin struct{}

type cred struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (Plugin) config(t protocol.Target) (sshclient.Config, error) {
	var c cred
	if err := json.Unmarshal(t.Credential, &c); err != nil {
		return sshclient.Config{}, err
	}
	port := t.Port
	if port == 0 {
		port = 22
	}
	return sshclient.Config{
		Host: t.IP, Port: port, User: c.Username, Password: c.Password,
		KnownHostsPath: sshclient.DefaultKnownHosts(),
	}, nil
}

// Discover verifies reachability + credentials by running `uname -a`.
func (p Plugin) Discover(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventDiscovery)
	cfg, err := p.config(t)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	out, err := sshclient.Run(ctx, cfg, "uname -a")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Status = protocol.StatusSuccess
	res.Result = map[string]any{"system": out}
	return res
}

// Collect runs the requested metric commands over one connection.
func (p Plugin) Collect(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventPoll)
	cfg, err := p.config(t)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	data := map[string]any{}
	for _, metric := range t.Metrics {
		cmd, ok := commandFor(metric)
		if !ok {
			res.Error = "unsupported metric: " + metric
			return res
		}
		out, err := sshclient.Run(ctx, cfg, cmd)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		parsed, err := parseMetric(metric, out)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		data[metric] = parsed
	}
	res.Status = protocol.StatusSuccess
	res.Result = data
	return res
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/plugins/linux/ ./internal/sshclient/`
Expected: PASS (parser tests; sshclient has no unit test yet — build must succeed).

- [ ] **Step 6: Commit**

```bash
git add internal/sshclient/ internal/plugins/linux/
git commit -m "feat(linux): known_hosts SSH client + Linux collector with JSON parser

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: SNMP collector

**Files:**
- Create: `internal/plugins/snmp/oids.go`, `internal/plugins/snmp/snmp.go`
- Test: `internal/plugins/snmp/snmp_test.go`

**Interfaces:**
- Consumes: `protocol`, `registry`, `github.com/gosnmp/gosnmp`.
- Produces:
  - `snmp.Plugin` registered as `protocol.TypeSNMP`.
  - `snmp.metricFromPDUs(metric string, pdus map[string]gosnmp.SnmpPDU) (map[string]any, bool)` — pure mapping from OID→value, unit-testable without a device.

- [ ] **Step 1: Write the failing test (pure OID→metric mapping)**

`internal/plugins/snmp/snmp_test.go`:
```go
package snmp

import (
	"testing"

	"github.com/gosnmp/gosnmp"
)

func TestMetricFromPDUsUptime(t *testing.T) {
	pdus := map[string]gosnmp.SnmpPDU{
		oidSysUpTime: {Name: oidSysUpTime, Type: gosnmp.TimeTicks, Value: uint32(12345)},
	}
	got, ok := metricFromPDUs("UPTIME", pdus)
	if !ok {
		t.Fatal("expected UPTIME mapping")
	}
	if got["system_uptime_ticks"].(uint32) != 12345 {
		t.Fatalf("bad uptime: %v", got["system_uptime_ticks"])
	}
}

func TestMetricFromPDUsUnknownMetric(t *testing.T) {
	if _, ok := metricFromPDUs("BOGUS", nil); ok {
		t.Fatal("unknown metric must not map")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/plugins/snmp/`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement OIDs and mapping**

`internal/plugins/snmp/oids.go`:
```go
package snmp

import "github.com/gosnmp/gosnmp"

const (
	oidSysUpTime = "1.3.6.1.2.1.1.3.0"
	oidSysDescr  = "1.3.6.1.2.1.1.1.0"
)

// metricOIDs lists the OIDs to GET for each supported metric.
var metricOIDs = map[string][]string{
	"UPTIME": {oidSysUpTime},
	"SYSTEM": {oidSysDescr},
}

// metricFromPDUs maps fetched PDUs to a metric map. Returns false for unknown metrics.
func metricFromPDUs(metric string, pdus map[string]gosnmp.SnmpPDU) (map[string]any, bool) {
	switch metric {
	case "UPTIME":
		return map[string]any{"system_uptime_ticks": pdus[oidSysUpTime].Value}, true
	case "SYSTEM":
		return map[string]any{"system_descr": pdus[oidSysDescr].Value}, true
	default:
		return nil, false
	}
}

func oidsFor(metrics []string) []string {
	var out []string
	for _, m := range metrics {
		out = append(out, metricOIDs[m]...)
	}
	return out
}
```

`internal/plugins/snmp/snmp.go`:
```go
package snmp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gosnmp/gosnmp"

	"ssh_plugin/internal/protocol"
	"ssh_plugin/internal/registry"
)

func init() { registry.Register(protocol.TypeSNMP, Plugin{}) }

// Plugin collects metrics over SNMP v2c/v3.
type Plugin struct{}

type cred struct {
	Version   string `json:"version"` // "2c" or "3"
	Community string `json:"community"`
}

func (Plugin) newClient(t protocol.Target) (*gosnmp.GoSNMP, error) {
	var c cred
	if err := json.Unmarshal(t.Credential, &c); err != nil {
		return nil, err
	}
	port := t.Port
	if port == 0 {
		port = 161
	}
	g := &gosnmp.GoSNMP{
		Target:    t.IP,
		Port:      uint16(port),
		Community: c.Community,
		Version:   gosnmp.Version2c,
		Timeout:   3 * time.Second,
		Retries:   1,
	}
	return g, nil
}

func (p Plugin) get(ctx context.Context, t protocol.Target, oids []string) (map[string]gosnmp.SnmpPDU, error) {
	g, err := p.newClient(t)
	if err != nil {
		return nil, err
	}
	if err := g.Connect(); err != nil {
		return nil, err
	}
	defer g.Conn.Close()

	res, err := g.Get(oids)
	if err != nil {
		return nil, err
	}
	out := map[string]gosnmp.SnmpPDU{}
	for _, v := range res.Variables {
		out[v.Name[1:]] = v // gosnmp prefixes OIDs with '.'
	}
	return out, nil
}

func (p Plugin) Discover(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventDiscovery)
	pdus, err := p.get(ctx, t, []string{oidSysDescr})
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Status = protocol.StatusSuccess
	res.Result = map[string]any{"system_descr": pdus[oidSysDescr].Value}
	return res
}

func (p Plugin) Collect(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventPoll)
	pdus, err := p.get(ctx, t, oidsFor(t.Metrics))
	if err != nil {
		res.Error = err.Error()
		return res
	}
	data := map[string]any{}
	for _, m := range t.Metrics {
		if mapped, ok := metricFromPDUs(m, pdus); ok {
			data[m] = mapped
		}
	}
	res.Status = protocol.StatusSuccess
	res.Result = data
	return res
}
```

Note: `newClient` reads `Version` but this initial cut hardcodes v2c; v3 fields extend `cred` in a follow-up. Keep the field to document intent.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/plugins/snmp/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/plugins/snmp/
git commit -m "feat(snmp): gosnmp collector with pure OID->metric mapping

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 8: WinRM collector

**Files:**
- Create: `internal/plugins/winrm/winrm.go`
- Test: `internal/plugins/winrm/winrm_test.go`

**Interfaces:**
- Consumes: `protocol`, `registry`, `github.com/masterzen/winrm`.
- Produces:
  - `winrm.Plugin` registered as `protocol.TypeWinRM`.
  - `winrm.psFor(metric string) (string, bool)` — pure metric→PowerShell mapping, unit-testable.
  - `winrm.parseMetric(metric, raw string) (map[string]any, error)` — parse JSON PS output.

- [ ] **Step 1: Write the failing test (pure command + parse)**

`internal/plugins/winrm/winrm_test.go`:
```go
package winrm

import "testing"

func TestPSForKnownMetric(t *testing.T) {
	if _, ok := psFor("CPU"); !ok {
		t.Fatal("CPU PowerShell should exist")
	}
	if _, ok := psFor("BOGUS"); ok {
		t.Fatal("unknown metric must not map")
	}
}

func TestParseMetricJSON(t *testing.T) {
	got, err := parseMetric("MEMORY", `{"system_memory_used_bytes": 100}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["system_memory_used_bytes"].(float64) != 100 {
		t.Fatalf("bad: %v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/plugins/winrm/`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement the WinRM collector**

`internal/plugins/winrm/winrm.go`:
```go
package winrm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/masterzen/winrm"

	"ssh_plugin/internal/protocol"
	"ssh_plugin/internal/registry"
)

func init() { registry.Register(protocol.TypeWinRM, Plugin{}) }

// Plugin collects Windows metrics over WinRM.
type Plugin struct{}

type cred struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// ps maps a metric to a PowerShell command that emits a JSON object.
var ps = map[string]string{
	"CPU":    `$c=(Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average; @{system_cpu_percent=$c} | ConvertTo-Json -Compress`,
	"MEMORY": `$o=Get-CimInstance Win32_OperatingSystem; @{system_memory_used_bytes=(($o.TotalVisibleMemorySize-$o.FreePhysicalMemory)*1024); system_memory_total_bytes=($o.TotalVisibleMemorySize*1024)} | ConvertTo-Json -Compress`,
	"DISK":   `$d=Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='C:'"; @{system_disk_used_bytes=($d.Size-$d.FreeSpace); system_disk_total_bytes=$d.Size} | ConvertTo-Json -Compress`,
}

func psFor(metric string) (string, bool) {
	c, ok := ps[metric]
	return c, ok
}

func parseMetric(metric, raw string) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("metric %s: invalid JSON output: %w", metric, err)
	}
	return m, nil
}

func (Plugin) run(ctx context.Context, t protocol.Target, psCmd string) (string, error) {
	var c cred
	if err := json.Unmarshal(t.Credential, &c); err != nil {
		return "", err
	}
	port := t.Port
	if port == 0 {
		port = 5985
	}
	endpoint := winrm.NewEndpoint(t.IP, port, false, false, nil, nil, nil, 0)
	client, err := winrm.NewClient(endpoint, c.Username, c.Password)
	if err != nil {
		return "", err
	}
	stdout, _, _, err := client.RunPSWithContext(ctx, psCmd)
	if err != nil {
		return "", err
	}
	return stdout, nil
}

func (p Plugin) Discover(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventDiscovery)
	out, err := p.run(ctx, t, `@{system=$env:COMPUTERNAME} | ConvertTo-Json -Compress`)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	parsed, err := parseMetric("SYSTEM", out)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Status = protocol.StatusSuccess
	res.Result = parsed
	return res
}

func (p Plugin) Collect(ctx context.Context, t protocol.Target) protocol.Result {
	res := protocol.NewResult(t, protocol.EventPoll)
	data := map[string]any{}
	for _, metric := range t.Metrics {
		psCmd, ok := psFor(metric)
		if !ok {
			res.Error = "unsupported metric: " + metric
			return res
		}
		out, err := p.run(ctx, t, psCmd)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		parsed, err := parseMetric(metric, out)
		if err != nil {
			res.Error = err.Error()
			return res
		}
		data[metric] = parsed
	}
	res.Status = protocol.StatusSuccess
	res.Result = data
	return res
}
```

Note: confirm the installed `masterzen/winrm` exposes `RunPSWithContext`; if the pinned version differs, use `RunWithContextWithString` with an encoded PowerShell payload. Adjust the single call site accordingly.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/plugins/winrm/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/plugins/winrm/
git commit -m "feat(winrm): masterzen/winrm collector with PowerShell metric mapping

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 9: Wire plugin registration, full build, end-to-end test

**Files:**
- Modify: `main.go` (uncomment the three plugin imports from Task 5)
- Create: `internal/engine/e2e_test.go`
- Create: `scripts/build.sh`

**Interfaces:**
- Consumes: everything above.
- Produces: a compiled binary at `../Lite-NMS/plugin/Lite_NMS_Plugin`; a passing end-to-end test feeding an envelope through `engine.Run` with a registered fake type.

- [ ] **Step 1: Uncomment plugin imports in main.go**

In `main.go`, ensure these lines are active:
```go
	_ "ssh_plugin/internal/plugins/linux"
	_ "ssh_plugin/internal/plugins/snmp"
	_ "ssh_plugin/internal/plugins/winrm"
```

- [ ] **Step 2: Write the end-to-end test**

`internal/engine/e2e_test.go`:
```go
package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"ssh_plugin/internal/protocol"
)

func TestEndToEndEnvelopeToResultLine(t *testing.T) {
	raw := `{"version":1,"event_type":"poll","targets":[{"request_id":"e1","job_id":9,"plugin_type":"OKP","metrics":["CPU"]}]}`
	env, err := protocol.DecodeEnvelope(base64.StdEncoding.EncodeToString([]byte(raw)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// OKP is registered by engine_test.go's TestRunPollSuccess via registry.Register.
	var sb strings.Builder
	Run(context.Background(), env, &sb, time.Second)

	line := strings.TrimSpace(sb.String())
	b, _ := base64.StdEncoding.DecodeString(line)
	var r protocol.Result
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("json: %v", err)
	}
	if r.RequestID != "e1" || r.EventType != protocol.EventPoll {
		t.Fatalf("discriminators missing: %+v", r)
	}
}
```

- [ ] **Step 3: Run the full test suite**

Run: `go test ./...`
Expected: all packages PASS.

- [ ] **Step 4: Create the build script**

`scripts/build.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
OUT="${1:-../Lite-NMS/plugin/Lite_NMS_Plugin}"
mkdir -p "$(dirname "$OUT")"
go build -o "$OUT" .
echo "built $OUT"
```

Then:
```bash
chmod +x scripts/build.sh
./scripts/build.sh
```
Expected: `built ../Lite-NMS/plugin/Lite_NMS_Plugin`.

- [ ] **Step 5: Smoke-test the binary against a real Linux device**

Create `/tmp/env.json` (base64 of a one-target LINUX poll envelope with your device IP + credential), then:
```bash
base64 /tmp/env.json > /tmp/env.b64
./Lite_NMS_Plugin /tmp/env.b64 | while read l; do echo "$l" | base64 -d; echo; done
```
Expected: one JSON result line with `"status":"success"` and a `CPU` object. (Ensure the device's host key is in `~/.ssh/known_hosts` first: `ssh-keyscan -H <ip> >> ~/.ssh/known_hosts`.)

- [ ] **Step 6: Commit**

```bash
git add main.go internal/engine/e2e_test.go scripts/build.sh
git commit -m "feat(engine): register collectors, build script, end-to-end test

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 10: Remove legacy code and write the README

**Files:**
- Delete: `ssh/utils.go`, `ssh/polling.go`, `ssh/discovery.go`, and any old top-level JSON samples that referenced the old contract
- Create/Replace: `README.md`
- Create: `.gitignore`

**Interfaces:**
- Produces: a clean repo with only the v2 code paths.

- [ ] **Step 1: Delete the legacy package**

```bash
git rm -r ssh/
git rm -f disc.json discovery.json input.json 2>/dev/null || true
```

- [ ] **Step 2: Verify nothing references the removed package**

Run: `go build ./... && go test ./...`
Expected: PASS (no import of `ssh_plugin/ssh` remains).

- [ ] **Step 3: Write .gitignore**

`.gitignore`:
```
/Lite_NMS_Plugin
/ssh_plugin
*.b64
.idea/
```

- [ ] **Step 4: Write README.md**

`README.md` — cover: purpose, the envelope contract (§4 of the spec, with one example in/out), the three collectors table, how to build (`./scripts/build.sh`), how to run a smoke test, and the plugin-registry extension point ("add a protocol = implement `registry.Plugin` and `registry.Register` in `init()`").

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: remove legacy ssh package, add README and gitignore

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** envelope contract (Tasks 2,5); registry over if/else (Task 3); bounded worker pool + panic/timeout (Task 4); stdout-only results / stderr logs (Tasks 4,5); Linux/SNMP/WinRM real collectors (Tasks 6,7,8); known_hosts verification (Task 6); no secrets in logs (Global Constraints; logs are stderr and never print credentials); subprocess file-in/stdout-out kept (Task 5); build artifact path matches backend (`plugin/Lite_NMS_Plugin`, Task 9).
- **Placeholder scan:** none — every code step is complete. Two explicit "confirm library API" notes (SNMP v3 extension, WinRM method name) are flagged as verification steps, not missing code.
- **Type consistency:** `protocol.Target/Result`, `registry.Plugin{Discover,Collect}`, `engine.Run`, `sshclient.Run/Config`, `commandFor/parseMetric/psFor/metricFromPDUs` names are used identically across tasks.
