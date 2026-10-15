# CLAUDE.md — DIRP Project Context

Complete reference for the **AI-Powered Distributed Incident Response Platform (DIRP)**. This file is the source of truth for how the system is built, why it's built that way, and how to work on it.

---

## 1. What this project is

DIRP is a developer-focused blend of **observability + incident management + AI root-cause analysis**, written in Go. It runs a small microservices system, and when something breaks it automatically determines **what** broke, **why**, and **what to do about it** — the job of an on-call SRE, automated end to end.

- **Module:** `github.com/ajay-1134/dirp`
- **Language / toolchain:** Go 1.25
- **Primary goal:** portfolio + interview piece demonstrating distributed-systems and Go concurrency depth.
- **Design north star:** works on a laptop with zero external dependencies (in-memory storage, no API key), and scales up to the real tooling stack (OpenTelemetry, Prometheus, Grafana, TimescaleDB, Claude) with `docker compose up`.

### The scenario it diagnoses

```
User → Order → Payment → Ledger → Notification      (5 traced microservices)
                  │
                  └── DB connection pool exhausted   ← injected fault
                          │
                          ▼
          Payment stalls → Order times out → error rate 1% → 35%
```

The engineering challenge is **not** noticing Order is failing — it's *not blaming Order* (the symptom) and instead walking the dependency graph back to **Payment's exhausted connection pool** (the true cause).

### What the platform does automatically

```
 Metrics (Prometheus)  +  Traces (OpenTelemetry/OTLP)
              │
              ▼
   Anomaly Detection Engine        online EWMA + z-score per service
              │
              ▼
   Incident created                severity + lifecycle state machine
              │
              ▼
   Dependency Graph Analysis       rebuilt from spans; walk to deepest cause
              │
              ▼
   AI Root-Cause Analysis          Claude reasons over the assembled evidence
              │
              ▼
   Ranked remediation → Live dashboard (Server-Sent Events)
```

---

## 2. Directory structure (every file)

```
AI-Powered-DIRP/
├── go.mod, go.sum                      # module github.com/ajay-1134/dirp (Go 1.25)
├── Dockerfile                          # multi-stage; builds all 6 binaries into one distroless image
├── docker-compose.yml                  # 5 services + platform + Prometheus + Grafana + TimescaleDB
├── README.md                           # user-facing intro + run instructions
├── CLAUDE.md                           # this file
├── .gitignore, .dockerignore
│
├── cmd/
│   ├── platform/main.go                # THE CONTROL PLANE: wires ingest→detect→incident→graph→AI→dashboard
│   └── services/                       # the 5 sample microservices (thin mains over internal/service)
│       ├── user/main.go                # entry point; /checkout; built-in load generator (LOAD_RPS)
│       ├── order/main.go               # /orders → calls Payment; its 5xx is the visible symptom
│       ├── payment/main.go             # /charges → uses bounded pool; hosts the chaos endpoints
│       ├── ledger/main.go              # /entries → calls Notification
│       └── notification/main.go        # /notify → leaf; stays healthy (control service)
│
├── internal/                           # shared code used by the sample services
│   ├── service/
│   │   ├── service.go                  # Server: Gin engine + telemetry + /healthz + /metrics + graceful shutdown
│   │   ├── client.go                   # Client: traced downstream HTTP client (otelhttp transport, 3s timeout)
│   │   └── env.go                      # Env(key, def) helper
│   ├── telemetry/
│   │   ├── telemetry.go                # OTel tracer provider + OTLP exporter + W3C propagation
│   │   └── metrics.go                  # Prometheus RED metrics + Gin middleware + DB-pool gauges
│   └── chaos/
│       ├── pool.go                     # Pool: channel-based bounded connection pool (mirrors database/sql)
│       └── controller.go               # Controller: exhausts/resets the pool; auto-releases after duration
│
└── platform/                           # the control-plane packages
    ├── model/model.go                  # ALL shared domain types (dependency-free, no import cycles)
    ├── store/
    │   ├── store.go                    # Store interface + Memory (in-memory ring-buffer) implementation
    │   └── timescale.go                # Timescale implementation (same interface; used when STORE_DSN set)
    ├── ingest/
    │   ├── scraper.go                  # metrics ingestion: scrape Prometheus, derive rate/error/p99/pool
    │   └── otlp.go                      # trace ingestion: embedded OTLP/gRPC receiver → model.Span
    ├── anomaly/detector.go             # online EWMA/z-score detection engine
    ├── incident/manager.go             # incident correlation, severity, lifecycle state machine
    ├── depgraph/
    │   ├── graph.go                    # rebuild dependency graph from spans
    │   └── analyze.go                  # walk graph to find root cause + heuristic remediation
    ├── ai/analyzer.go                  # Claude evidence-driven root-cause (falls back to heuristic)
    └── dashboard/
        ├── server.go                   # HTTP API + SSE hub
        └── dashboard.html              # single-file live UI (embedded via go:embed)

deploy/
├── prometheus.yml                      # Prometheus scrape config (the "real Prometheus" in the stack)
└── grafana-datasource.yml             # auto-provisions the Prometheus datasource in Grafana

scripts/
├── run-local.sh                        # build + run everything locally, no Docker
└── demo.sh                             # inject the fault and print the diagnosed incident
```

---

## 3. The sample microservices (`cmd/services`, `internal/`)

All five are built on `internal/service.Server`, so each `main.go` only declares its business routes. Uniformity is deliberate: the platform can only correlate services that emit signals identically.

### Request chain and ports

| Service | Port | Route | Calls | Notes |
|---|---|---|---|---|
| user | 8081 | `POST /checkout` | order | entry point; runs load generator when `LOAD_RPS>0` |
| order | 8082 | `POST /orders` | payment | returns 502 when Payment fails — the visible symptom |
| payment | 8083 | `POST /charges` | ledger | leases a pool connection per charge; hosts chaos endpoints |
| ledger | 8084 | `POST /entries` | notification | records the entry |
| notification | 8085 | `POST /notify` | — | leaf; healthy throughout (root-cause control) |

Every service also exposes `GET /healthz` and `GET /metrics`.

### `internal/service`
- **`service.go` — `Server`:** builds a `gin.Engine` with middleware order Recovery → otelgin (opens server span, extracts incoming trace context) → metrics (records after handler). Mounts `/healthz` + `/metrics`. `Run()` blocks on SIGINT/SIGTERM, then does graceful HTTP + telemetry shutdown. Accessors: `Router()`, `Metrics()`, `Logger()`.
- **`client.go` — `Client`:** `NewClient(name, base)` returns an HTTP client whose transport is `otelhttp.NewTransport(...)` so outbound calls inject the W3C `traceparent` header — this is what stitches all five hops into one trace. **3-second timeout is intentional**: when Payment stalls, callers time out fast, which is what makes the error-rate spike observable. Methods: `GetJSON`, `PostJSON`.
- **`env.go`:** `Env(key, def)` — lets the same binary run under compose (hostnames = service names) and locally (localhost).

### `internal/telemetry`
- **`telemetry.go` — `Setup(ctx, Config)`:** installs a composite W3C TraceContext + Baggage propagator, builds an OTel resource with `service.name`, and a `TracerProvider`. If `OTLPEndpoint` is empty it uses a no-op provider (service still runs standalone); otherwise it exports over OTLP/gRPC with `AlwaysSample`. `Provider.Shutdown` flushes on exit.
- **`metrics.go` — `Metrics`:** one Prometheus `Registry` per service. Instruments (RED method + DB pool), all const-labeled with `service`:
  - `http_request_duration_seconds` (histogram, buckets 1ms–16s)
  - `http_requests_total` (counter, labels method/route/status)
  - `http_requests_in_flight` (gauge)
  - `db_connections_in_use`, `db_connections_max` (gauges)
  - `db_connection_wait_total` (counter)
  - plus Go runtime + process collectors.
  - `Middleware()` records duration/count using `c.FullPath()` (matched route, e.g. `/orders/:id`) to keep label cardinality bounded. `SetDBPoolStats`/`ObserveDBWait` let Payment publish pool state.

### `internal/chaos` — the fault mechanism
- **`pool.go` — `Pool`:** a bounded connection pool backed by a **buffered channel of tokens** (`tokens chan struct{}`). `Acquire(ctx)` receives a token (fast path) or blocks until one frees / ctx cancels (slow path, incrementing `waitCount`). `Conn.Release()` returns the token. `Stats()` returns `{InUse, Max, WaitCount, WaitDuration}`. This mirrors `database/sql` pool semantics exactly but is self-contained (no DB required) so exhaustion is deterministic and race-free.
- **`controller.go` — `Controller`:** `Exhaust(holders, duration)` grabs up to `holders` connections **without blocking** (each acquire uses a 50ms context so injection never deadlocks against queued traffic) and holds them; a goroutine auto-releases after `duration`. `Reset()` releases immediately. `Status()` returns `{Active, HeldConns, Until}`.

### Payment's chaos control plane (`cmd/services/payment`)
- `POST /chaos/db-pool-exhaust?holders=N&duration=45s` — inject
- `POST /chaos/reset` — clear
- `GET /chaos/status` — current state

Each `/charges` request: `pool.Acquire(ctx)` → 12ms simulated query → call Ledger. Under exhaustion, Acquire blocks until the propagated request context times out, returning 503; `publishPoolStats` copies `pool.Stats()` into the Prometheus gauges every second.

---

## 4. The platform control plane (`cmd/platform` + `platform/`)

`cmd/platform/main.go` is the orchestrator. Boot sequence:

1. **Storage** — `store.NewMemory` by default; `store.NewTimescale(STORE_DSN)` if the env var is set (falls back to memory on connect error).
2. **Metrics scraper** — `ingest.NewScraper(targets, 2s, store)`, runs in a goroutine.
3. **OTLP receiver** — `ingest.NewOTLPReceiver(:4317, store)`, runs in a goroutine.
4. **Detector + incident manager + AI analyzer** — constructed.
5. **Wiring:** `manager.OnChange(dash.Broadcast)` streams every incident mutation to the dashboard; `manager.OnOpen(...)` launches the analysis pipeline asynchronously (so detection is never blocked on graph work or a Claude round-trip).
6. **Detection loop** — every 2s: read the latest snapshot per service, feed each to `detector.Observe`, hand the batch to `manager.Process`.
7. **HTTP server** — Gin engine with the dashboard routes on `PLATFORM_ADDR` (default `:8080`).

### `platform/model` — shared vocabulary
Dependency-free types used everywhere: `ServiceSnapshot` (per-service health: RequestRate, ErrorRate, P99Latency, DB pool), `Span`, `Edge`, `Severity` (P1–P4), `IncidentStatus` (detected → investigating → root_cause_identified → resolved), `Evidence`, `RootCause` (Service, Summary, Confidence, Remediation, Source), `Incident`.

### `platform/store` — pluggable persistence
- **`Store` interface:** `AddSnapshot`, `SnapshotsSince`, `LatestSnapshot`, `Services`, `AddSpan`, `SpansSince`, `SaveIncident`, `Incident`, `Incidents`.
- **`Memory`:** in-memory maps + slices, retention-windowed (default 30 min), mutex-guarded. **Default and fully runtime-verified.**
- **`Timescale`:** pgxpool-backed. Schema: `snapshots` + `spans` hypertables (best-effort `create_hypertable`; degrades to plain Postgres), `incidents` as JSONB. Selected by `STORE_DSN`. Compiles and is wired into compose; exercised via the compose stack (not in the local sandbox — no DB there).

### `platform/ingest` — the two ingestion paths
- **`scraper.go` — `Scraper`:** every 2s, GETs each target's `/metrics`, parses with **`expfmt.NewTextParser(prommodel.LegacyValidation)`** (see Gotchas), and reduces raw families into a `ServiceSnapshot`:
  - `RequestRate` / `ErrorRate` from counter deltas between scrapes.
  - `P99Latency` from **per-scrape histogram bucket deltas** (`p99FromDelta`) — like `histogram_quantile(0.99, rate(..._bucket[interval]))`. Per-service previous state (`counterState`, including previous bucket counts) is retained to compute deltas.
  - DB pool gauges + wait counter read directly.
- **`otlp.go` — `OTLPReceiver`:** an embedded gRPC server implementing the OTLP `TraceService`. Flattens `ResourceSpans → ScopeSpans → Span` into `model.Span` (hex-encoded trace/span/parent ids, `service.name` from resource attributes, error = `STATUS_CODE_ERROR`). Embedding it (vs. a standalone collector) keeps the demo to a single binary while staying genuine OTLP on the wire.

### `platform/anomaly` — detection engine
- **`Detector`** keeps a per-service `serviceBaseline` with online EWMA mean+variance (`ewma` type, West's incremental variance) for error rate and latency.
- **Rules** (`Observe(snapshot) → []Anomaly`):
  - *Error rate:* `ErrorRate ≥ ErrorRateFloor (0.05)` **and** z ≥ `ZThreshold (3.0)`.
  - *Latency:* `p99 ≥ LatencyFloor (0.25s)` **and** z ≥ 3 **and** `p99 ≥ 2× baseline` — a triple gate. The absolute floor is load-bearing, not belt-and-braces (see Gotchas).
  - *DB pool:* utilization ≥ `DBUtilThreshold (0.9)` — a threshold, not z-score, because a full pool is definitionally bad and the strongest root-cause marker. Evaluated **before** the no-traffic guard, since the pool is an instantaneous gauge (see Gotchas).
- **Warmup:** first `Warmup (5)` samples only train, never alarm. Only intervals that actually served traffic count toward the warmup budget.
- **No-traffic guard:** an interval with `RequestRate == 0` is skipped for the interval-derived signals — `p99=0`/`errorRate=0` there mean "not measured", not "healthy" (see Gotchas).
- **Baseline freezing:** baselines update only when the service looks healthy, so a sustained incident can't "learn" the bad state as normal.

### `platform/incident` — lifecycle management
- **`Manager`** tracks a single active incident (matches the single-fault demo; extends to subgraph clustering). Incident IDs start at **INC-1042** (`seq` seeded at 1041).
- **`Process(anoms, now)`:**
  - Zero anomalies → increment `healthyStreak`; after `resolveAfter (3)` clean cycles, mark **resolved** (sets `ResolvedAt`).
  - Anomalies present → open an incident if none active (`OnOpen` fires), then upsert evidence (deduped by `service|kind`, newest wins).
- **Severity** is computed from **accumulated evidence** (not one cycle): `worstErr ≥ 0.25` or `≥3 affected services` → P1; `≥0.10` → P2; `≥0.05` → P3; else P4.
- **`SetRootCause(id, rc)`** attaches the analysis result and advances status to `root_cause_identified`.
- Hooks: `OnOpen(fn)` (analysis trigger), `OnChange(fn)` (dashboard broadcast).

### `platform/depgraph` — symptom → cause
- **`graph.go` — `Build(spans)`:** indexes spans by id; for each span whose parent lives in a **different** service, records a `caller → callee` edge (call/error counts, per-edge p99). `Downstream(service)` returns callees.
- **`analyze.go` — `FindRootCause(symptom, Signals)`:** the core rule — the true cause is the **deepest anomalous service whose own dependencies are healthy** (an anomalous service with an anomalous callee is just relaying failure from below). DB-pool saturation is a decisive origin marker. Returns `Analysis{RootService, SymptomPath, Reason, Confidence, Remediation}`. `remediationFor` gives DB-pool-specific advice (increase pool / inspect queries / circuit breaker) or generic downstream guidance. `ToRootCause()` produces a `model.RootCause` with `Source: "heuristic"`.

### `platform/ai` — Claude root cause
- **`Analyzer`:** `NewAnalyzer()` checks `ANTHROPIC_API_KEY`; if absent, `Enabled()` is false and callers use the heuristic.
- **`Analyze(ctx, incident, graphAnalysis, edges)`:** builds a prompt inlining the dependency graph, the anomaly evidence, and the heuristic's first guess, then calls `client.Messages.New` with **`anthropic.ModelClaudeOpus4_8`** and **adaptive thinking** (`ThinkingConfigParamUnion{OfAdaptive: ...}`). Parses a JSON object `{root_service, summary, confidence, remediation[]}` from the response (tolerates prose/fences by scanning outer braces). **The model reasons only over evidence we assemble — it does not fetch data** (grounded, fast, cheap). Any failure → returns the heuristic `RootCause` so an answer always exists. Result tagged `Source: "ai"`.

### `platform/dashboard` — live UI + API
- **`server.go` — `Server`:** routes + an SSE `hub`. `Broadcast(incident)` marshals and fans out to all connected clients (drops for slow clients rather than blocking the pipeline).
- **`dashboard.html`:** single self-contained file (embedded via `//go:embed`). Polls `/api/services`, `/api/graph`, `/api/incidents` every 2s and subscribes to `/api/stream` (SSE) for live incident updates. Renders service-health tiles, the dependency graph, and the incident feed with severity badge, evidence, AI/heuristic root cause, and ranked remediation.

---

## 5. Configuration (environment variables)

### Sample services
| Var | Default | Purpose |
|---|---|---|
| `<SVC>_ADDR` | `:808x` | listen address (`USER_ADDR`, `ORDER_ADDR`, …) |
| `OTLP_ENDPOINT` | `""` (no-op tracing) | platform's OTLP receiver, e.g. `localhost:4317` |
| `ORDER_URL` / `PAYMENT_URL` / `LEDGER_URL` / `NOTIFICATION_URL` | `http://localhost:808x` | downstream base URLs |
| `LOAD_RPS` | `0` (off) | user service's built-in load generator rate |
| `PAYMENT_DB_MAX_CONNS` | `10` | Payment's pool size (smaller = easier to exhaust) |

### Platform
| Var | Default | Purpose |
|---|---|---|
| `PLATFORM_ADDR` | `:8080` | dashboard + API |
| `PLATFORM_OTLP_ADDR` | `:4317` | OTLP trace receiver |
| `SCRAPE_TARGETS` | 5 localhost services | `name=url,name=url,…`; service name must match the `service` metric label / OTel `service.name` |
| `STORE_DSN` | `""` (in-memory) | Postgres/TimescaleDB DSN to enable persistence |
| `ANTHROPIC_API_KEY` | `""` (heuristic) | enables Claude root-cause analysis |

---

## 6. HTTP endpoints

**Platform (`:8080`)**
- `GET /` — live dashboard
- `GET /api/services` — latest `ServiceSnapshot` per service
- `GET /api/graph` — `{edges: [...]}` reconstructed from recent spans
- `GET /api/incidents` — incident feed (newest first)
- `GET /api/incidents/:id` — one incident
- `GET /api/stream` — SSE stream of incident updates
- OTLP/gRPC trace receiver on `:4317`

**Payment (`:8083`)** — `POST /chaos/db-pool-exhaust?holders=N&duration=45s`, `POST /chaos/reset`, `GET /chaos/status`

**All services** — `GET /healthz`, `GET /metrics`

---

## 7. How to run

### Local, no Docker (in-memory storage, verified path)
```bash
./scripts/run-local.sh      # builds 6 binaries, starts platform + 5 services with tracing + LOAD_RPS
# in another terminal:
./scripts/demo.sh           # injects the fault, prints the diagnosed incident
# dashboard: http://localhost:8080
```

### Full stack with Docker (real tooling)
```bash
docker compose up --build
# dashboard :8080 · prometheus :9090 · grafana :3000 (anon admin) · timescaledb :5432
./scripts/demo.sh
```
> Requires the Docker Compose plugin (not installed on the original dev machine — install Docker Desktop or `docker-compose-plugin`).

### Enabling Claude
Set `ANTHROPIC_API_KEY` before starting → root cause authored by Claude Opus 4.8 (`source: ai`); otherwise the deterministic heuristic (`source: heuristic`). Everything else is identical.

### Common dev commands
```bash
go build ./...                      # build everything
go vet ./...                        # vet
gofmt -l .                          # list unformatted files (should be empty)
go build -o /tmp/bin/<name> ./cmd/... # build a single binary
```

---

## 8. Key design decisions

- **Symptom vs. cause.** Root-cause analysis walks the trace-derived graph to the *deepest* anomalous service whose dependencies are healthy — a cascade is attributed to its origin, not where it first surfaced.
- **Windowed p99.** Percentiles are computed over per-scrape histogram **deltas**, not cumulative totals — otherwise a past latency spike pegs p99 high forever and incidents never resolve.
- **Evidence-driven AI.** The LLM reasons over evidence the platform already assembled; it never goes hunting. Grounded, fast, cheap — and the heuristic fallback guarantees an answer with no key.
- **Runs anywhere.** Storage and the AI layer are pluggable/optional, so the full demo works with no database and no API key, and scales to real tooling in compose.
- **Gin everywhere.** All HTTP servers use Gin (chosen deliberately over chi). Keep it that way.
- **Single binaries.** No standalone collector; the platform embeds its own OTLP receiver and Prometheus scraper. `docker compose up` is the whole system.
- **Why Go.** The ingestion pipeline is a textbook concurrent producer/consumer problem — goroutines, channels, `context` cancellation, per-service worker state — exactly Go's strength.

---

## 9. Non-obvious gotchas (already solved — don't reintroduce)

1. **Prometheus parser scheme.** A zero-value `expfmt.TextParser` panics `"Invalid name validation scheme requested: unset"` in `prometheus/common` v0.66+. Must use `expfmt.NewTextParser(prommodel.LegacyValidation)`. (The global `model.NameValidationScheme` is unreliable due to init ordering.)
2. **Cumulative vs. windowed p99.** Computing p99 over the cumulative histogram lets a past spike dominate forever → incidents never auto-resolve. Always diff bucket counts between scrapes (`p99FromDelta`).
3. **Severity from accumulated evidence.** Compute severity from the incident's full evidence set, not a single detection cycle — a cycle that re-trips only latency/pool (not error rate) would otherwise understate severity.
4. **Client timeout is load-bearing.** The downstream client's 3s timeout is what converts a Payment stall into fast upstream 5xx; don't raise it casually.
5. **Analysis runs off the detection loop.** `OnOpen` launches analysis in a goroutine; never call Claude synchronously inside `Process`.
6. **Anthropic SDK specifics.** Model `anthropic.ModelClaudeOpus4_8`; adaptive thinking via `ThinkingConfigParamUnion{OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{}}` (never `budget_tokens` — it 400s on Opus 4.8). Parse the text block as JSON.
7. **The latency rule needs an absolute floor.** p99 comes from histogram buckets, so it is *quantized*: a healthy service sitting on the 10ms bound that drifts to the next bound (25ms) is a 2.5× jump, and a steady signal has ~0 variance so its z-score is `+Inf`. Both statistical gates clear on a completely healthy system — this opened a spurious P1 on `notification-service` about 60s after startup, with no fault injected, and burned the demo's `INC-1042` id. `LatencyFloor (0.25s)` is what prevents it; never drop it back to a z-score-plus-multiple rule. Covered by `TestBucketJitterDoesNotAlarm`.
8. **Never train baselines on a zero-traffic interval.** When no requests complete in an interval the scraper reports `p99=0` and `errorRate=0` because nothing was *measured*. Training on those zeros pins the baseline near zero; once real traffic arrives every sample looks like a huge multiple of it, and since anomalous samples are never learned from, **the baseline can never recover and the service stays anomalous forever** — incidents then never auto-resolve (observed stuck >11 min after the fault cleared). Covered by `TestEmptyIntervalsDoNotCorruptBaseline`.
9. **The DB-pool rule must bypass the no-traffic guard.** The pool is an instantaneous gauge, and full exhaustion is exactly the case where every request blocks long enough that none complete within a scrape interval — so `RequestRate` is 0 precisely when pool saturation is the decisive signal. Check the pool *before* returning on an empty interval, or the platform goes blind to the root cause it exists to find (it degrades to generic 0.7-confidence "deepest unhealthy service" advice instead of the 0.9 pool-specific diagnosis). Covered by `TestPoolSaturationDetectedWhenNoRequestsComplete`.

---

## 10. Extension points

- **Persistence:** the `Timescale` store is wired but only exercised via compose — verify it end-to-end, add migrations/indexes, or add continuous aggregates.
- **Concurrent incidents:** the incident manager tracks one active incident; cluster anomalies by dependency subgraph for multiple simultaneous incidents.
- **Grafana dashboards:** only the datasource is provisioned; add dashboard JSON under `deploy/` for a pre-built metrics view.
- **More fault types:** add chaos scenarios beyond pool exhaustion (latency injection, error injection, dependency outage) and corresponding detection/remediation.
- **Alerting/notifications:** the incident `OnChange` hook is the natural place to fan out to Slack/PagerDuty/webhooks.

---

## 11. Tech stack

Go 1.25 · Gin · OpenTelemetry (SDK + otelgin + otelhttp + OTLP/gRPC exporter & receiver) · Prometheus (`client_golang` + `common/expfmt`) · pgx/pgxpool (TimescaleDB) · `anthropic-sdk-go` (Claude Opus 4.8) · Docker Compose (services + Prometheus + Grafana + TimescaleDB).

---

## 12. Verification status

- **Fully runtime-verified end-to-end** (local, in-memory): baseline traffic → inject fault → error-rate cascade → anomaly detection → INC-1042 opened (P1) → dependency graph rebuilt from real OTLP traces → root cause correctly attributed to `payment-service` (not Order) at 0.9 confidence with pool-specific remediation → **auto-resolution** ~10s after the fault clears. `demo.sh` exits 0 with the full incident report.
- **False-positive soak:** 180s of fault-free traffic produces **zero** incidents (this previously produced a spurious P1 at ~60s; see Gotchas 7–9).
- **Unit tests:** 67 tests across `anomaly`, `ingest`, `depgraph`, `incident`, `store`; pass under `-race`. The three gotchas above have named regression tests that fail if the fix is reverted.
- **Compiles + vets clean; gofmt clean.**
- **TimescaleDB path:** compiles and is wired via `STORE_DSN`/compose; not yet exercised against a live database in the dev sandbox (no Docker there).
- **AI path:** not yet exercised — every verified run was `source: heuristic` (no `ANTHROPIC_API_KEY` in the sandbox).

> **Local build note:** on a machine whose Xcode CLT SDK is newer than its Go toolchain, the default cgo link fails (`tapi error: malformed file`). Build with `CGO_ENABLED=0` — nothing here needs cgo.
