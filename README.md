# DIRP — AI-Powered Distributed Incident Response Platform

A developer-focused blend of **observability + incident management + AI root-cause analysis**, written in Go. It runs a small microservices system, and when something breaks it automatically figures out **what** broke, **why**, and **what to do about it** — the way a senior SRE would, but in seconds and without a human staring at dashboards.

> Inject a failure into the Payment service and watch the platform open an incident, reconstruct the service dependency graph from live traces, trace the failure back to its true origin (not just the symptom), and produce a ranked remediation — live on a dashboard.

---

## The scenario it diagnoses

```
User → Order → Payment → Ledger → Notification         (5 traced microservices)
                  │
                  └── DB connection pool exhausted  ← injected fault
                          │
                          ▼
          Payment stalls → Order times out → error rate 1% → 35%
```

The hard part is not noticing that Order is failing. It's *not blaming Order* — the symptom — and instead walking the dependency graph back to **Payment's exhausted connection pool**, the actual cause.

## What the platform does, automatically

```
 Metrics (Prometheus)  +  Traces (OpenTelemetry/OTLP)
              │
              ▼
   Anomaly Detection Engine        (online EWMA + z-score per service)
              │
              ▼
   Incident created                (severity + lifecycle state machine)
              │
              ▼
   Dependency Graph Analysis       (rebuilt from spans; walk to deepest cause)
              │
              ▼
   AI Root-Cause Analysis          (Claude reasons over the assembled evidence)
              │
              ▼
   Ranked remediation → Live dashboard (SSE)
```

### Example output

```
🚨 INCIDENT #INC-1042        severity: P1        status: root_cause_identified
   affected: payment-service
   evidence:
     - [order-service]   error rate 96.0% (baseline 0.0%)
     - [order-service]   p99 latency 5000ms, up 9900% from baseline 50ms
     - [payment-service] DB connection pool 5/5 in use (100%)
   root cause: payment-service  (confidence 0.9)
     DB connection pool exhaustion at payment-service is saturating downstream
     capacity, causing upstream timeouts.
   remediation:
     1. Increase the DB connection pool capacity on payment-service.
     2. Inspect for long-running or stuck queries holding connections.
     3. Enable a circuit breaker on callers of payment-service.
```

---

## Run it

### Option A — full stack with Docker (real tooling)

Brings up the 5 services, the platform, **Prometheus**, **Grafana**, and **TimescaleDB**:

```bash
docker compose up --build
# dashboard:   http://localhost:8080
# prometheus:  http://localhost:9090
# grafana:     http://localhost:3000   (anonymous admin)

./scripts/demo.sh        # inject the fault; the incident appears on the dashboard
```

> Requires the Docker Compose plugin. If `docker compose` isn't found, install Docker Desktop or the `docker-compose-plugin` package.

### Option B — locally, no Docker

```bash
./scripts/run-local.sh   # builds + starts everything (in-memory storage)
# in another terminal:
./scripts/demo.sh
```

### Enabling Claude root-cause analysis

Set `ANTHROPIC_API_KEY` before starting. With a key, the root cause is authored by **Claude (Opus 4.8)** reasoning over the assembled evidence (`source: ai`); without one, the platform falls back to a deterministic graph heuristic (`source: heuristic`). Everything else is identical.

---

## Architecture

| Component | Package | What it does |
|---|---|---|
| Sample services | `cmd/services/*` | 5 Gin services chained with W3C trace-context propagation; each emits OTel traces + Prometheus metrics |
| Chaos injection | `internal/chaos` | Bounded connection pool + a controller that exhausts it on demand (`POST /chaos/db-pool-exhaust`) |
| Shared framework | `internal/service`, `internal/telemetry` | Uniform telemetry wiring so every service emits correlatable signals |
| Metrics ingestion | `platform/ingest/scraper.go` | Scrapes each service's Prometheus endpoint; derives rate, error-rate, and **windowed p99** (histogram deltas) |
| Trace ingestion | `platform/ingest/otlp.go` | Embedded OTLP/gRPC receiver → spans |
| Storage | `platform/store` | Pluggable `Store`: in-memory (default) or **TimescaleDB** (`STORE_DSN`) |
| Anomaly detection | `platform/anomaly` | Online EWMA mean/variance → z-score per signal, with warmup and baseline freezing during incidents |
| Incident manager | `platform/incident` | Correlates anomalies into one incident; severity from blast radius; lifecycle state machine with auto-resolution |
| Graph analysis | `platform/depgraph` | Rebuilds the dependency graph from parent/child spans; walks it to the deepest unhealthy service (the cause, not the symptom) |
| AI analysis | `platform/ai` | Sends the structured evidence to Claude; parses a ranked root-cause + remediation |
| Dashboard | `platform/dashboard` | HTTP API + a single-file live UI streamed over Server-Sent Events |

### Why Go
The ingestion pipeline is a textbook concurrent producer/consumer problem — goroutines, channels, `context` cancellation, per-service worker state — which is exactly where Go is strongest. The whole system compiles to static binaries and comes up with `docker compose up`.

### Design notes worth calling out
- **Symptom vs. cause.** Root-cause analysis walks the trace-derived graph to the *deepest* anomalous service whose own dependencies are healthy — so a cascade is attributed to its origin, not the first place it surfaced.
- **Windowed p99.** Percentiles are computed over per-scrape histogram *deltas* (like `histogram_quantile(rate(...))`), not cumulative totals — otherwise a past latency spike would peg p99 forever and incidents would never resolve.
- **Evidence-driven AI.** The LLM reasons over evidence the platform has already assembled; it doesn't go hunting for data. That keeps the analysis grounded, fast, and cheap.
- **Runs anywhere.** Storage and the AI layer are both optional/pluggable, so the full demo works on a laptop with no database and no API key, and scales up to the real tooling in compose.

---

## Endpoints

Platform (`:8080`)
- `GET /` — live dashboard
- `GET /api/services` — latest health snapshot per service
- `GET /api/graph` — current dependency edges
- `GET /api/incidents` — incident feed
- `GET /api/stream` — SSE stream of incident updates
- OTLP trace receiver on `:4317`

Payment chaos control (`:8083`)
- `POST /chaos/db-pool-exhaust?holders=N&duration=45s`
- `POST /chaos/reset`
- `GET /chaos/status`
