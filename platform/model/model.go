// Package model holds the core domain types shared across the platform control
// plane: the per-service telemetry snapshots the scraper produces, the trace
// spans the OTLP receiver ingests, and the incident/evidence types the anomaly
// detector and AI layer produce. Keeping these in one dependency-free package
// lets ingestion, detection, graph analysis, and the API all speak the same
// vocabulary without import cycles.
package model

import "time"

// ServiceSnapshot is a point-in-time summary of one service's health, computed
// by the scraper from that service's Prometheus metrics each interval. It is the
// atomic unit the anomaly detector reasons over.
type ServiceSnapshot struct {
	Service   string    `json:"service"`
	Timestamp time.Time `json:"timestamp"`

	// RequestRate is requests per second since the previous scrape.
	RequestRate float64 `json:"request_rate"`
	// ErrorRate is the fraction (0..1) of requests returning 5xx since the
	// previous scrape.
	ErrorRate float64 `json:"error_rate"`
	// P99Latency is the 99th-percentile request latency in seconds, derived
	// from the service's latency histogram.
	P99Latency float64 `json:"p99_latency"`

	// DB pool telemetry (zero for services without a pool). Utilization is the
	// key exhaustion signal: DBInUse / DBMax approaching 1.0.
	DBInUse     int   `json:"db_in_use"`
	DBMax       int   `json:"db_max"`
	DBWaitTotal int64 `json:"db_wait_total"`
}

// DBUtilization returns pool utilization in [0,1], or 0 when the service has no
// pool.
func (s ServiceSnapshot) DBUtilization() float64 {
	if s.DBMax == 0 {
		return 0
	}
	return float64(s.DBInUse) / float64(s.DBMax)
}

// Span is a simplified OpenTelemetry span: enough to reconstruct the service
// dependency graph and per-hop latency/errors, without carrying the full OTLP
// structure around.
type Span struct {
	TraceID      string        `json:"trace_id"`
	SpanID       string        `json:"span_id"`
	ParentSpanID string        `json:"parent_span_id"`
	Service      string        `json:"service"`
	Name         string        `json:"name"`
	Start        time.Time     `json:"start"`
	Duration     time.Duration `json:"duration"`
	Error        bool          `json:"error"`
}

// Edge is a directed dependency between two services (caller -> callee),
// aggregated from spans over a time window.
type Edge struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	CallCount  int     `json:"call_count"`
	ErrorCount int     `json:"error_count"`
	P99Latency float64 `json:"p99_latency"` // seconds
}

// Severity is the incident priority, P1 (highest) through P4.
type Severity string

const (
	SevP1 Severity = "P1"
	SevP2 Severity = "P2"
	SevP3 Severity = "P3"
	SevP4 Severity = "P4"
)

// IncidentStatus tracks an incident through its lifecycle.
type IncidentStatus string

const (
	StatusDetected      IncidentStatus = "detected"
	StatusInvestigating IncidentStatus = "investigating"
	StatusRootCause     IncidentStatus = "root_cause_identified"
	StatusResolved      IncidentStatus = "resolved"
)

// Evidence is a single supporting datum for an incident, phrased so it can go
// straight into the dashboard and the AI prompt.
type Evidence struct {
	Service string    `json:"service"`
	Kind    string    `json:"kind"`   // e.g. "error_rate", "p99_latency", "db_pool"
	Detail  string    `json:"detail"` // human-readable, e.g. "p99 latency 420% above baseline"
	Value   float64   `json:"value"`
	At      time.Time `json:"at"`
}

// RootCause is the platform's conclusion about why an incident happened,
// populated by graph analysis and refined by the AI layer.
type RootCause struct {
	Service     string   `json:"service"`     // the service identified as the origin
	Summary     string   `json:"summary"`     // one-line explanation
	Confidence  float64  `json:"confidence"`  // 0..1
	Remediation []string `json:"remediation"` // ranked suggested actions
	Source      string   `json:"source"`      // "heuristic" or "ai"
}

// Incident is the central record: what broke, the evidence, and the conclusion.
type Incident struct {
	ID              string         `json:"id"`
	Status          IncidentStatus `json:"status"`
	Severity        Severity       `json:"severity"`
	AffectedService string         `json:"affected_service"` // where the symptom appeared
	Title           string         `json:"title"`
	OpenedAt        time.Time      `json:"opened_at"`
	ResolvedAt      *time.Time     `json:"resolved_at,omitempty"`
	Evidence        []Evidence     `json:"evidence"`
	RootCause       *RootCause     `json:"root_cause,omitempty"`
}
