package depgraph

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ajay-1134/dirp/platform/model"
)

// chain builds the demo's span topology for one trace:
//
//	user -> order -> payment -> ledger -> notification
//
// Each hop is a span whose parent lives in the calling service, which is exactly
// what Build keys edges off.
func chain(traceID string, dur time.Duration, errored map[string]bool) []model.Span {
	services := []string{
		"user-service", "order-service", "payment-service",
		"ledger-service", "notification-service",
	}
	var spans []model.Span
	for i, svc := range services {
		sp := model.Span{
			TraceID:  traceID,
			SpanID:   traceID + "-" + string(rune('a'+i)),
			Service:  svc,
			Name:     "handle",
			Start:    time.Now(),
			Duration: dur,
			Error:    errored[svc],
		}
		if i == 0 {
			sp.ParentSpanID = "0000000000000000" // OTLP root: all-zero parent
		} else {
			sp.ParentSpanID = traceID + "-" + string(rune('a'+i-1))
		}
		spans = append(spans, sp)
	}
	return spans
}

func demoGraph(t *testing.T) *Graph {
	t.Helper()
	var spans []model.Span
	for i := 0; i < 5; i++ {
		spans = append(spans, chain(string(rune('A'+i)), 20*time.Millisecond, nil)...)
	}
	return Build(spans)
}

func TestBuildDerivesEdgesFromSpanParents(t *testing.T) {
	g := demoGraph(t)

	want := []model.Edge{
		{From: "ledger-service", To: "notification-service", CallCount: 5},
		{From: "order-service", To: "payment-service", CallCount: 5},
		{From: "payment-service", To: "ledger-service", CallCount: 5},
		{From: "user-service", To: "order-service", CallCount: 5},
	}
	got := g.EdgesList()
	if len(got) != len(want) {
		t.Fatalf("got %d edges, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].From != want[i].From || got[i].To != want[i].To {
			t.Fatalf("edge %d = %s->%s, want %s->%s", i, got[i].From, got[i].To, want[i].From, want[i].To)
		}
		if got[i].CallCount != want[i].CallCount {
			t.Fatalf("edge %s->%s call count = %d, want %d",
				got[i].From, got[i].To, got[i].CallCount, want[i].CallCount)
		}
	}
}

func TestBuildIgnoresRootAndSameServiceParents(t *testing.T) {
	spans := []model.Span{
		// Root span with an all-zero parent: no edge.
		{SpanID: "1", ParentSpanID: "0000000000000000", Service: "user-service"},
		// Internal span within the same service: no self-edge.
		{SpanID: "2", ParentSpanID: "1", Service: "user-service"},
		// Genuine cross-service call.
		{SpanID: "3", ParentSpanID: "2", Service: "order-service"},
		// Parent never captured in this window: no edge.
		{SpanID: "4", ParentSpanID: "missing", Service: "ledger-service"},
	}

	g := Build(spans)
	edges := g.EdgesList()
	if len(edges) != 1 {
		t.Fatalf("got %d edges, want 1: %+v", len(edges), edges)
	}
	if edges[0].From != "user-service" || edges[0].To != "order-service" {
		t.Fatalf("edge = %s->%s, want user-service->order-service", edges[0].From, edges[0].To)
	}
}

func TestBuildCountsErrorsPerEdge(t *testing.T) {
	var spans []model.Span
	// Three traces where payment failed.
	for i := 0; i < 3; i++ {
		spans = append(spans, chain(string(rune('A'+i)), time.Second,
			map[string]bool{"payment-service": true})...)
	}
	// Two clean traces.
	for i := 3; i < 5; i++ {
		spans = append(spans, chain(string(rune('A'+i)), 20*time.Millisecond, nil)...)
	}

	g := Build(spans)
	e, ok := g.Edges["order-service|payment-service"]
	if !ok {
		t.Fatalf("missing order->payment edge")
	}
	if e.CallCount != 5 || e.ErrorCount != 3 {
		t.Fatalf("edge calls=%d errors=%d, want calls=5 errors=3", e.CallCount, e.ErrorCount)
	}
}

func TestDownstreamIsACopy(t *testing.T) {
	g := demoGraph(t)
	ds := g.Downstream("order-service")
	if !reflect.DeepEqual(ds, []string{"payment-service"}) {
		t.Fatalf("downstream(order) = %v, want [payment-service]", ds)
	}
	// Mutating the returned slice must not corrupt the graph's own adjacency.
	ds[0] = "clobbered"
	if again := g.Downstream("order-service"); again[0] != "payment-service" {
		t.Fatalf("Downstream returned an aliased slice; graph was mutated to %v", again)
	}
}

// TestBlamesPaymentNotOrder is the whole point of the project: Order is where
// the failure is visible, but Payment is where it originates. Getting this
// backwards would make the platform an ordinary alerting tool.
func TestBlamesPaymentNotOrder(t *testing.T) {
	g := demoGraph(t)

	sig := Signals{
		Anomalous:   map[string]bool{"order-service": true, "payment-service": true},
		DBSaturated: map[string]bool{"payment-service": true},
		ErrorRate:   map[string]float64{"order-service": 0.35, "payment-service": 0.35},
	}

	got := g.FindRootCause("order-service", sig)

	if got.RootService != "payment-service" {
		t.Fatalf("root cause = %q, want payment-service (order is only the symptom)", got.RootService)
	}
	if want := []string{"order-service", "payment-service"}; !reflect.DeepEqual(got.SymptomPath, want) {
		t.Fatalf("symptom path = %v, want %v", got.SymptomPath, want)
	}
	if got.Confidence != 0.9 {
		t.Fatalf("confidence = %v, want 0.9 for a decisive pool signal", got.Confidence)
	}
	if !strings.Contains(got.Reason, "pool exhaustion") {
		t.Fatalf("reason %q does not mention pool exhaustion", got.Reason)
	}
	if len(got.Remediation) == 0 {
		t.Fatalf("no remediation produced")
	}
}

// TestDeepestAnomalousWins covers the general rule without the decisive DB
// signal: an anomalous service whose callee is also anomalous is relaying a
// failure, not causing it.
func TestDeepestAnomalousWins(t *testing.T) {
	g := demoGraph(t)

	sig := Signals{
		Anomalous: map[string]bool{
			"order-service":   true,
			"payment-service": true,
			"ledger-service":  true,
		},
		DBSaturated: map[string]bool{},
		ErrorRate:   map[string]float64{"order-service": 0.2},
	}

	got := g.FindRootCause("order-service", sig)

	// notification-service is healthy, so ledger is the deepest unhealthy
	// service with healthy dependencies.
	if got.RootService != "ledger-service" {
		t.Fatalf("root cause = %q, want ledger-service", got.RootService)
	}
	want := []string{"order-service", "payment-service", "ledger-service"}
	if !reflect.DeepEqual(got.SymptomPath, want) {
		t.Fatalf("symptom path = %v, want %v", got.SymptomPath, want)
	}
	if got.Confidence != 0.7 {
		t.Fatalf("confidence = %v, want 0.7", got.Confidence)
	}
}

func TestSymptomIsItsOwnCause(t *testing.T) {
	g := demoGraph(t)

	// Only order is unhealthy; everything it depends on is fine.
	sig := Signals{
		Anomalous:   map[string]bool{"order-service": true},
		DBSaturated: map[string]bool{},
		ErrorRate:   map[string]float64{"order-service": 0.4},
	}

	got := g.FindRootCause("order-service", sig)

	if got.RootService != "order-service" {
		t.Fatalf("root cause = %q, want order-service", got.RootService)
	}
	if !reflect.DeepEqual(got.SymptomPath, []string{"order-service"}) {
		t.Fatalf("symptom path = %v, want [order-service]", got.SymptomPath)
	}
	if got.Confidence != 0.5 {
		t.Fatalf("confidence = %v, want 0.5", got.Confidence)
	}
}

// TestEmptyGraphFallsBackToSymptom covers the window where an incident opens
// before any spans have been ingested.
func TestEmptyGraphFallsBackToSymptom(t *testing.T) {
	g := Build(nil)

	sig := Signals{
		Anomalous:   map[string]bool{"order-service": true},
		DBSaturated: map[string]bool{},
		ErrorRate:   map[string]float64{"order-service": 0.4},
	}

	got := g.FindRootCause("order-service", sig)
	if got.RootService != "order-service" {
		t.Fatalf("root cause = %q, want the symptom service as fallback", got.RootService)
	}
	if len(got.Remediation) == 0 {
		t.Fatalf("remediation must still be produced with no graph")
	}
}

func TestRemediationIsPoolSpecificWhenSaturated(t *testing.T) {
	pool := remediationFor("payment-service", Signals{
		DBSaturated: map[string]bool{"payment-service": true},
	})
	if len(pool) == 0 || !strings.Contains(pool[0], "pool capacity") {
		t.Fatalf("pool remediation did not lead with capacity advice: %v", pool)
	}

	generic := remediationFor("payment-service", Signals{DBSaturated: map[string]bool{}})
	for _, step := range generic {
		if strings.Contains(step, "SetMaxOpenConns") {
			t.Fatalf("generic remediation leaked pool-specific advice: %v", generic)
		}
	}
}

func TestToRootCauseTagsHeuristicSource(t *testing.T) {
	a := Analysis{
		RootService: "payment-service",
		Reason:      "because",
		Confidence:  0.9,
		Remediation: []string{"do a thing"},
	}
	rc := a.ToRootCause()

	if rc.Source != "heuristic" {
		t.Fatalf("source = %q, want heuristic", rc.Source)
	}
	if rc.Service != "payment-service" || rc.Summary != "because" || rc.Confidence != 0.9 {
		t.Fatalf("conversion lost data: %+v", rc)
	}
}
