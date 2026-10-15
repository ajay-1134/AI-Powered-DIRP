package incident

import (
	"testing"
	"time"

	"github.com/ajay-1134/dirp/platform/anomaly"
	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

func newTestManager(t *testing.T, resolveAfter int) (*Manager, store.Store) {
	t.Helper()
	st := store.NewMemory(30 * time.Minute)
	return NewManager(st, resolveAfter), st
}

// current returns the newest incident the manager has persisted. The store is
// the source of truth the dashboard and analysis pipeline both read from, so
// asserting against it also checks that state is actually being saved.
func current(t *testing.T, st store.Store) model.Incident {
	t.Helper()
	incs := st.Incidents()
	if len(incs) == 0 {
		t.Fatalf("no incident was recorded")
	}
	return incs[0]
}

func anom(service string, kind anomaly.Kind, value float64) anomaly.Anomaly {
	return anomaly.Anomaly{
		Service: service,
		Kind:    kind,
		Value:   value,
		Detail:  string(kind) + " on " + service,
	}
}

func TestOpensIncidentWithDemoID(t *testing.T) {
	m, _ := newTestManager(t, 3)

	var opened []model.Incident
	m.OnOpen(func(inc model.Incident) { opened = append(opened, inc) })

	now := time.Now()
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)

	if len(opened) != 1 {
		t.Fatalf("OnOpen fired %d times, want 1", len(opened))
	}
	if opened[0].ID != "INC-1042" {
		t.Fatalf("first incident id = %q, want INC-1042", opened[0].ID)
	}

	// A second cycle of anomalies must not open a second incident.
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now.Add(2*time.Second))
	if len(opened) != 1 {
		t.Fatalf("OnOpen fired %d times across two anomalous cycles, want 1", len(opened))
	}
}

func TestAffectedServiceIsTheErrorRateService(t *testing.T) {
	m, st := newTestManager(t, 3)

	// Pool saturation is listed first, but the user-visible symptom is the
	// service throwing errors.
	m.Process([]anomaly.Anomaly{
		anom("payment-service", anomaly.KindDBPool, 1.0),
		anom("order-service", anomaly.KindErrorRate, 0.35),
	}, time.Now())

	if got := current(t, st).AffectedService; got != "order-service" {
		t.Fatalf("affected service = %q, want order-service", got)
	}
}

// TestSeverityUsesAccumulatedEvidence guards a subtle trap: severity must be
// computed from everything the incident has seen, not from the current
// detection cycle. A later cycle that only re-trips the pool signal would
// otherwise downgrade a P1 to a P4.
func TestSeverityUsesAccumulatedEvidence(t *testing.T) {
	m, st := newTestManager(t, 3)
	now := time.Now()

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)
	if got := current(t, st).Severity; got != model.SevP1 {
		t.Fatalf("severity after a 35%% error cycle = %v, want P1", got)
	}

	// Next cycle: only the pool signal re-trips.
	m.Process([]anomaly.Anomaly{anom("payment-service", anomaly.KindDBPool, 1.0)}, now.Add(2*time.Second))
	if got := current(t, st).Severity; got != model.SevP1 {
		t.Fatalf("severity dropped to %v after a pool-only cycle, want it held at P1", got)
	}
}

func TestSeverityScalesWithBlastRadius(t *testing.T) {
	for _, tc := range []struct {
		name  string
		anoms []anomaly.Anomaly
		want  model.Severity
	}{
		{
			name:  "single service, modest errors",
			anoms: []anomaly.Anomaly{anom("a", anomaly.KindErrorRate, 0.06)},
			want:  model.SevP3,
		},
		{
			name:  "single service, elevated errors",
			anoms: []anomaly.Anomaly{anom("a", anomaly.KindErrorRate, 0.12)},
			want:  model.SevP2,
		},
		{
			name:  "single service, severe errors",
			anoms: []anomaly.Anomaly{anom("a", anomaly.KindErrorRate, 0.30)},
			want:  model.SevP1,
		},
		{
			name: "three services affected",
			anoms: []anomaly.Anomaly{
				anom("a", anomaly.KindLatency, 1),
				anom("b", anomaly.KindLatency, 1),
				anom("c", anomaly.KindLatency, 1),
			},
			want: model.SevP1,
		},
		{
			name:  "latency only on one service",
			anoms: []anomaly.Anomaly{anom("a", anomaly.KindLatency, 1)},
			want:  model.SevP4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, st := newTestManager(t, 3)
			m.Process(tc.anoms, time.Now())
			if got := current(t, st).Severity; got != tc.want {
				t.Fatalf("severity = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvidenceIsDedupedNewestWins(t *testing.T) {
	m, st := newTestManager(t, 3)
	now := time.Now()

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.10)}, now)
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.40)}, now.Add(2*time.Second))

	ev := current(t, st).Evidence
	if len(ev) != 1 {
		t.Fatalf("got %d evidence entries, want 1 deduped entry: %+v", len(ev), ev)
	}
	if ev[0].Value != 0.40 {
		t.Fatalf("evidence value = %v, want the newest (0.40)", ev[0].Value)
	}
}

func TestEvidenceOrderIsStable(t *testing.T) {
	m, st := newTestManager(t, 3)

	m.Process([]anomaly.Anomaly{
		anom("payment-service", anomaly.KindErrorRate, 0.3),
		anom("order-service", anomaly.KindLatency, 5),
		anom("payment-service", anomaly.KindDBPool, 1),
		anom("order-service", anomaly.KindErrorRate, 0.3),
	}, time.Now())

	ev := current(t, st).Evidence
	want := []struct{ service, kind string }{
		{"order-service", "error_rate"},
		{"order-service", "p99_latency"},
		{"payment-service", "db_pool"},
		{"payment-service", "error_rate"},
	}
	if len(ev) != len(want) {
		t.Fatalf("got %d evidence entries, want %d", len(ev), len(want))
	}
	for i, w := range want {
		if ev[i].Service != w.service || ev[i].Kind != w.kind {
			t.Fatalf("evidence[%d] = %s/%s, want %s/%s", i, ev[i].Service, ev[i].Kind, w.service, w.kind)
		}
	}
}

func TestAutoResolvesAfterCleanCycles(t *testing.T) {
	const resolveAfter = 3
	m, st := newTestManager(t, resolveAfter)

	var changes []model.Incident
	m.OnChange(func(inc model.Incident) { changes = append(changes, inc) })

	now := time.Now()
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)

	// Not enough clean cycles yet.
	for i := 1; i < resolveAfter; i++ {
		m.Process(nil, now.Add(time.Duration(i)*2*time.Second))
		if got := current(t, st).Status; got == model.StatusResolved {
			t.Fatalf("resolved after only %d clean cycles, want %d", i, resolveAfter)
		}
	}

	m.Process(nil, now.Add(resolveAfter*2*time.Second))

	inc := current(t, st)
	if inc.Status != model.StatusResolved {
		t.Fatalf("status = %v, want resolved", inc.Status)
	}
	if inc.ResolvedAt == nil {
		t.Fatalf("ResolvedAt was not stamped")
	}
	if len(changes) == 0 || changes[len(changes)-1].Status != model.StatusResolved {
		t.Fatalf("resolution was not broadcast via OnChange")
	}
}

func TestHealthyStreakResetsOnNewAnomaly(t *testing.T) {
	const resolveAfter = 3
	m, st := newTestManager(t, resolveAfter)
	now := time.Now()

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)
	m.Process(nil, now.Add(2*time.Second))
	m.Process(nil, now.Add(4*time.Second))
	// A flap: the incident is still live, so the streak restarts.
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now.Add(6*time.Second))
	m.Process(nil, now.Add(8*time.Second))
	m.Process(nil, now.Add(10*time.Second))

	if got := current(t, st).Status; got == model.StatusResolved {
		t.Fatalf("incident resolved despite a flap resetting the healthy streak")
	}
}

func TestAfterResolutionANewIncidentGetsANewID(t *testing.T) {
	m, st := newTestManager(t, 1)
	now := time.Now()

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)
	m.Process(nil, now.Add(2*time.Second)) // resolves immediately with resolveAfter=1

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now.Add(4*time.Second))
	if got := current(t, st).ID; got != "INC-1043" {
		t.Fatalf("second incident id = %q, want INC-1043", got)
	}
}

func TestProcessWithNoActiveIncidentIsANoop(t *testing.T) {
	m, st := newTestManager(t, 3)

	var changes int
	m.OnChange(func(model.Incident) { changes++ })

	m.Process(nil, time.Now())

	if changes != 0 {
		t.Fatalf("OnChange fired %d times with no active incident, want 0", changes)
	}
	if len(st.Incidents()) != 0 {
		t.Fatalf("an incident was persisted with no anomalies")
	}
}

func TestSetRootCauseAdvancesStatus(t *testing.T) {
	m, st := newTestManager(t, 3)
	now := time.Now()
	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)

	id := current(t, st).ID
	m.SetRootCause(id, model.RootCause{
		Service:    "payment-service",
		Summary:    "pool exhaustion",
		Confidence: 0.9,
		Source:     "heuristic",
	})

	inc, ok := st.Incident(id)
	if !ok {
		t.Fatalf("incident %s missing from the store", id)
	}
	if inc.Status != model.StatusRootCause {
		t.Fatalf("status = %v, want root_cause_identified", inc.Status)
	}
	if inc.RootCause == nil || inc.RootCause.Service != "payment-service" {
		t.Fatalf("root cause not attached: %+v", inc.RootCause)
	}
}

// TestSetRootCauseDoesNotReopenResolved covers the race where analysis finishes
// after the fault has already cleared.
func TestSetRootCauseDoesNotReopenResolved(t *testing.T) {
	m, st := newTestManager(t, 1)
	now := time.Now()

	m.Process([]anomaly.Anomaly{anom("order-service", anomaly.KindErrorRate, 0.35)}, now)
	id := current(t, st).ID
	m.Process(nil, now.Add(2*time.Second)) // resolves

	m.SetRootCause(id, model.RootCause{Service: "payment-service", Source: "ai"})

	inc, _ := st.Incident(id)
	if inc.Status != model.StatusResolved {
		t.Fatalf("status = %v, want it to stay resolved", inc.Status)
	}
	if inc.RootCause == nil {
		t.Fatalf("root cause should still be attached to a resolved incident")
	}
}

func TestSetRootCauseOnUnknownIncidentIsSafe(t *testing.T) {
	m, _ := newTestManager(t, 3)
	// Must not panic or invent an incident.
	m.SetRootCause("INC-9999", model.RootCause{Service: "payment-service"})
}
