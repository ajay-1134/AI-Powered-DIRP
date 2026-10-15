package store

import (
	"reflect"
	"testing"
	"time"

	"github.com/ajay-1134/dirp/platform/model"
)

func snapAt(service string, at time.Time, errRate float64) model.ServiceSnapshot {
	return model.ServiceSnapshot{Service: service, Timestamp: at, ErrorRate: errRate}
}

func TestLatestSnapshotReturnsMostRecent(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	base := time.Now()

	m.AddSnapshot(snapAt("payment-service", base, 0.01))
	m.AddSnapshot(snapAt("payment-service", base.Add(2*time.Second), 0.35))

	got, ok := m.LatestSnapshot("payment-service")
	if !ok {
		t.Fatalf("no snapshot found")
	}
	if got.ErrorRate != 0.35 {
		t.Fatalf("error rate = %v, want the newest (0.35)", got.ErrorRate)
	}
}

func TestLatestSnapshotMissingService(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	if _, ok := m.LatestSnapshot("nope"); ok {
		t.Fatalf("reported a snapshot for a service that was never seen")
	}
}

func TestServicesAreSortedAndUnique(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	now := time.Now()
	for _, svc := range []string{"user-service", "order-service", "payment-service", "order-service"} {
		m.AddSnapshot(snapAt(svc, now, 0))
	}

	want := []string{"order-service", "payment-service", "user-service"}
	if got := m.Services(); !reflect.DeepEqual(got, want) {
		t.Fatalf("services = %v, want %v", got, want)
	}
}

// TestRetentionTrimsOldSnapshots keeps a long-running demo from growing without
// bound.
func TestRetentionTrimsOldSnapshots(t *testing.T) {
	const retention = time.Minute
	m := NewMemory(retention)
	base := time.Now()

	// Well outside the window once the newest sample lands.
	m.AddSnapshot(snapAt("svc", base, 0))
	m.AddSnapshot(snapAt("svc", base.Add(30*time.Second), 0))
	m.AddSnapshot(snapAt("svc", base.Add(5*time.Minute), 0))

	got := m.SnapshotsSince("svc", base.Add(-time.Hour))
	if len(got) != 1 {
		t.Fatalf("got %d retained snapshots, want 1: %+v", len(got), got)
	}
	if !got[0].Timestamp.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("retained the wrong snapshot: %v", got[0].Timestamp)
	}
}

func TestSnapshotsSinceIsInclusiveAndOrdered(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	base := time.Now()
	for i := 0; i < 5; i++ {
		m.AddSnapshot(snapAt("svc", base.Add(time.Duration(i)*time.Second), float64(i)))
	}

	got := m.SnapshotsSince("svc", base.Add(2*time.Second))
	if len(got) != 3 {
		t.Fatalf("got %d snapshots, want 3 (inclusive of the boundary)", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.Before(got[i-1].Timestamp) {
			t.Fatalf("snapshots are not in ascending time order")
		}
	}
}

func TestSpansSinceFiltersByStart(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	base := time.Now()

	m.AddSpan(model.Span{SpanID: "old", Service: "a", Start: base})
	m.AddSpan(model.Span{SpanID: "new", Service: "b", Start: base.Add(10 * time.Second)})

	got := m.SpansSince(base.Add(5 * time.Second))
	if len(got) != 1 || got[0].SpanID != "new" {
		t.Fatalf("got %+v, want only the newer span", got)
	}
}

func TestSpansSinceEmptyIsNonNil(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	// The API marshals this straight to JSON; nil would render as null rather
	// than an empty array.
	if got := m.SpansSince(time.Now()); got == nil {
		t.Fatalf("SpansSince returned nil, want an empty slice")
	}
}

func TestIncidentsAreNewestFirst(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	now := time.Now()

	m.SaveIncident(model.Incident{ID: "INC-1042", OpenedAt: now})
	m.SaveIncident(model.Incident{ID: "INC-1043", OpenedAt: now.Add(time.Minute)})

	got := m.Incidents()
	if len(got) != 2 {
		t.Fatalf("got %d incidents, want 2", len(got))
	}
	if got[0].ID != "INC-1043" {
		t.Fatalf("first incident = %q, want the newest (INC-1043)", got[0].ID)
	}
}

// TestSaveIncidentUpsertsWithoutDuplicating matters because the manager saves
// the same incident on every detection cycle.
func TestSaveIncidentUpsertsWithoutDuplicating(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	now := time.Now()

	m.SaveIncident(model.Incident{ID: "INC-1042", OpenedAt: now, Status: model.StatusDetected})
	m.SaveIncident(model.Incident{ID: "INC-1042", OpenedAt: now, Status: model.StatusInvestigating})
	m.SaveIncident(model.Incident{ID: "INC-1042", OpenedAt: now, Status: model.StatusResolved})

	got := m.Incidents()
	if len(got) != 1 {
		t.Fatalf("got %d incidents, want 1 after three saves of the same id", len(got))
	}
	if got[0].Status != model.StatusResolved {
		t.Fatalf("status = %v, want the latest write to win", got[0].Status)
	}
}

func TestIncidentLookupByID(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	m.SaveIncident(model.Incident{ID: "INC-1042", Severity: model.SevP1})

	got, ok := m.Incident("INC-1042")
	if !ok || got.Severity != model.SevP1 {
		t.Fatalf("lookup = (%+v, %v), want the saved incident", got, ok)
	}
	if _, ok := m.Incident("INC-9999"); ok {
		t.Fatalf("found an incident that was never saved")
	}
}

func TestNewMemoryRejectsNonPositiveRetention(t *testing.T) {
	if m := NewMemory(0); m.retention != 30*time.Minute {
		t.Fatalf("retention = %v, want the 30m default", m.retention)
	}
	if m := NewMemory(-time.Hour); m.retention != 30*time.Minute {
		t.Fatalf("retention = %v, want the 30m default", m.retention)
	}
}

// TestConcurrentAccessIsSafe exercises the mutex under -race: ingestion writes
// while the detection loop and the HTTP API read.
func TestConcurrentAccessIsSafe(t *testing.T) {
	m := NewMemory(30 * time.Minute)
	base := time.Now()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			m.AddSnapshot(snapAt("svc", base.Add(time.Duration(i)*time.Second), 0))
			m.AddSpan(model.Span{SpanID: "s", Service: "svc", Start: base})
			m.SaveIncident(model.Incident{ID: "INC-1042", OpenedAt: base})
		}
	}()

	for i := 0; i < 200; i++ {
		m.LatestSnapshot("svc")
		m.Services()
		m.SpansSince(base)
		m.Incidents()
	}
	<-done
}
