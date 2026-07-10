// Package store holds the platform's ingested telemetry and incidents behind a
// small interface. The default implementation is in-memory so the platform runs
// with no external database; a TimescaleDB-backed implementation satisfies the
// same interface when a DSN is configured (see timescale.go), so nothing
// upstream changes when persistence is turned on.
package store

import (
	"sort"
	"sync"
	"time"

	"github.com/ajay-1134/dirp/platform/model"
)

// Store is the persistence contract used by ingestion, detection, and the API.
type Store interface {
	// AddSnapshot records one service health snapshot.
	AddSnapshot(s model.ServiceSnapshot)
	// SnapshotsSince returns a service's snapshots at or after t, oldest first.
	SnapshotsSince(service string, t time.Time) []model.ServiceSnapshot
	// LatestSnapshot returns the most recent snapshot for a service.
	LatestSnapshot(service string) (model.ServiceSnapshot, bool)
	// Services lists every service seen so far.
	Services() []string

	// AddSpan records one ingested trace span.
	AddSpan(sp model.Span)
	// SpansSince returns all spans started at or after t.
	SpansSince(t time.Time) []model.Span

	// SaveIncident inserts or updates an incident by ID.
	SaveIncident(inc model.Incident)
	// Incident fetches one incident by ID.
	Incident(id string) (model.Incident, bool)
	// Incidents returns all incidents, newest first.
	Incidents() []model.Incident
}

// Memory is the in-memory Store. It bounds retention by a fixed window so a
// long-running demo doesn't grow without limit.
type Memory struct {
	retention time.Duration

	mu        sync.RWMutex
	snapshots map[string][]model.ServiceSnapshot
	spans     []model.Span
	incidents map[string]model.Incident
	incOrder  []string // incident IDs in insertion order
}

// NewMemory creates an in-memory store retaining data for the given window.
func NewMemory(retention time.Duration) *Memory {
	if retention <= 0 {
		retention = 30 * time.Minute
	}
	return &Memory{
		retention: retention,
		snapshots: make(map[string][]model.ServiceSnapshot),
		incidents: make(map[string]model.Incident),
	}
}

func (m *Memory) AddSnapshot(s model.ServiceSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	series := append(m.snapshots[s.Service], s)
	// Trim anything older than the retention window.
	cutoff := s.Timestamp.Add(-m.retention)
	i := 0
	for i < len(series) && series[i].Timestamp.Before(cutoff) {
		i++
	}
	m.snapshots[s.Service] = series[i:]
}

func (m *Memory) SnapshotsSince(service string, t time.Time) []model.ServiceSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	series := m.snapshots[service]
	out := make([]model.ServiceSnapshot, 0, len(series))
	for _, s := range series {
		if !s.Timestamp.Before(t) {
			out = append(out, s)
		}
	}
	return out
}

func (m *Memory) LatestSnapshot(service string) (model.ServiceSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	series := m.snapshots[service]
	if len(series) == 0 {
		return model.ServiceSnapshot{}, false
	}
	return series[len(series)-1], true
}

func (m *Memory) Services() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.snapshots))
	for svc := range m.snapshots {
		out = append(out, svc)
	}
	sort.Strings(out)
	return out
}

func (m *Memory) AddSpan(sp model.Span) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, sp)
	// Trim old spans relative to the newest span's start time.
	cutoff := sp.Start.Add(-m.retention)
	i := 0
	for i < len(m.spans) && m.spans[i].Start.Before(cutoff) {
		i++
	}
	m.spans = m.spans[i:]
}

func (m *Memory) SpansSince(t time.Time) []model.Span {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.Span, 0)
	for _, sp := range m.spans {
		if !sp.Start.Before(t) {
			out = append(out, sp)
		}
	}
	return out
}

func (m *Memory) SaveIncident(inc model.Incident) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.incidents[inc.ID]; !exists {
		m.incOrder = append(m.incOrder, inc.ID)
	}
	m.incidents[inc.ID] = inc
}

func (m *Memory) Incident(id string) (model.Incident, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inc, ok := m.incidents[id]
	return inc, ok
}

func (m *Memory) Incidents() []model.Incident {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.Incident, 0, len(m.incOrder))
	// Newest first.
	for i := len(m.incOrder) - 1; i >= 0; i-- {
		out = append(out, m.incidents[m.incOrder[i]])
	}
	return out
}

// compile-time check
var _ Store = (*Memory)(nil)
