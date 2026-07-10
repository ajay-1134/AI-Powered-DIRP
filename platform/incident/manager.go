// Package incident turns a stream of anomalies into managed incidents. It
// correlates the anomalies seen in a detection cycle into a single incident
// (the injected fault trips several services at once — Order's error rate,
// Payment's latency and pool — and they are one event, not three), computes
// severity from blast radius, drives the incident lifecycle state machine, and
// auto-resolves once the system is healthy again.
package incident

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ajay-1134/dirp/platform/anomaly"
	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

// Manager owns incident lifecycle. For this demo it tracks a single active
// incident at a time, which matches the single-fault scenario; the design
// extends to clustering by dependency subgraph for concurrent incidents.
type Manager struct {
	store        store.Store
	resolveAfter int // consecutive healthy cycles before auto-resolving

	mu            sync.Mutex
	active        *model.Incident
	evidence      map[string]model.Evidence // keyed by service|kind, newest wins
	healthyStreak int
	seq           int

	// onOpen fires once when an incident is opened, so the orchestrator can
	// kick off dependency-graph + AI analysis. onChange fires on every mutation
	// so the dashboard can stream updates.
	onOpen   func(model.Incident)
	onChange func(model.Incident)
}

// NewManager builds a manager. resolveAfter is how many consecutive
// anomaly-free cycles must pass before an incident auto-resolves.
func NewManager(s store.Store, resolveAfter int) *Manager {
	if resolveAfter < 1 {
		resolveAfter = 3
	}
	return &Manager{
		store:        s,
		resolveAfter: resolveAfter,
		seq:          1041, // so the first incident is the demo's INC-1042
		onOpen:       func(model.Incident) {},
		onChange:     func(model.Incident) {},
	}
}

// OnOpen registers the callback fired when a new incident opens.
func (m *Manager) OnOpen(fn func(model.Incident)) { m.onOpen = fn }

// OnChange registers the callback fired on every incident mutation.
func (m *Manager) OnChange(fn func(model.Incident)) { m.onChange = fn }

// Process consumes the anomalies from one detection cycle and advances incident
// state. It is the single entry point the detection loop calls each interval.
func (m *Manager) Process(anoms []anomaly.Anomaly, now time.Time) {
	m.mu.Lock()

	if len(anoms) == 0 {
		inc, resolved := m.handleHealthyLocked(now)
		m.mu.Unlock()
		if resolved {
			m.onChange(inc)
		}
		return
	}

	m.healthyStreak = 0
	opened := false
	if m.active == nil {
		m.openLocked(anoms, now)
		opened = true
	}
	m.applyEvidenceLocked(anoms, now)
	inc := *m.active
	m.store.SaveIncident(inc)
	m.mu.Unlock()

	// Fire callbacks outside the lock so analysis/broadcast can't deadlock us.
	if opened {
		m.onOpen(inc)
	}
	m.onChange(inc)
}

func (m *Manager) handleHealthyLocked(now time.Time) (model.Incident, bool) {
	if m.active == nil {
		return model.Incident{}, false
	}
	m.healthyStreak++
	if m.healthyStreak < m.resolveAfter {
		return model.Incident{}, false
	}
	m.active.Status = model.StatusResolved
	m.active.ResolvedAt = &now
	inc := *m.active
	m.store.SaveIncident(inc)
	m.active = nil
	m.evidence = nil
	return inc, true
}

func (m *Manager) openLocked(anoms []anomaly.Anomaly, now time.Time) {
	m.seq++
	affected := affectedService(anoms)
	m.active = &model.Incident{
		ID:              fmt.Sprintf("INC-%d", m.seq),
		Status:          model.StatusDetected,
		AffectedService: affected,
		Title:           fmt.Sprintf("Elevated errors on %s", affected),
		OpenedAt:        now,
	}
	m.evidence = make(map[string]model.Evidence)
}

func (m *Manager) applyEvidenceLocked(anoms []anomaly.Anomaly, now time.Time) {
	for _, a := range anoms {
		key := a.Service + "|" + string(a.Kind)
		m.evidence[key] = model.Evidence{
			Service: a.Service,
			Kind:    string(a.Kind),
			Detail:  a.Detail,
			Value:   a.Value,
			At:      now,
		}
	}
	m.active.Evidence = sortedEvidence(m.evidence)
	m.active.Severity = severity(m.active.Evidence)
	if m.active.Status == model.StatusDetected {
		m.active.Status = model.StatusInvestigating
	}
}

// SetRootCause attaches an analysis conclusion to an incident and advances its
// state. Called by the orchestrator after graph + AI analysis completes.
func (m *Manager) SetRootCause(id string, rc model.RootCause) {
	m.mu.Lock()
	inc, ok := m.store.Incident(id)
	if !ok {
		m.mu.Unlock()
		return
	}
	inc.RootCause = &rc
	if inc.Status != model.StatusResolved {
		inc.Status = model.StatusRootCause
	}
	if m.active != nil && m.active.ID == id {
		m.active.RootCause = &rc
		m.active.Status = inc.Status
	}
	m.store.SaveIncident(inc)
	m.mu.Unlock()
	m.onChange(inc)
}

// affectedService picks the service the incident is "about" for its title: the
// one showing elevated errors (the user-visible symptom), falling back to the
// first anomalous service.
func affectedService(anoms []anomaly.Anomaly) string {
	for _, a := range anoms {
		if a.Kind == anomaly.KindErrorRate {
			return a.Service
		}
	}
	return anoms[0].Service
}

// severity scales with blast radius and worst error rate. It is computed from
// the incident's accumulated evidence (its current state), not a single
// detection cycle — otherwise a cycle that happens to re-trip only the latency
// or pool signal would understate the severity.
func severity(ev []model.Evidence) model.Severity {
	services := map[string]struct{}{}
	worstErr := 0.0
	for _, e := range ev {
		services[e.Service] = struct{}{}
		if e.Kind == string(anomaly.KindErrorRate) && e.Value > worstErr {
			worstErr = e.Value
		}
	}
	switch {
	case worstErr >= 0.25 || len(services) >= 3:
		return model.SevP1
	case worstErr >= 0.10:
		return model.SevP2
	case worstErr >= 0.05:
		return model.SevP3
	default:
		return model.SevP4
	}
}

func sortedEvidence(m map[string]model.Evidence) []model.Evidence {
	out := make([]model.Evidence, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	// Stable, readable order: by service then kind.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}
