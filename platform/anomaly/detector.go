// Package anomaly implements the detection engine. For each service it keeps an
// online statistical baseline (exponentially-weighted mean and variance) of the
// key signals — error rate, p99 latency, DB pool utilization — and flags a
// snapshot as anomalous when it deviates sharply from that baseline. Online
// EWMA/z-score is deliberately chosen over a fixed threshold: it adapts to each
// service's normal behavior, so the same detector works for a service that
// normally runs at 5ms and one that runs at 500ms.
package anomaly

import (
	"fmt"
	"math"

	"github.com/ajay-1134/dirp/platform/model"
)

// Kind identifies which signal tripped.
type Kind string

const (
	KindErrorRate Kind = "error_rate"
	KindLatency   Kind = "p99_latency"
	KindDBPool    Kind = "db_pool"
)

// Anomaly is a single detection: what tripped, on which service, how far from
// baseline, and a human-readable detail suitable for evidence and prompts.
type Anomaly struct {
	Service  string
	Kind     Kind
	Value    float64
	Baseline float64
	Score    float64 // z-score (std deviations from baseline)
	Detail   string
	Snapshot model.ServiceSnapshot
}

// Config tunes sensitivity.
type Config struct {
	// Warmup is how many samples a service needs before detection engages, so
	// early noise doesn't open spurious incidents.
	Warmup int
	// Alpha is the EWMA smoothing factor (0..1); higher tracks recent values
	// faster. 0.2 gives a baseline over roughly the last ~10 samples.
	Alpha float64
	// ZThreshold is the z-score above which a deviation is anomalous.
	ZThreshold float64
	// ErrorRateFloor is an absolute error-rate gate: below it we never alarm,
	// regardless of z-score, so a jump from 0 to 2 errors doesn't page anyone.
	ErrorRateFloor float64
	// DBUtilThreshold is the pool-utilization fraction considered saturated.
	DBUtilThreshold float64
}

// DefaultConfig returns sensible defaults for the demo's timings.
func DefaultConfig() Config {
	return Config{
		Warmup:          5,
		Alpha:           0.2,
		ZThreshold:      3.0,
		ErrorRateFloor:  0.05,
		DBUtilThreshold: 0.9,
	}
}

// Detector holds per-service baselines and applies the detection rules.
type Detector struct {
	cfg      Config
	baseline map[string]*serviceBaseline
}

// NewDetector builds a detector with the given config.
func NewDetector(cfg Config) *Detector {
	return &Detector{cfg: cfg, baseline: make(map[string]*serviceBaseline)}
}

// serviceBaseline tracks EWMA mean and variance per signal for one service.
type serviceBaseline struct {
	n         int
	errorRate ewma
	latency   ewma
}

// Observe feeds a snapshot to the detector and returns any anomalies it trips.
// It updates baselines only with values that are themselves not anomalous, so a
// sustained incident doesn't "learn" the bad state as normal.
func (d *Detector) Observe(s model.ServiceSnapshot) []Anomaly {
	b, ok := d.baseline[s.Service]
	if !ok {
		b = &serviceBaseline{
			errorRate: newEWMA(d.cfg.Alpha),
			latency:   newEWMA(d.cfg.Alpha),
		}
		d.baseline[s.Service] = b
	}

	var out []Anomaly

	// During warmup, only learn; don't detect.
	if b.n < d.cfg.Warmup {
		b.errorRate.update(s.ErrorRate)
		b.latency.update(s.P99Latency)
		b.n++
		return nil
	}

	// --- Error rate -------------------------------------------------------
	errZ := b.errorRate.z(s.ErrorRate)
	errAnom := s.ErrorRate >= d.cfg.ErrorRateFloor && errZ >= d.cfg.ZThreshold
	if errAnom {
		out = append(out, Anomaly{
			Service:  s.Service,
			Kind:     KindErrorRate,
			Value:    s.ErrorRate,
			Baseline: b.errorRate.mean,
			Score:    errZ,
			Detail: fmt.Sprintf("error rate %.1f%% (baseline %.1f%%, %.1fσ)",
				s.ErrorRate*100, b.errorRate.mean*100, errZ),
			Snapshot: s,
		})
	}

	// --- p99 latency ------------------------------------------------------
	latZ := b.latency.z(s.P99Latency)
	// Require both a statistical spike and a meaningful absolute multiple so a
	// 2ms->6ms blip on a fast service doesn't alarm.
	latAnom := latZ >= d.cfg.ZThreshold && b.latency.mean > 0 && s.P99Latency >= 2*b.latency.mean
	if latAnom {
		pct := 0.0
		if b.latency.mean > 0 {
			pct = (s.P99Latency/b.latency.mean - 1) * 100
		}
		out = append(out, Anomaly{
			Service:  s.Service,
			Kind:     KindLatency,
			Value:    s.P99Latency,
			Baseline: b.latency.mean,
			Score:    latZ,
			Detail: fmt.Sprintf("p99 latency %.0fms, up %.0f%% from baseline %.0fms",
				s.P99Latency*1000, pct, b.latency.mean*1000),
			Snapshot: s,
		})
	}

	// --- DB pool saturation ----------------------------------------------
	// This is a threshold, not a z-score: a full pool is definitionally bad and
	// is the strongest possible root-cause signal, so we always surface it.
	if util := s.DBUtilization(); util >= d.cfg.DBUtilThreshold {
		out = append(out, Anomaly{
			Service:  s.Service,
			Kind:     KindDBPool,
			Value:    util,
			Baseline: d.cfg.DBUtilThreshold,
			Score:    math.Inf(1),
			Detail: fmt.Sprintf("DB connection pool %d/%d in use (%.0f%%)",
				s.DBInUse, s.DBMax, util*100),
			Snapshot: s,
		})
	}

	// Only update baselines when the service looks healthy, so an ongoing
	// incident doesn't drag the baseline up to meet it.
	if !errAnom && !latAnom {
		b.errorRate.update(s.ErrorRate)
		b.latency.update(s.P99Latency)
	}
	b.n++

	return out
}

// ewma is an exponentially-weighted running mean and variance, giving us a
// cheap online z-score without storing history.
type ewma struct {
	alpha    float64
	mean     float64
	variance float64
	init     bool
}

func newEWMA(alpha float64) ewma { return ewma{alpha: alpha} }

func (e *ewma) update(x float64) {
	if !e.init {
		e.mean = x
		e.variance = 0
		e.init = true
		return
	}
	// West's incremental EWMA variance.
	diff := x - e.mean
	incr := e.alpha * diff
	e.mean += incr
	e.variance = (1 - e.alpha) * (e.variance + diff*incr)
}

// z returns how many standard deviations x is above the mean. When variance is
// ~0 (a very stable signal) but x jumps, we return a large score so the
// deviation still registers.
func (e *ewma) z(x float64) float64 {
	if !e.init {
		return 0
	}
	std := math.Sqrt(e.variance)
	if std < 1e-9 {
		if x > e.mean {
			return math.Inf(1)
		}
		return 0
	}
	return (x - e.mean) / std
}
