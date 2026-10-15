package anomaly

import (
	"math"
	"testing"

	"github.com/ajay-1134/dirp/platform/model"
)

// snap builds a snapshot for a service that serves traffic. RequestRate is
// non-zero because an interval with no requests is deliberately ignored by the
// detector (see TestEmptyIntervalsDoNotCorruptBaseline).
func snap(service string, errRate, p99 float64) model.ServiceSnapshot {
	return model.ServiceSnapshot{
		Service:     service,
		RequestRate: 12,
		ErrorRate:   errRate,
		P99Latency:  p99,
	}
}

// warm feeds n healthy samples so the detector leaves warmup with a baseline at
// the given latency and a zero error rate.
func warm(t *testing.T, d *Detector, service string, p99 float64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if got := d.Observe(snap(service, 0, p99)); len(got) != 0 {
			t.Fatalf("warmup sample %d unexpectedly produced anomalies: %+v", i, got)
		}
	}
}

func kinds(anoms []Anomaly) map[Kind]bool {
	out := map[Kind]bool{}
	for _, a := range anoms {
		out[a.Kind] = true
	}
	return out
}

func TestWarmupOnlyLearns(t *testing.T) {
	d := NewDetector(DefaultConfig())
	// Even a wild value during warmup must not alarm.
	for i := 0; i < DefaultConfig().Warmup; i++ {
		if got := d.Observe(snap("svc", 0.9, 9)); len(got) != 0 {
			t.Fatalf("sample %d during warmup produced anomalies: %+v", i, got)
		}
	}
}

// TestBucketJitterDoesNotAlarm is the regression test for the false-positive
// bug: p99 is derived from histogram buckets, so a healthy service sitting on
// the 10ms bound that drifts to the next bound (25ms) is a 2.5x jump against a
// near-zero-variance baseline. Both statistical gates clear, and before the
// absolute LatencyFloor existed this opened an incident on a system with no
// fault at all.
func TestBucketJitterDoesNotAlarm(t *testing.T) {
	for _, tc := range []struct {
		name          string
		base, drifted float64
	}{
		{"10ms to 25ms", 0.010, 0.025},
		{"25ms to 50ms", 0.025, 0.050},
		{"50ms to 100ms", 0.050, 0.100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDetector(DefaultConfig())
			warm(t, d, "svc", tc.base, DefaultConfig().Warmup)

			// Sanity: this really does clear both statistical gates, so the
			// floor is the only thing preventing the false positive.
			b := d.baseline["svc"]
			if z := b.latency.z(tc.drifted); z < DefaultConfig().ZThreshold {
				t.Fatalf("precondition failed: z=%v did not clear the z gate", z)
			}
			if tc.drifted < 2*b.latency.mean {
				t.Fatalf("precondition failed: %v is not 2x baseline %v", tc.drifted, b.latency.mean)
			}

			for i := 0; i < 10; i++ {
				if got := d.Observe(snap("svc", 0, tc.drifted)); len(got) != 0 {
					t.Fatalf("healthy bucket drift alarmed at sample %d: %+v", i, got)
				}
			}
		})
	}
}

// TestRealStallAlarms is the other half of the floor: a genuine dependency
// stall must still be caught. The downstream client times out at 3s, so a
// stalled hop surfaces in the seconds range.
func TestRealStallAlarms(t *testing.T) {
	d := NewDetector(DefaultConfig())
	warm(t, d, "payment-service", 0.05, DefaultConfig().Warmup)

	got := kinds(d.Observe(snap("payment-service", 0, 5.0)))
	if !got[KindLatency] {
		t.Fatalf("a 50ms -> 5s stall was not detected; got %v", got)
	}
}

func TestErrorRateFloorGatesSmallSpikes(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	warm(t, d, "svc", 0.05, cfg.Warmup)

	// 2% errors against a 0% baseline is statistically extreme but below the
	// absolute floor, so it must not alarm.
	if got := kinds(d.Observe(snap("svc", 0.02, 0.05))); got[KindErrorRate] {
		t.Fatalf("error rate below the floor alarmed")
	}
	// 30% clears the floor and must alarm.
	if got := kinds(d.Observe(snap("svc", 0.30, 0.05))); !got[KindErrorRate] {
		t.Fatalf("error rate of 30%% was not detected")
	}
}

func TestDBPoolSaturationIsThresholdBased(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	warm(t, d, "payment-service", 0.05, cfg.Warmup)

	// A pool is a gauge, not a baseline-relative signal: saturation alarms on
	// the threshold alone, with no warmup history of "pool was full before".
	s := snap("payment-service", 0, 0.05)
	s.DBInUse, s.DBMax = 10, 10
	got := d.Observe(s)
	if !kinds(got)[KindDBPool] {
		t.Fatalf("full pool was not detected: %+v", got)
	}
	for _, a := range got {
		if a.Kind == KindDBPool && a.Value != 1.0 {
			t.Fatalf("utilization = %v, want 1.0", a.Value)
		}
	}

	// Half-used is normal and must stay quiet.
	s.DBInUse = 5
	if kinds(d.Observe(s))[KindDBPool] {
		t.Fatalf("a half-used pool alarmed")
	}
}

// TestPoolSaturationDetectedWhenNoRequestsComplete is the case that makes pool
// detection independent of traffic. When the pool is fully exhausted, every
// request blocks until it times out, so a scrape interval can pass with zero
// completed requests and therefore a reported RequestRate of 0. The pool gauge
// is still live and still says 10/10 — and that is the decisive root-cause
// signal, so it must be reported rather than skipped along with the
// interval-derived signals.
func TestPoolSaturationDetectedWhenNoRequestsComplete(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	warm(t, d, "payment-service", 0.05, cfg.Warmup)

	stalled := model.ServiceSnapshot{
		Service:     "payment-service",
		RequestRate: 0, // everything is blocked on the pool; nothing completed
		DBInUse:     10,
		DBMax:       10,
	}

	got := kinds(d.Observe(stalled))
	if !got[KindDBPool] {
		t.Fatalf("pool exhaustion was missed on a stalled interval; got %v", got)
	}
	// The interval-derived signals must stay quiet: p99=0 and errorRate=0 here
	// mean "not measured", not "healthy".
	if got[KindLatency] || got[KindErrorRate] {
		t.Fatalf("interval signals alarmed on an empty interval: %v", got)
	}
}

// TestWarmupCountsOnlyRealTraffic makes sure silent startup scrapes cannot
// consume the warmup budget and leave the detector armed with no real baseline.
func TestWarmupCountsOnlyRealTraffic(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)

	for i := 0; i < 20; i++ {
		s := snap("svc", 0, 0)
		s.RequestRate = 0
		d.Observe(s)
	}
	if b := d.baseline["svc"]; b.n != 0 {
		t.Fatalf("warmup counter = %d after 20 silent scrapes, want 0", b.n)
	}
}

func TestNoPoolServiceNeverReportsPoolAnomaly(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	warm(t, d, "order-service", 0.05, cfg.Warmup)

	// DBMax == 0 means "no pool", which must read as 0% utilization rather than
	// a divide-by-zero or a spurious 100%.
	if kinds(d.Observe(snap("order-service", 0, 0.05)))[KindDBPool] {
		t.Fatalf("a service with no pool reported pool saturation")
	}
}

// TestEmptyIntervalsDoNotCorruptBaseline is the regression test for the
// stuck-baseline bug. Before startup traffic ramps, the scraper reports
// p99=0 and errorRate=0 for every service simply because nothing was measured.
// Training on those zeros pinned the baseline near zero; once real traffic
// arrived every sample looked like a huge multiple of it, and because an
// anomalous sample is never learned from, the baseline could never recover.
func TestEmptyIntervalsDoNotCorruptBaseline(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)

	// A deliberately slow-but-healthy service, above LatencyFloor so the floor
	// alone cannot be what saves us here.
	const normal = 0.5

	// Startup: several scrapes before any traffic arrives.
	for i := 0; i < 5; i++ {
		s := snap("slow-service", 0, 0)
		s.RequestRate = 0
		if got := d.Observe(s); len(got) != 0 {
			t.Fatalf("empty interval produced anomalies: %+v", got)
		}
	}

	// Traffic arrives; the detector should now warm up on real values only.
	warm(t, d, "slow-service", normal, cfg.Warmup)

	if b := d.baseline["slow-service"]; math.Abs(b.latency.mean-normal) > 1e-9 {
		t.Fatalf("baseline latency = %v, want %v (empty intervals leaked into it)", b.latency.mean, normal)
	}

	// Steady state at its true normal must stay quiet indefinitely.
	for i := 0; i < 20; i++ {
		if got := d.Observe(snap("slow-service", 0, normal)); len(got) != 0 {
			t.Fatalf("healthy steady state alarmed at sample %d: %+v", i, got)
		}
	}
}

// TestServiceRecoversAfterIncident is the behavior the platform's auto-resolution
// depends on: once the fault clears, the service must stop producing anomalies
// so the incident manager's healthy streak can build.
func TestServiceRecoversAfterIncident(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	const normal = 0.5
	warm(t, d, "payment-service", normal, cfg.Warmup)

	// Fault: sustained stall.
	for i := 0; i < 10; i++ {
		if !kinds(d.Observe(snap("payment-service", 0.4, 5.0)))[KindLatency] {
			t.Fatalf("stall not detected at sample %d", i)
		}
	}

	// Recovery: back to the pre-incident normal.
	for i := 0; i < 10; i++ {
		if got := d.Observe(snap("payment-service", 0, normal)); len(got) != 0 {
			t.Fatalf("recovered service still anomalous at sample %d: %+v", i, got)
		}
	}
}

// TestBaselineFrozenDuringIncident guards the intended freeze semantics: a
// sustained bad state must not be learned as the new normal, otherwise a long
// incident would silently resolve itself.
func TestBaselineFrozenDuringIncident(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	const normal = 0.5
	warm(t, d, "svc", normal, cfg.Warmup)

	for i := 0; i < 30; i++ {
		d.Observe(snap("svc", 0, 5.0))
	}

	if b := d.baseline["svc"]; math.Abs(b.latency.mean-normal) > 1e-9 {
		t.Fatalf("baseline drifted to %v during a sustained incident, want it pinned at %v",
			b.latency.mean, normal)
	}
	// Still anomalous after 30 cycles — the incident stays open.
	if !kinds(d.Observe(snap("svc", 0, 5.0)))[KindLatency] {
		t.Fatalf("sustained incident stopped being reported")
	}
}

func TestPerServiceBaselinesAreIndependent(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg)
	// A fast service and a slow one; the slow one's normal is 100x the fast
	// one's and neither should alarm on the other's account.
	warm(t, d, "fast", 0.005, cfg.Warmup)
	warm(t, d, "slow", 0.5, cfg.Warmup)

	if got := d.Observe(snap("slow", 0, 0.5)); len(got) != 0 {
		t.Fatalf("slow service alarmed at its own normal: %+v", got)
	}
	if got := d.Observe(snap("fast", 0, 0.005)); len(got) != 0 {
		t.Fatalf("fast service alarmed at its own normal: %+v", got)
	}
}

func TestEWMAZeroVarianceProducesInfiniteZ(t *testing.T) {
	e := newEWMA(0.2)
	for i := 0; i < 5; i++ {
		e.update(1.0)
	}
	if z := e.z(2.0); !math.IsInf(z, 1) {
		t.Fatalf("z for a jump off a perfectly stable signal = %v, want +Inf", z)
	}
	if z := e.z(0.5); z != 0 {
		t.Fatalf("z for a drop off a stable signal = %v, want 0 (we only alarm upward)", z)
	}
}

func TestEWMATracksMean(t *testing.T) {
	e := newEWMA(0.5)
	e.update(10) // first sample seeds the mean
	if e.mean != 10 {
		t.Fatalf("mean after seed = %v, want 10", e.mean)
	}
	e.update(20) // halfway with alpha 0.5
	if math.Abs(e.mean-15) > 1e-9 {
		t.Fatalf("mean = %v, want 15", e.mean)
	}
}
