package ingest

import (
	"math"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// bounds mirrors the histogram buckets the services register in
// internal/telemetry, plus the implicit +Inf bucket Prometheus always emits.
var bounds = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 16, math.Inf(1)}

// cumulative builds the cumulative bucket counts a Prometheus histogram would
// expose after observing the given latencies.
func cumulative(latencies ...float64) (map[float64]uint64, uint64) {
	m := make(map[float64]uint64, len(bounds))
	for _, b := range bounds {
		m[b] = 0
	}
	for _, l := range latencies {
		for _, b := range bounds {
			if l <= b {
				m[b]++
			}
		}
	}
	return m, uint64(len(latencies))
}

// merge returns the cumulative state after observing more latencies on top of a
// previous state — i.e. what the next scrape would see.
func merge(prev map[float64]uint64, prevCount uint64, more ...float64) (map[float64]uint64, uint64) {
	add, addCount := cumulative(more...)
	out := make(map[float64]uint64, len(prev))
	for b, c := range prev {
		out[b] = c + add[b]
	}
	return out, prevCount + addCount
}

func repeat(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func TestP99FromDeltaNoTraffic(t *testing.T) {
	prev, prevCount := cumulative(repeat(0.005, 50)...)
	// Nothing new observed this interval.
	if got := p99FromDelta(prev, prevCount, prev, prevCount); got != 0 {
		t.Fatalf("p99 with no new requests = %v, want 0", got)
	}
}

// TestP99FromDeltaIgnoresAgedOutSpike is the windowed-percentile guarantee: a
// latency spike that happened in an earlier interval must stop influencing p99
// once it ages out. Computing over cumulative buckets instead would keep p99
// pegged high forever and an incident would never auto-resolve.
func TestP99FromDeltaIgnoresAgedOutSpike(t *testing.T) {
	// Interval 1: a hundred fast requests and one very slow one.
	prev, prevCount := cumulative(append(repeat(0.005, 100), 5.0)...)

	// Interval 2: a hundred fast requests, nothing slow.
	cur, curCount := merge(prev, prevCount, repeat(0.005, 100)...)

	got := p99FromDelta(prev, prevCount, cur, curCount)
	if got != 0.005 {
		t.Fatalf("windowed p99 = %v, want 0.005 — the previous interval's spike leaked in", got)
	}
}

func TestP99FromDeltaReflectsCurrentSpike(t *testing.T) {
	prev, prevCount := cumulative(repeat(0.005, 100)...)
	// This interval: 10% of requests stall at 5s.
	cur, curCount := merge(prev, prevCount, append(repeat(0.005, 90), repeat(5.0, 10)...)...)

	got := p99FromDelta(prev, prevCount, cur, curCount)
	if got != 5.0 {
		t.Fatalf("windowed p99 = %v, want 5.0", got)
	}
}

func TestP99FromDeltaHandlesCounterReset(t *testing.T) {
	prev, prevCount := cumulative(repeat(0.005, 500)...)
	// The service restarted: counters are now lower than the previous scrape.
	cur, curCount := cumulative(repeat(0.005, 3)...)

	if got := p99FromDelta(prev, prevCount, cur, curCount); got != 0 {
		t.Fatalf("p99 across a counter reset = %v, want 0", got)
	}
}

func sp(s string) *string   { return &s }
func fp(f float64) *float64 { return &f }

func counterFamily(series ...struct {
	status string
	value  float64
}) *dto.MetricFamily {
	mf := &dto.MetricFamily{Name: sp("http_requests_total")}
	for _, s := range series {
		mf.Metric = append(mf.Metric, &dto.Metric{
			Label:   []*dto.LabelPair{{Name: sp("status"), Value: sp(s.status)}},
			Counter: &dto.Counter{Value: fp(s.value)},
		})
	}
	return mf
}

func TestSumRequestsSplitsErrorsByStatus(t *testing.T) {
	mf := counterFamily(
		struct {
			status string
			value  float64
		}{"200", 900},
		struct {
			status string
			value  float64
		}{"404", 50},
		struct {
			status string
			value  float64
		}{"500", 30},
		struct {
			status string
			value  float64
		}{"503", 20},
	)

	total, errs := sumRequests(mf)
	if total != 1000 {
		t.Fatalf("total = %v, want 1000", total)
	}
	// Only 5xx counts as a server error; 4xx is a client problem.
	if errs != 50 {
		t.Fatalf("errors = %v, want 50 (5xx only)", errs)
	}
}

func TestSumRequestsNilFamily(t *testing.T) {
	total, errs := sumRequests(nil)
	if total != 0 || errs != 0 {
		t.Fatalf("got (%v, %v), want (0, 0) for a missing family", total, errs)
	}
}

// TestParseMetricsUsesLegacyValidation guards a real trap: a zero-value
// expfmt.TextParser has an unset name-validation scheme and panics during
// parsing in prometheus/common v0.66+. parseMetrics must construct the parser
// with an explicit scheme.
func TestParseMetricsUsesLegacyValidation(t *testing.T) {
	const exposition = `# HELP http_requests_total Total HTTP requests by status.
# TYPE http_requests_total counter
http_requests_total{service="payment-service",method="POST",route="/charges",status="200"} 900
http_requests_total{service="payment-service",method="POST",route="/charges",status="503"} 100
# HELP db_connections_in_use Database connections currently in use.
# TYPE db_connections_in_use gauge
db_connections_in_use{service="payment-service"} 10
# HELP db_connections_max Maximum database connections allowed.
# TYPE db_connections_max gauge
db_connections_max{service="payment-service"} 10
`

	families, err := parseMetrics([]byte(exposition))
	if err != nil {
		t.Fatalf("parseMetrics: %v", err)
	}

	total, errs := sumRequests(families["http_requests_total"])
	if total != 1000 || errs != 100 {
		t.Fatalf("got (total=%v, errors=%v), want (1000, 100)", total, errs)
	}
	if v := gaugeValue(families["db_connections_in_use"]); v != 10 {
		t.Fatalf("db_connections_in_use = %v, want 10", v)
	}
	if v := gaugeValue(families["db_connections_max"]); v != 10 {
		t.Fatalf("db_connections_max = %v, want 10", v)
	}
}

func TestParseMetricsRejectsGarbage(t *testing.T) {
	if _, err := parseMetrics([]byte("this is not exposition format {{{\n")); err == nil {
		t.Fatalf("expected an error for malformed exposition text")
	}
}

func TestAggregateBucketsSumsAcrossSeries(t *testing.T) {
	// Two series of the same histogram family (e.g. two routes) must be summed
	// into one set of bucket counts.
	mk := func(count uint64, b1, b2 uint64) *dto.Metric {
		return &dto.Metric{Histogram: &dto.Histogram{
			SampleCount: &count,
			Bucket: []*dto.Bucket{
				{UpperBound: fp(0.005), CumulativeCount: &b1},
				{UpperBound: fp(0.01), CumulativeCount: &b2},
			},
		}}
	}
	mf := &dto.MetricFamily{
		Name:   sp("http_request_duration_seconds"),
		Metric: []*dto.Metric{mk(10, 8, 10), mk(5, 1, 5)},
	}

	got, count := aggregateBuckets(mf)
	if count != 15 {
		t.Fatalf("sample count = %v, want 15", count)
	}
	if got[0.005] != 9 {
		t.Fatalf("bucket 0.005 = %v, want 9", got[0.005])
	}
	if got[0.01] != 15 {
		t.Fatalf("bucket 0.01 = %v, want 15", got[0.01])
	}
}

func TestGaugeAndCounterValueNilSafe(t *testing.T) {
	if v := gaugeValue(nil); v != 0 {
		t.Fatalf("gaugeValue(nil) = %v, want 0", v)
	}
	if v := counterValue(nil); v != 0 {
		t.Fatalf("counterValue(nil) = %v, want 0", v)
	}
}
