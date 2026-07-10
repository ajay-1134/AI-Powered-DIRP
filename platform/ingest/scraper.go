// Package ingest pulls telemetry into the platform. The Scraper is the metrics
// side: it periodically fetches each service's Prometheus /metrics endpoint,
// parses the exposition format, and reduces the raw families into a compact
// ServiceSnapshot (request rate, error rate, p99 latency, DB pool state). Rates
// are computed as deltas between consecutive scrapes, exactly like Prometheus's
// own rate() would.
package ingest

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"

	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

// Target is a service to scrape.
type Target struct {
	Service string // logical name, must match the service's "service" label
	URL     string // full metrics URL, e.g. "http://localhost:8083/metrics"
}

// Scraper polls targets and writes snapshots to the store.
type Scraper struct {
	targets  []Target
	interval time.Duration
	store    store.Store
	client   *http.Client

	// prev holds the last scrape's cumulative counters per service, used to
	// turn monotonic counters into per-interval rates.
	prev map[string]counterState
}

type counterState struct {
	at        time.Time
	totalReqs float64
	totalErrs float64
	// buckets holds the previous scrape's cumulative histogram bucket counts
	// (keyed by upper bound) and total count, so p99 can be computed over the
	// per-interval delta — the same thing Prometheus's histogram_quantile(rate)
	// does. Using cumulative buckets instead would let a past latency spike peg
	// p99 high forever and prevent incident resolution.
	buckets     map[float64]uint64
	bucketCount uint64
}

// NewScraper builds a scraper for the given targets and scrape interval.
func NewScraper(targets []Target, interval time.Duration, s store.Store) *Scraper {
	return &Scraper{
		targets:  targets,
		interval: interval,
		store:    s,
		client:   &http.Client{Timeout: 3 * time.Second},
		prev:     make(map[string]counterState),
	}
}

// Run scrapes on a ticker until ctx is cancelled.
func (sc *Scraper) Run(ctx context.Context) {
	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()
	// Scrape once immediately so we don't wait a full interval for first data.
	sc.scrapeAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sc.scrapeAll(ctx)
		}
	}
}

func (sc *Scraper) scrapeAll(ctx context.Context) {
	now := time.Now()
	for _, t := range sc.targets {
		snap, err := sc.scrapeOne(ctx, t, now)
		if err != nil {
			continue // a down target is itself a signal, but we keep scraping others
		}
		sc.store.AddSnapshot(snap)
	}
}

func (sc *Scraper) scrapeOne(ctx context.Context, t Target, now time.Time) (model.ServiceSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return model.ServiceSnapshot{}, err
	}
	resp, err := sc.client.Do(req)
	if err != nil {
		return model.ServiceSnapshot{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.ServiceSnapshot{}, err
	}

	families, err := parseMetrics(body)
	if err != nil {
		return model.ServiceSnapshot{}, err
	}

	totalReqs, totalErrs := sumRequests(families["http_requests_total"])
	curBuckets, curCount := aggregateBuckets(families["http_request_duration_seconds"])
	dbInUse := gaugeValue(families["db_connections_in_use"])
	dbMax := gaugeValue(families["db_connections_max"])
	dbWait := counterValue(families["db_connection_wait_total"])

	snap := model.ServiceSnapshot{
		Service:     t.Service,
		Timestamp:   now,
		DBInUse:     int(dbInUse),
		DBMax:       int(dbMax),
		DBWaitTotal: int64(dbWait),
	}

	// Convert cumulative counters/histograms into per-interval quantities using
	// the delta since the previous scrape.
	if prev, ok := sc.prev[t.Service]; ok {
		dt := now.Sub(prev.at).Seconds()
		if dt > 0 {
			dReqs := totalReqs - prev.totalReqs
			dErrs := totalErrs - prev.totalErrs
			if dReqs < 0 { // counter reset (service restart)
				dReqs, dErrs = 0, 0
			}
			snap.RequestRate = dReqs / dt
			if dReqs > 0 {
				snap.ErrorRate = dErrs / dReqs
			}
		}
		// p99 over the requests seen in this interval only.
		snap.P99Latency = p99FromDelta(prev.buckets, prev.bucketCount, curBuckets, curCount)
	}
	sc.prev[t.Service] = counterState{
		at:          now,
		totalReqs:   totalReqs,
		totalErrs:   totalErrs,
		buckets:     curBuckets,
		bucketCount: curCount,
	}

	return snap, nil
}

// parseMetrics parses Prometheus text-format bytes into metric families keyed
// by metric name.
func parseMetrics(body []byte) (map[string]*dto.MetricFamily, error) {
	// NewTextParser sets the metric-name validation scheme on the parser
	// itself; a zero-value TextParser defaults to an unset scheme and panics
	// during parsing. Our services emit standard (legacy) metric names.
	parser := expfmt.NewTextParser(prommodel.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	return families, nil
}

// sumRequests totals all http_requests_total series and, separately, those with
// a 5xx status label — the numerator and denominator of the error rate.
func sumRequests(mf *dto.MetricFamily) (total, errors float64) {
	if mf == nil {
		return 0, 0
	}
	for _, m := range mf.GetMetric() {
		v := m.GetCounter().GetValue()
		total += v
		for _, l := range m.GetLabel() {
			if l.GetName() == "status" && strings.HasPrefix(l.GetValue(), "5") {
				errors += v
			}
		}
	}
	return total, errors
}

// aggregateBuckets sums all series of a histogram family into a single set of
// cumulative bucket counts (keyed by upper bound) plus the total sample count.
// These are cumulative-since-start; callers diff two snapshots to get a window.
func aggregateBuckets(mf *dto.MetricFamily) (map[float64]uint64, uint64) {
	agg := map[float64]uint64{}
	var totalCount uint64
	if mf == nil {
		return agg, 0
	}
	for _, m := range mf.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		totalCount += h.GetSampleCount()
		for _, b := range h.GetBucket() {
			agg[b.GetUpperBound()] += b.GetCumulativeCount()
		}
	}
	return agg, totalCount
}

// p99FromDelta interpolates the 99th percentile over only the requests observed
// between the previous and current cumulative histogram snapshots. This mirrors
// histogram_quantile(0.99, rate(..._bucket[interval])) and ensures a past
// latency spike stops influencing p99 once it ages out of the window — which is
// what allows an incident to auto-resolve after recovery.
func p99FromDelta(prev map[float64]uint64, prevCount uint64, cur map[float64]uint64, curCount uint64) float64 {
	deltaCount := int64(curCount) - int64(prevCount)
	if deltaCount <= 0 {
		return 0 // no new requests this interval
	}
	type bucket struct {
		le    float64
		count int64 // per-interval cumulative count at this bound
	}
	buckets := make([]bucket, 0, len(cur))
	for le, c := range cur {
		d := int64(c) - int64(prev[le])
		if d < 0 {
			d = 0 // counter reset
		}
		buckets = append(buckets, bucket{le: le, count: d})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })

	target := 0.99 * float64(deltaCount)
	for i, b := range buckets {
		if float64(b.count) >= target {
			if math.IsInf(b.le, 1) {
				if i >= 1 {
					return buckets[i-1].le // largest finite bound as lower estimate
				}
				return 0
			}
			return b.le
		}
	}
	if len(buckets) == 0 {
		return 0
	}
	return buckets[len(buckets)-1].le
}

func gaugeValue(mf *dto.MetricFamily) float64 {
	if mf == nil || len(mf.GetMetric()) == 0 {
		return 0
	}
	return mf.GetMetric()[0].GetGauge().GetValue()
}

func counterValue(mf *dto.MetricFamily) float64 {
	if mf == nil || len(mf.GetMetric()) == 0 {
		return 0
	}
	return mf.GetMetric()[0].GetCounter().GetValue()
}
