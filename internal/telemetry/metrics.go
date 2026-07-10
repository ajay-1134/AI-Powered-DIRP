package telemetry

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the standard RED-method instruments (Rate, Errors, Duration)
// that every service exposes. The platform scrapes these to feed its anomaly
// detector: a latency histogram and a status-coded request counter are enough
// to compute error rate and p99 per service.
type Metrics struct {
	reg         *prometheus.Registry
	service     string
	reqDuration *prometheus.HistogramVec
	reqTotal    *prometheus.CounterVec
	inFlight    prometheus.Gauge
	dbPoolInUse prometheus.Gauge
	dbPoolMax   prometheus.Gauge
	dbWaitCount prometheus.Counter
}

// NewMetrics creates a fresh registry and registers the standard instruments,
// labeling everything with the service name so scraped series are unambiguous.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	constLabels := prometheus.Labels{"service": service}

	m := &Metrics{
		reg:     reg,
		service: service,
		reqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "HTTP request latency in seconds.",
			ConstLabels: constLabels,
			// Buckets tuned for web-service latencies from 1ms to ~16s so the
			// platform can resolve a p99 that jumps from 20ms to multiple
			// seconds during the injected incident.
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 16},
		}, []string{"method", "route", "status"}),
		reqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "Total HTTP requests by status.",
			ConstLabels: constLabels,
		}, []string{"method", "route", "status"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "http_requests_in_flight",
			Help:        "In-flight HTTP requests.",
			ConstLabels: constLabels,
		}),
		dbPoolInUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "db_connections_in_use",
			Help:        "Database connections currently in use.",
			ConstLabels: constLabels,
		}),
		dbPoolMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "db_connections_max",
			Help:        "Maximum database connections allowed.",
			ConstLabels: constLabels,
		}),
		dbWaitCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "db_connection_wait_total",
			Help:        "Total number of times a caller waited for a DB connection.",
			ConstLabels: constLabels,
		}),
	}

	reg.MustRegister(
		m.reqDuration, m.reqTotal, m.inFlight,
		m.dbPoolInUse, m.dbPoolMax, m.dbWaitCount,
		// Go runtime + process metrics come free and are useful evidence
		// (goroutine leaks, GC pauses) during an incident.
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return m
}

// Handler returns the /metrics HTTP handler for this service's registry,
// adapted to Gin's handler signature.
func (m *Metrics) Handler() gin.HandlerFunc {
	h := promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
	return gin.WrapH(h)
}

// Middleware records duration, count, and in-flight gauge for every request.
// It uses Gin's matched route (c.FullPath, e.g. "/orders/:id") so the "route"
// label stays low-cardinality rather than exploding per unique id.
func (m *Metrics) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		m.inFlight.Inc()
		defer m.inFlight.Dec()

		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path // unmatched route (e.g. 404)
		}
		status := strconv.Itoa(c.Writer.Status())
		elapsed := time.Since(start).Seconds()
		m.reqDuration.WithLabelValues(c.Request.Method, route, status).Observe(elapsed)
		m.reqTotal.WithLabelValues(c.Request.Method, route, status).Inc()
	}
}

// SetDBPoolStats lets a service publish its current DB pool utilization. The
// Payment service calls this on a ticker so pool exhaustion is visible to the
// platform as first-class evidence, not just an inferred symptom.
func (m *Metrics) SetDBPoolStats(inUse, max int) {
	m.dbPoolInUse.Set(float64(inUse))
	m.dbPoolMax.Set(float64(max))
}

// ObserveDBWait increments the DB connection-wait counter.
func (m *Metrics) ObserveDBWait() { m.dbWaitCount.Inc() }
