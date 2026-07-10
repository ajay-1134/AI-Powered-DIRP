// Command platform is the DIRP control plane — the star of the system. It:
//
//	ingests   metrics (scraping each service's Prometheus endpoint) and traces
//	          (an embedded OTLP receiver);
//	detects   anomalies with an online EWMA/z-score engine;
//	correlates them into incidents with a lifecycle state machine;
//	analyzes  root cause by walking the trace-derived dependency graph and
//	          refining the conclusion with Claude;
//	serves    a live dashboard that streams incidents as they are diagnosed.
//
// It runs as a single binary. Storage is in-memory by default so the whole demo
// comes up with no external database.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/service"
	"github.com/ajay-1134/dirp/platform/ai"
	"github.com/ajay-1134/dirp/platform/anomaly"
	"github.com/ajay-1134/dirp/platform/dashboard"
	"github.com/ajay-1134/dirp/platform/depgraph"
	"github.com/ajay-1134/dirp/platform/incident"
	"github.com/ajay-1134/dirp/platform/ingest"
	"github.com/ajay-1134/dirp/platform/model"
	"github.com/ajay-1134/dirp/platform/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "platform")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Storage ----------------------------------------------------------
	// In-memory by default (zero external dependencies); TimescaleDB when
	// STORE_DSN is set (docker-compose provisions it). Both satisfy store.Store,
	// so nothing downstream changes.
	var st store.Store
	if dsn := os.Getenv("STORE_DSN"); dsn != "" {
		ts, err := store.NewTimescale(ctx, dsn)
		if err != nil {
			log.Error("timescale unavailable, falling back to in-memory", "err", err)
			st = store.NewMemory(30 * time.Minute)
		} else {
			st = ts
			log.Info("storage: timescaledb")
		}
	} else {
		st = store.NewMemory(30 * time.Minute)
		log.Info("storage: in-memory")
	}

	// --- Ingestion: metrics scraper --------------------------------------
	targets := scrapeTargets()
	scraper := ingest.NewScraper(targets, 2*time.Second, st)
	go scraper.Run(ctx)
	log.Info("metrics scraper started", "targets", len(targets))

	// --- Ingestion: embedded OTLP trace receiver -------------------------
	otlpAddr := service.Env("PLATFORM_OTLP_ADDR", ":4317")
	receiver := ingest.NewOTLPReceiver(otlpAddr, st)
	go func() {
		if err := receiver.Run(ctx); err != nil {
			log.Error("otlp receiver stopped", "err", err)
		}
	}()
	log.Info("otlp trace receiver started", "addr", otlpAddr)

	// --- Detection + incident management ---------------------------------
	detector := anomaly.NewDetector(anomaly.DefaultConfig())
	manager := incident.NewManager(st, 3)
	analyzer := ai.NewAnalyzer()
	log.Info("root-cause AI", "enabled", analyzer.Enabled())

	// --- Dashboard --------------------------------------------------------
	dash := dashboard.NewServer(st)
	manager.OnChange(dash.Broadcast)

	// When an incident opens, run the analysis pipeline asynchronously so the
	// detection loop is never blocked on graph work or a Claude round-trip.
	manager.OnOpen(func(inc model.Incident) {
		go analyze(ctx, log, st, manager, analyzer, inc)
	})

	// The detection loop is the heartbeat: every interval it reads the latest
	// snapshot per service, feeds the detector, and hands the resulting
	// anomalies to the incident manager.
	go detectionLoop(ctx, log, st, detector, manager)

	// --- HTTP server ------------------------------------------------------
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	dash.Register(engine)

	addr := service.Env("PLATFORM_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: engine, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("dashboard listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("dashboard server", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// detectionLoop drives anomaly detection on a fixed cadence.
func detectionLoop(ctx context.Context, log *slog.Logger, st store.Store, det *anomaly.Detector, mgr *incident.Manager) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			var anoms []anomaly.Anomaly
			for _, svc := range st.Services() {
				snap, ok := st.LatestSnapshot(svc)
				if !ok {
					continue
				}
				anoms = append(anoms, det.Observe(snap)...)
			}
			mgr.Process(anoms, now)
		}
	}
}

// analyze reconstructs the dependency graph for the incident window, derives a
// heuristic root cause, and refines it with Claude, then attaches the result.
func analyze(ctx context.Context, log *slog.Logger, st store.Store, mgr *incident.Manager, analyzer *ai.Analyzer, inc model.Incident) {
	// Give traces a moment to arrive for the incident window, then build the
	// graph from spans around the time the incident opened.
	time.Sleep(1500 * time.Millisecond)
	spans := st.SpansSince(inc.OpenedAt.Add(-2 * time.Minute))
	g := depgraph.Build(spans)

	sig := signalsFrom(inc)
	ga := g.FindRootCause(inc.AffectedService, sig)

	analysisCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rc := analyzer.Analyze(analysisCtx, inc, ga, g.EdgesList())

	log.Info("root cause determined",
		"incident", inc.ID, "service", rc.Service, "source", rc.Source, "confidence", rc.Confidence)
	mgr.SetRootCause(inc.ID, rc)
}

// signalsFrom translates an incident's evidence into the graph analyzer's
// Signals: which services are anomalous, which show DB saturation, and their
// error rates.
func signalsFrom(inc model.Incident) depgraph.Signals {
	sig := depgraph.Signals{
		Anomalous:   map[string]bool{},
		DBSaturated: map[string]bool{},
		ErrorRate:   map[string]float64{},
	}
	for _, ev := range inc.Evidence {
		sig.Anomalous[ev.Service] = true
		switch ev.Kind {
		case string(anomaly.KindDBPool):
			sig.DBSaturated[ev.Service] = true
		case string(anomaly.KindErrorRate):
			sig.ErrorRate[ev.Service] = ev.Value
		}
	}
	return sig
}

// scrapeTargets reads the SCRAPE_TARGETS env var ("name=url,name=url,...") or
// falls back to the five demo services on localhost.
func scrapeTargets() []ingest.Target {
	if raw := os.Getenv("SCRAPE_TARGETS"); raw != "" {
		var out []ingest.Target
		for _, pair := range strings.Split(raw, ",") {
			kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(kv) == 2 {
				out = append(out, ingest.Target{Service: kv[0], URL: kv[1]})
			}
		}
		return out
	}
	return []ingest.Target{
		{Service: "user-service", URL: "http://localhost:8081/metrics"},
		{Service: "order-service", URL: "http://localhost:8082/metrics"},
		{Service: "payment-service", URL: "http://localhost:8083/metrics"},
		{Service: "ledger-service", URL: "http://localhost:8084/metrics"},
		{Service: "notification-service", URL: "http://localhost:8085/metrics"},
	}
}
