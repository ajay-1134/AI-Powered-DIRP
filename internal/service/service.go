// Package service is the shared scaffolding that every sample microservice is
// built on. It owns the cross-cutting concerns — routing, telemetry wiring,
// health checks, graceful shutdown — so each service's main.go only declares
// its own business handlers. Uniformity here is deliberate: the incident
// platform can only correlate services that emit signals identically.
package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/ajay-1134/dirp/internal/telemetry"
)

// Config describes a service instance.
type Config struct {
	Name         string // logical service name, e.g. "order-service"
	Addr         string // listen address, e.g. ":8082"
	OTLPEndpoint string // OpenTelemetry collector gRPC endpoint (may be empty)
}

// Server is a ready-to-run Gin HTTP service with telemetry already wired in.
type Server struct {
	cfg     Config
	log     *slog.Logger
	tel     *telemetry.Provider
	metrics *telemetry.Metrics
	engine  *gin.Engine
	http    *http.Server
}

// New builds a Server: it initializes telemetry, installs the standard
// middleware chain, and mounts /healthz and /metrics. Register business routes
// on Router(), then call Run.
func New(ctx context.Context, cfg Config) (*Server, error) {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("service", cfg.Name)

	tel, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:  cfg.Name,
		OTLPEndpoint: cfg.OTLPEndpoint,
	})
	if err != nil {
		return nil, err
	}

	metrics := telemetry.NewMetrics(cfg.Name)

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	// Middleware order matters: Recovery first so panics become 500s and are
	// still measured; otelgin opens the server span and extracts the incoming
	// W3C trace context so this service's spans join the caller's trace;
	// metrics last so it wraps the handler and observes the final status code.
	engine.Use(gin.Recovery())
	engine.Use(otelgin.Middleware(cfg.Name))
	engine.Use(metrics.Middleware())

	engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	engine.GET("/metrics", metrics.Handler())

	return &Server{
		cfg:     cfg,
		log:     log,
		tel:     tel,
		metrics: metrics,
		engine:  engine,
	}, nil
}

// Router exposes the Gin engine so a service can register its own routes.
func (s *Server) Router() *gin.Engine { return s.engine }

// Metrics exposes the metrics registry so a service (e.g. Payment) can publish
// custom instruments like DB pool utilization.
func (s *Server) Metrics() *telemetry.Metrics { return s.metrics }

// Logger returns the service's structured logger.
func (s *Server) Logger() *slog.Logger { return s.log }

// Run starts the HTTP server and blocks until the process receives SIGINT or
// SIGTERM, then shuts down gracefully and flushes telemetry.
func (s *Server) Run() error {
	s.http = &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("listening", "addr", s.cfg.Addr)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.http.Shutdown(shutdownCtx); err != nil {
		s.log.Error("http shutdown", "err", err)
	}
	if err := s.tel.Shutdown(shutdownCtx); err != nil {
		s.log.Error("telemetry shutdown", "err", err)
	}
	return nil
}
