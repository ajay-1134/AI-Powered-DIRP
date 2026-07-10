// Command payment charges a customer and is the deliberate weak point of the
// demo. Each charge leases a connection from a bounded pool to "talk to the
// payments DB". A chaos endpoint can exhaust that pool, at which point charges
// block waiting for a connection — driving up p99 latency, tripping the
// upstream Order timeout, and spiking the system-wide error rate. This is the
// root cause the platform must ultimately discover.
package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/chaos"
	"github.com/ajay-1134/dirp/internal/service"
)

type chargeRequest struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

func main() {
	ctx := context.Background()
	srv, err := service.New(ctx, service.Config{
		Name:         "payment-service",
		Addr:         service.Env("PAYMENT_ADDR", ":8083"),
		OTLPEndpoint: service.Env("OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("init payment-service: %v", err)
	}

	// A small pool makes exhaustion easy to trigger and easy to reason about.
	maxConns := envInt("PAYMENT_DB_MAX_CONNS", 10)
	pool := chaos.NewPool(maxConns)
	ctl := chaos.NewController(pool)
	ledger := service.NewClient("ledger-service",
		service.Env("LEDGER_URL", "http://localhost:8084"))

	// Continuously publish pool utilization so the platform sees exhaustion as
	// first-class evidence (db_connections_in_use approaching db_connections_max)
	// rather than only inferring it from latency.
	go publishPoolStats(srv, pool)

	srv.Router().POST("/charges", func(c *gin.Context) {
		var req chargeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Acquire a DB connection. Under exhaustion this blocks until the
		// request context (propagated from the caller) is cancelled/timed out.
		conn, err := pool.Acquire(c.Request.Context())
		if err != nil {
			srv.Metrics().ObserveDBWait()
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":    "payment db unavailable: " + err.Error(),
				"order_id": req.OrderID,
			})
			return
		}
		defer conn.Release()

		// Simulate the query that persists the charge.
		time.Sleep(12 * time.Millisecond)
		paymentID := "pay_" + req.OrderID

		// Record the entry in the ledger within the same trace.
		var ledgerResp map[string]any
		if err := ledger.PostJSON(c.Request.Context(), "/entries", gin.H{
			"order_id":   req.OrderID,
			"user_id":    req.UserID,
			"amount":     req.Amount,
			"payment_id": paymentID,
		}, &ledgerResp); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error":    "ledger call failed: " + err.Error(),
				"order_id": req.OrderID,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":     "charged",
			"payment_id": paymentID,
			"order_id":   req.OrderID,
		})
	})

	// --- Chaos control plane -------------------------------------------------
	// POST /chaos/db-pool-exhaust?holders=10&duration=60s  -> inject
	// POST /chaos/reset                                    -> clear
	// GET  /chaos/status                                   -> current state
	srv.Router().POST("/chaos/db-pool-exhaust", func(c *gin.Context) {
		holders := queryInt(c, "holders", maxConns)
		duration := queryDuration(c, "duration", 60*time.Second)
		state := ctl.Exhaust(holders, duration)
		srv.Logger().Warn("chaos: db pool exhaustion injected",
			"holders", state.HeldConns, "duration", duration.String())
		c.JSON(http.StatusOK, gin.H{"injected": true, "state": state})
	})
	srv.Router().POST("/chaos/reset", func(c *gin.Context) {
		state := ctl.Reset()
		srv.Logger().Info("chaos: reset")
		c.JSON(http.StatusOK, gin.H{"reset": true, "state": state})
	})
	srv.Router().GET("/chaos/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, ctl.Status())
	})

	if err := srv.Run(); err != nil {
		log.Fatalf("payment-service: %v", err)
	}
}

// publishPoolStats copies pool utilization into Prometheus gauges every second.
func publishPoolStats(srv *service.Server, pool *chaos.Pool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s := pool.Stats()
		srv.Metrics().SetDBPoolStats(s.InUse, s.Max)
	}
}

func envInt(key string, def int) int {
	if v := service.Env(key, ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func queryInt(c *gin.Context, key string, def int) int {
	if v := c.Query(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func queryDuration(c *gin.Context, key string, def time.Duration) time.Duration {
	if v := c.Query(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
