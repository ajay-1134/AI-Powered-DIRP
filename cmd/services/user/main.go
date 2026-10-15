// Command user is the public entry point of the demo chain. It exposes a
// /checkout endpoint that starts a request flowing through Order -> Payment ->
// Ledger -> Notification. It also runs an optional built-in load generator so
// the system carries steady baseline traffic — without a baseline, there is no
// "1% error rate" for the incident to perturb into "35%".
package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/service"
)

type checkoutRequest struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

func main() {
	ctx := context.Background()
	srv, err := service.New(ctx, service.Config{
		Name:         "user-service",
		Addr:         service.Env("USER_ADDR", ":8081"),
		OTLPEndpoint: service.Env("OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("init user-service: %v", err)
	}

	order := service.NewClient("order-service",
		service.Env("ORDER_URL", "http://localhost:8082"))

	checkout := func(c *gin.Context) {
		var req checkoutRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		var orderResp map[string]any
		if err := order.PostJSON(c.Request.Context(), "/orders", gin.H{
			"user_id": req.UserID,
			"amount":  req.Amount,
		}, &orderResp); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "checkout failed: " + err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "order": orderResp})
	}
	srv.Router().POST("/checkout", checkout)

	// Built-in load generator, enabled with LOAD_RPS > 0. It calls the full
	// chain in-process (reusing the same traced client) so the whole system has
	// continuous, traced baseline traffic during a demo.
	if rps := envInt("LOAD_RPS", 0); rps > 0 {
		go generateLoad(order, rps)
		srv.Logger().Info("load generator started", "rps", rps)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("user-service: %v", err)
	}
}

// generateLoad drives approximately rps checkouts per second against Order,
// each on its own trace, until the process exits.
func generateLoad(order *service.Client, rps int) {
	interval := time.Second / time.Duration(rps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(1)) // deterministic amounts; time not needed
	for range ticker.C {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var resp map[string]any
			_ = order.PostJSON(ctx, "/orders", gin.H{
				"user_id": "u_load",
				"amount":  float64(rng.Intn(9000)+100) / 100,
			}, &resp)
		}()
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
