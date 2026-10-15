// Command ledger records the accounting entry for a successful payment and then
// triggers a user notification. It sits downstream of Payment and upstream of
// Notification. It stays healthy during the injected incident — the failure
// originates in Payment and never reaches here, which is exactly the kind of
// boundary the platform must respect when assigning blame.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/service"
)

type ledgerRequest struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	PaymentID string  `json:"payment_id"`
}

func main() {
	ctx := context.Background()
	srv, err := service.New(ctx, service.Config{
		Name:         "ledger-service",
		Addr:         service.Env("LEDGER_ADDR", ":8084"),
		OTLPEndpoint: service.Env("OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("init ledger-service: %v", err)
	}

	notification := service.NewClient("notification-service",
		service.Env("NOTIFICATION_URL", "http://localhost:8085"))

	srv.Router().POST("/entries", func(c *gin.Context) {
		var req ledgerRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// Simulate writing a double-entry record.
		time.Sleep(6 * time.Millisecond)

		// Fire the downstream notification within the same trace.
		var notifyResp map[string]any
		if err := notification.PostJSON(c.Request.Context(), "/notify", gin.H{
			"order_id": req.OrderID,
			"user_id":  req.UserID,
			"amount":   req.Amount,
		}, &notifyResp); err != nil {
			// A notification failure is non-fatal to the ledger write, but we
			// surface it so it shows up in traces as a downstream error.
			c.JSON(http.StatusOK, gin.H{
				"status":   "recorded",
				"order_id": req.OrderID,
				"notified": false,
				"warning":  err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":   "recorded",
			"order_id": req.OrderID,
			"notified": true,
		})
	})

	if err := srv.Run(); err != nil {
		log.Fatalf("ledger-service: %v", err)
	}
}
