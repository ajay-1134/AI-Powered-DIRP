// Command notification is the leaf service in the demo chain. It simulates
// sending a user notification (email/SMS) after a ledger entry is written. It
// has no downstream dependencies, so it stays healthy throughout the incident —
// useful as a control: the platform should NOT implicate it in the root cause.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/service"
)

type notifyRequest struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

func main() {
	ctx := context.Background()
	srv, err := service.New(ctx, service.Config{
		Name:         "notification-service",
		Addr:         service.Env("NOTIFICATION_ADDR", ":8085"),
		OTLPEndpoint: service.Env("OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("init notification-service: %v", err)
	}

	srv.Router().POST("/notify", func(c *gin.Context) {
		var req notifyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// Simulate the small, stable cost of dispatching a notification.
		time.Sleep(8 * time.Millisecond)
		c.JSON(http.StatusOK, gin.H{"status": "sent", "order_id": req.OrderID})
	})

	if err := srv.Run(); err != nil {
		log.Fatalf("notification-service: %v", err)
	}
}
