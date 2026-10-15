// Command order creates an order and charges it via the Payment service. It is
// the service whose SLOs visibly break during the incident: when Payment stalls
// on its exhausted pool, Order's calls exceed the client timeout and it starts
// returning 5xx. The platform will first SEE the problem here (Order error rate
// 1% -> 35%) but must trace it back to Payment as the true cause.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ajay-1134/dirp/internal/service"
)

type createOrderRequest struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

func main() {
	ctx := context.Background()
	srv, err := service.New(ctx, service.Config{
		Name:         "order-service",
		Addr:         service.Env("ORDER_ADDR", ":8082"),
		OTLPEndpoint: service.Env("OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("init order-service: %v", err)
	}

	payment := service.NewClient("payment-service",
		service.Env("PAYMENT_URL", "http://localhost:8083"))

	srv.Router().POST("/orders", func(c *gin.Context) {
		var req createOrderRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		orderID := "ord_" + time.Now().Format("150405.000000")

		var payResp map[string]any
		if err := payment.PostJSON(c.Request.Context(), "/charges", gin.H{
			"order_id": orderID,
			"user_id":  req.UserID,
			"amount":   req.Amount,
		}, &payResp); err != nil {
			// Payment stalled or failed: this is the upstream-visible failure.
			c.JSON(http.StatusBadGateway, gin.H{
				"error":    "payment failed: " + err.Error(),
				"order_id": orderID,
			})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"status":   "created",
			"order_id": orderID,
			"payment":  payResp,
		})
	})

	if err := srv.Run(); err != nil {
		log.Fatalf("order-service: %v", err)
	}
}
