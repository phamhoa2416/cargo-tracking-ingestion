package handler

import (
	"context"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

type HealthHandler struct {
	dbClient   HealthChecker
	mqttClient interface{ IsConnected() bool }
}

func NewHealthHandler(
	dbClient HealthChecker,
	mqttClient interface{ IsConnected() bool },
) *HealthHandler {
	return &HealthHandler{
		dbClient:   dbClient,
		mqttClient: mqttClient,
	}
}

func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "cargo-tracking-ingestion",
	})
}

func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	checks := make(map[string]string)

	if err := h.dbClient.HealthCheck(ctx); err != nil {
		checks["database"] = "unhealthy: " + err.Error()
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"checks": checks,
		})
		return
	}

	checks["database"] = "healthy"

	if !h.mqttClient.IsConnected() {
		checks["mqtt"] = "unhealthy: not connected"
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"checks": checks,
		})
		return
	}

	checks["mqtt"] = "healthy"

	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
		"checks": checks,
	})
}
