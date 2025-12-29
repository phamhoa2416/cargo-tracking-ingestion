package http

import (
	"cargo-tracking-ingestion/internal/api/http/handler"
	"cargo-tracking-ingestion/internal/config"

	"github.com/gin-gonic/gin"
)

type Router struct {
	engine           *gin.Engine
	telemetryHandler *handler.TelemetryHandler
	websocketHandler *handler.WebSocketHandler
}

func NewRouter(
	config *config.Config,
	telemetryHandler *handler.TelemetryHandler,
	websocketHandler *handler.WebSocketHandler,
) *Router {
	if config.Server.Host == "0.0.0.0" {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	return &Router{
		engine:           engine,
		telemetryHandler: telemetryHandler,
		websocketHandler: websocketHandler,
	}
}

func (r *Router) Setup() *gin.Engine {
	ws := r.engine.Group("/ws")
	ws.GET("/telemetry", r.websocketHandler.HandleWebSocket)

	v1 := r.engine.Group("/api/v1")
	v1.POST("/telemetry", r.telemetryHandler.IngestTelemetry)
	v1.POST("/telemetry/batch", r.telemetryHandler.IngestBatch)
	v1.POST("/heartbeat", r.telemetryHandler.Heartbeat)

	devices := v1.Group("/devices")
	{
		devices.GET("/:id/location/latest", r.telemetryHandler.GetLatestLocation)
		devices.GET("/:id/location/history", r.telemetryHandler.GetLocationHistory)
		devices.GET("/:id/events", r.telemetryHandler.GetDeviceEvents)
	}

	shipments := v1.Group("/shipments")
	{
		shipments.GET("/:id/tracking", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "Not implemented yet"})
		})
		shipments.GET("/:id/tracking/live", func(c *gin.Context) {
			c.JSON(200, gin.H{"message": "Not implemented yet"})
		})
	}

	return r.engine
}

func (r *Router) Engine() *gin.Engine {
	return r.engine
}
