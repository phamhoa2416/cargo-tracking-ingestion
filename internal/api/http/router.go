package http

import (
	"cargo-tracking-ingestion/internal/api/http/handler"
	"cargo-tracking-ingestion/internal/api/http/middleware"
	"cargo-tracking-ingestion/internal/config"
	"github.com/gin-gonic/gin"
)

type Router struct {
	engine           *gin.Engine
	telemetryHandler *handler.TelemetryHandler
	shipmentHandler  *handler.ShipmentHandler
	websocketHandler *handler.WebSocketHub
	healthHandler    *handler.HealthHandler
	jwtValidator     *middleware.JwtValidator
	rateLimiter      *middleware.RateLimiter
}

func NewRouter(
	cfg *config.Config,
	telemetryHandler *handler.TelemetryHandler,
	shipmentHandler *handler.ShipmentHandler,
	websocketHandler *handler.WebSocketHub,
	healthHandler *handler.HealthHandler,
	jwtValidator *middleware.JwtValidator,
) *Router {
	if cfg.Server.Host == "0.0.0.0" || cfg.Server.Host == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()

	// Global middlewares - order matters
	engine.Use(gin.Recovery())
	engine.Use(middleware.CORS("*"))
	engine.Use(middleware.Logger())

	rateLimiter := middleware.NewRateLimiter(
		float64(cfg.RateLimit.RequestsPerSecond),
		cfg.RateLimit.Burst,
	)

	return &Router{
		engine:           engine,
		telemetryHandler: telemetryHandler,
		shipmentHandler:  shipmentHandler,
		websocketHandler: websocketHandler,
		healthHandler:    healthHandler,
		jwtValidator:     jwtValidator,
		rateLimiter:      rateLimiter,
	}
}

func (r *Router) Setup() *gin.Engine {
	r.engine.GET("/health", r.healthHandler.Health)
	r.engine.GET("/ready", r.healthHandler.Ready)

	// WebSocket endpoint (requires JWT)
	ws := r.engine.Group("/ws")
	ws.Use(r.jwtValidator.Validate())
	{
		ws.GET("/telemetry", r.websocketHandler.HandleWebSocket)
	}

	// API v1 endpoints (requires JWT and rate limiting)
	v1 := r.engine.Group("/api/v1")
	v1.Use(r.jwtValidator.Validate())
	v1.Use(r.rateLimiter.Limit())
	{
		// Telemetry ingestion endpoints
		v1.POST("/telemetry", r.telemetryHandler.IngestTelemetry)
		v1.POST("/telemetry/batch", r.telemetryHandler.IngestBatch)
		v1.POST("/heartbeat", r.telemetryHandler.Heartbeat)

		// Device endpoints
		devices := v1.Group("/devices")
		{
			devices.GET("/:id/telemetry/latest", r.telemetryHandler.GetLatestTelemetry)
			devices.GET("/:id/location/latest", r.telemetryHandler.GetLatestLocation)
			devices.GET("/:id/location/history", r.telemetryHandler.GetLocationHistory)
			devices.GET("/:id/events", r.telemetryHandler.GetDeviceEvents)
		}

		// Shipment endpoints
		shipments := v1.Group("/shipments")
		{
			shipments.POST("/tracking", r.shipmentHandler.CreateTrackingPoint)
			shipments.GET("/:id/tracking", r.shipmentHandler.GetTrackingHistory)
			shipments.GET("/:id/tracking/live", r.shipmentHandler.GetLiveTracking)
		}
	}

	return r.engine
}

func (r *Router) Engine() *gin.Engine {
	return r.engine
}
