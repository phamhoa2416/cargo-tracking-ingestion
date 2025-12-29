package http

import (
	"github.com/gin-gonic/gin"

	"cargo-tracking-ingestion/internal/api/http/handler"
	"cargo-tracking-ingestion/internal/api/http/middleware"
	"cargo-tracking-ingestion/internal/config"
)

type Router struct {
	engine           *gin.Engine
	telemetryHandler *handler.TelemetryHandler
	websocketHandler *handler.WebSocketHandler
	healthHandler    *handler.HealthHandler
	jwtValidator     *middleware.JwtValidator
	rateLimiter      *middleware.RateLimiter
}

func NewRouter(
	cfg *config.Config,
	telemetryHandler *handler.TelemetryHandler,
	websocketHandler *handler.WebSocketHandler,
	healthHandler *handler.HealthHandler,
	jwtValidator *middleware.JwtValidator,
) *Router {
	if cfg.Server.Host == "0.0.0.0" {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.Logger())
	engine.Use(middleware.CORS())

	rateLimiter := middleware.NewRateLimiter(
		cfg.RateLimit.RequestsPerSecond,
		cfg.RateLimit.Burst,
	)

	return &Router{
		engine:           engine,
		telemetryHandler: telemetryHandler,
		websocketHandler: websocketHandler,
		healthHandler:    healthHandler,
		jwtValidator:     jwtValidator,
		rateLimiter:      rateLimiter,
	}
}

func (r *Router) Setup() *gin.Engine {
	r.engine.GET("/health", r.healthHandler.Health)
	r.engine.GET("/ready", r.healthHandler.Ready)

	ws := r.engine.Group("/ws")
	ws.Use(r.jwtValidator.Validate())
	{
		ws.GET("/telemetry", r.websocketHandler.HandleWebSocket)
	}

	v1 := r.engine.Group("/api/v1")
	v1.Use(r.jwtValidator.Validate())
	v1.Use(r.rateLimiter.Limit())
	{
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
	}

	return r.engine
}

func (r *Router) Engine() *gin.Engine {
	return r.engine
}
