package main

import (
	api "cargo-tracking-ingestion/internal/api/http"
	"cargo-tracking-ingestion/internal/api/http/handler"
	"cargo-tracking-ingestion/internal/api/http/middleware"
	service "cargo-tracking-ingestion/internal/application/telemetry"
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/infrastructure/mqtt"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"cargo-tracking-ingestion/internal/worker"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load cfg: %v", err)
	}

	dbClient, err := timescale.NewClient(&cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbClient.Close()
	log.Println("Connected to TimescaleDB successfully")

	repository := timescale.NewRepository(dbClient)

	batchWriter := worker.NewBatchWriter(repository, &cfg.Worker)
	eventDetector := worker.NewEventDetector(repository, cfg.Worker.EventDetectorWorkers)

	batchWriter.Start()
	eventDetector.Start()
	log.Println("Workers started successfully")

	telemetryService := service.NewService(repository, batchWriter, eventDetector)
	wsHandler := handler.NewWebSocketHandler()
	mqttClient := mqtt.NewClient(&cfg.MQTT)

	mqttClient.SetTelemetryHandler(func(payload []byte, topic string) error {
		t, err := mqtt.ParseTelemetryPayload(payload)
		if err != nil {
			log.Printf("Failed to parse telemetry payload: %v", err)
			return err
		}

		if err := telemetryService.IngestTelemetry(context.Background(), t); err != nil {
			log.Printf("Failed to ingest telemetry: %v", err)
			return err
		}

		wsHandler.Broadcast(t)

		return nil
	})

	mqttClient.SetHeartbeatHandler(func(payload []byte, topic string) error {
		hb, err := mqtt.ParseHeartbeatPayload(payload)
		if err != nil {
			log.Printf("Failed to parse heartbeat payload: %v", err)
			return err
		}

		if err := telemetryService.RecordHeartbeat(context.Background(), hb); err != nil {
			log.Printf("Failed to record heartbeat: %v", err)
			return err
		}

		log.Printf("Heartbeat received from device: %s", hb.DeviceID)
		return nil
	})

	if err := mqttClient.Connect(context.Background()); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	defer mqttClient.Disconnect()
	log.Println("Connected to MQTT broker")

	jwtValidator, err := middleware.NewJwtValidator(
		cfg.JWT.PublicKeyPath,
		cfg.JWT.Issuer,
		cfg.JWT.Audience,
	)
	if err != nil {
		log.Fatalf("Failed to initialize JWT validator: %v", err)
	}

	telemetryHandler := handler.NewTelemetryHandler(telemetryService, repository)
	healthHandler := handler.NewHealthHandler(dbClient, mqttClient)

	router := api.NewRouter(
		cfg,
		telemetryHandler,
		wsHandler,
		healthHandler,
		jwtValidator,
	)
	engine := router.Setup()

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      engine,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	go func() {
		log.Printf("HTTP server listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	batchWriter.Stop()
	eventDetector.Stop()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped gracefully")
}
