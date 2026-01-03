package main

import (
	api "cargo-tracking-ingestion/internal/api/http"
	"cargo-tracking-ingestion/internal/api/http/handler"
	"cargo-tracking-ingestion/internal/api/http/middleware"
	shipmentService "cargo-tracking-ingestion/internal/application/shipment"
	telemetryService "cargo-tracking-ingestion/internal/application/telemetry"
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/infrastructure/mqtt"
	"cargo-tracking-ingestion/internal/infrastructure/rabbitmq"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	shipmentRepo "cargo-tracking-ingestion/internal/infrastructure/timescale/shipment"
	telemetryRepo "cargo-tracking-ingestion/internal/infrastructure/timescale/telemetry"
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
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	// Initialize TimescaleDB
	dbClient, err := timescale.NewClient(&cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbClient.Close()
	log.Println("Connected to TimescaleDB successfully")

	// Initialize repositories
	telemetryRepository := telemetryRepo.NewRepository(dbClient)
	shipmentRepository := shipmentRepo.NewRepository(dbClient)

	var redisClient *redis.Client
	var redisCache *redis.Cache
	var redisPubsub *redis.PubSub
	if cfg.Redis.Host != "" {
		redisClient, err = redis.NewClient(&cfg.Redis)
		if err != nil {
			log.Printf("WARNING: Failed to connect to Redis: %v. Continuing without Redis...", err)
		} else {
			defer func(redisClient *redis.Client) {
				err := redisClient.Close()
				if err != nil {
					log.Printf("WARNING: Failed to close Redis client: %v", err)
				}
			}(redisClient)
			redisCache = redis.NewCache(redisClient)
			redisPubsub = redis.NewPubSub(redisClient)
			log.Println("Connected to Redis successfully")
		}
	} else {
		log.Println("Redis configuration not provided. Continuing without Redis...")
	}

	var rmqClient *rabbitmq.Client
	var rmqPublisher *rabbitmq.Publisher
	var rmqConsumer *rabbitmq.Consumer
	if cfg.RabbitMQ.URL != "" {
		rmqClient, err = rabbitmq.NewClient(&cfg.RabbitMQ)
		if err != nil {
			log.Printf("WARNING: Failed to connect to RabbitMQ: %v. Continuing without RabbitMQ...", err)
		} else {
			defer func(rmqClient *rabbitmq.Client) {
				err := rmqClient.Close()
				if err != nil {
					log.Printf("WARNING: Failed to close RabbitMQ client: %v", err)
				}
			}(rmqClient)
			rmqPublisher = rabbitmq.NewPublisher(rmqClient, &cfg.RabbitMQ)
			log.Println("Connected to RabbitMQ successfully")

			// Initialize RabbitMQ Consumer if Redis is available
			if redisCache != nil {
				rmqConsumer = rabbitmq.NewConsumer(context.Background(), rmqClient, &cfg.RabbitMQ, redisCache)
				if err := rmqConsumer.Start(); err != nil {
					log.Printf("WARNING: Failed to start RabbitMQ consumer: %v", err)
				} else {
					log.Println("RabbitMQ consumer started successfully")
					defer rmqConsumer.Stop()
				}
			}
		}
	} else {
		log.Println("RabbitMQ not configured, skipping...")
	}

	// Initialize workers
	batchWriter := worker.NewBatchWriter(telemetryRepository, &cfg.Worker)

	eventDetector := worker.NewEventDetector(
		telemetryRepository,
		cfg.Worker.EventDetectorWorkers,
		rmqPublisher, // can be nil
		redisPubsub,  // can be nil
	)

	batchWriter.Start()
	eventDetector.Start()
	log.Println("Workers started successfully")
	defer func() {
		batchWriter.Stop()
		eventDetector.Stop()
	}()

	telemetryService := telemetryService.NewService(
		telemetryRepository,
		batchWriter,
		eventDetector,
		redisCache,   // can be nil
		redisPubsub,  // can be nil
		rmqPublisher, // can be nil
	)

	shipmentService := shipmentService.NewService(
		shipmentRepository,
		redisCache,   // can be nil
		redisPubsub,  // can be nil
		rmqPublisher, // can be nil
	)

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
	log.Println("Connected to MQTT broker successfully")

	jwtValidator, err := middleware.NewJwtValidator(
		cfg.JWT.PublicKeyPath,
		cfg.JWT.Issuer,
		cfg.JWT.Audience,
	)
	if err != nil {
		log.Fatalf("Failed to initialize JWT validator: %v", err)
	}

	telemetryHandler := handler.NewTelemetryHandler(telemetryService, telemetryRepository)
	shipmentHandler := handler.NewShipmentHandler(shipmentService)
	healthHandler := handler.NewHealthHandler(dbClient, mqttClient)

	router := api.NewRouter(
		cfg,
		telemetryHandler,
		shipmentHandler,
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
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	log.Println("Stopping workers...")
	batchWriter.Stop()
	eventDetector.Stop()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped gracefully")
}
