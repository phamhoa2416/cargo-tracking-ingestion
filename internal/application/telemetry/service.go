package telemetry

import (
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/rabbitmq"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	repo "cargo-tracking-ingestion/internal/infrastructure/timescale/telemetry"
	"cargo-tracking-ingestion/internal/worker"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo          *repo.Repository
	batchWriter   *worker.BatchWriter
	eventDetector *worker.EventDetector
	cache         *redis.Cache
	pubsub        *redis.PubSub
	publisher     *rabbitmq.Publisher
}

func NewService(
	repo *repo.Repository,
	batchWriter *worker.BatchWriter,
	eventDetector *worker.EventDetector,
	cache *redis.Cache,
	pubsub *redis.PubSub,
	publisher *rabbitmq.Publisher,
) *Service {
	return &Service{
		repo:          repo,
		batchWriter:   batchWriter,
		eventDetector: eventDetector,
		cache:         cache,
		pubsub:        pubsub,
		publisher:     publisher,
	}
}

func (s *Service) IngestTelemetry(ctx context.Context, t *telemetry.Telemetry) error {
	if t.Time.IsZero() {
		t.Time = time.Now()
	}

	if err := ValidateTelemetry(t); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	s.batchWriter.Add(t)
	s.eventDetector.Process(t)

	go s.updateCacheAndPublish(ctx, t)

	return nil
}

func (s *Service) updateCacheAndPublish(ctx context.Context, t *telemetry.Telemetry) {
	if t.Latitude != nil && t.Longitude != nil {
		if s.cache != nil {
			location := &redis.DeviceLocation{
				DeviceID:  t.DeviceID,
				Latitude:  *t.Latitude,
				Longitude: *t.Longitude,
				Speed:     t.Speed,
				Accuracy:  t.Accuracy,
				Timestamp: t.Time,
			}
			if err := s.cache.SetDeviceLocation(ctx, location); err != nil {
				log.Printf("Failed to cache device location: %v", err)
			}
		}

		if s.publisher != nil {
			rmqLocation := &rabbitmq.Location{
				Latitude:  *t.Latitude,
				Longitude: *t.Longitude,
				Accuracy:  t.Accuracy,
				Timestamp: t.Time,
			}
			if err := s.publisher.PublishDeviceLocation(ctx, t.DeviceID, rmqLocation); err != nil {
				log.Printf("Failed to publish device location: %v", err)
			}
		}
	}

	if s.cache != nil {
		status := &redis.DeviceStatus{
			DeviceID:       t.DeviceID,
			IsOnline:       true,
			BatteryLevel:   t.BatteryLevel,
			SignalStrength: t.SignalStrength,
			IsMoving:       t.IsMoving,
			LastHeartbeat:  t.Time,
		}
		if err := s.cache.SetDeviceStatus(ctx, status); err != nil {
			log.Printf("Failed to cache device status: %v", err)
		}
	}

	if s.pubsub != nil {
		update := &redis.TelemetryUpdate{
			DeviceID:    t.DeviceID,
			Timestamp:   t.Time.Unix(),
			HasLocation: t.Latitude != nil && t.Longitude != nil,
			Data:        make(map[string]interface{}),
		}
		if t.Temperature != nil {
			update.Data["temperature"] = *t.Temperature
		}
		if t.Humidity != nil {
			update.Data["humidity"] = *t.Humidity
		}
		if t.CO2 != nil {
			update.Data["co2"] = *t.CO2
		}
		if t.Light != nil {
			update.Data["light"] = *t.Light
		}
		if t.Lean != nil {
			update.Data["lean"] = *t.Lean
		}
		if t.BatteryLevel != nil {
			update.Data["battery_level"] = *t.BatteryLevel
		}
		if t.SignalStrength != nil {
			update.Data["signal_strength"] = *t.SignalStrength
		}

		if err := s.pubsub.PublishTelemetryUpdate(ctx, update); err != nil {
			log.Printf("Failed to publish telemetry update: %v", err)
		}
	}

	if s.publisher != nil && (t.BatteryLevel != nil || t.SignalStrength != nil) {
		if err := s.publisher.PublishDeviceHeartbeat(ctx, t.DeviceID, t.BatteryLevel, t.SignalStrength, t.Time); err != nil {
			log.Printf("Failed to publish device heartbeat: %v", err)
		}
	}
}

func (s *Service) IngestTelemetryBatch(ctx context.Context, batch []telemetry.Telemetry) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	if len(batch) > 100 {
		return fmt.Errorf("batch size exceeds limit of 100")
	}

	for i := range batch {
		t := &(batch)[i]
		if t.Time.IsZero() {
			t.Time = time.Now()
		}

		if err := ValidateTelemetry(t); err != nil {
			return fmt.Errorf("validation failed at index %d: %w", i, err)
		}

		s.batchWriter.Add(t)
		s.eventDetector.Process(t)
	}

	return nil
}

func (s *Service) GetLatestTelemetry(ctx context.Context, deviceId uuid.UUID) (*telemetry.Telemetry, error) {
	// Query database for latest telemetry
	t, err := s.repo.GetLatestTelemetry(ctx, deviceId)
	if err != nil {
		return nil, err
	}

	// If we have cached status and telemetry is missing some fields, enrich it
	if t != nil && s.cache != nil {
		cachedStatus, err := s.cache.GetDeviceStatus(ctx, deviceId)
		if err == nil && cachedStatus != nil {
			// Only fill in missing fields from cache
			if t.BatteryLevel == nil && cachedStatus.BatteryLevel != nil {
				t.BatteryLevel = cachedStatus.BatteryLevel
			}
			if t.SignalStrength == nil && cachedStatus.SignalStrength != nil {
				t.SignalStrength = cachedStatus.SignalStrength
			}
			if t.IsMoving == nil && cachedStatus.IsMoving != nil {
				t.IsMoving = cachedStatus.IsMoving
			}
		}
	}

	return t, nil
}

func (s *Service) GetLatestLocation(ctx context.Context, deviceId uuid.UUID) (*telemetry.Location, error) {
	// Try cache first (read-through cache)
	if s.cache != nil {
		cached, err := s.cache.GetDeviceLocation(ctx, deviceId)
		if err == nil && cached != nil {
			return &telemetry.Location{
				DeviceID:  cached.DeviceID,
				Time:      cached.Timestamp,
				Latitude:  cached.Latitude,
				Longitude: cached.Longitude,
				Speed:     cached.Speed,
				Accuracy:  cached.Accuracy,
			}, nil
		}
	}

	// Fallback to database
	location, err := s.repo.GetLatestLocation(ctx, deviceId)
	if err != nil {
		return nil, err
	}

	// Cache the result if available
	if location != nil && s.cache != nil {
		cached := &redis.DeviceLocation{
			DeviceID:  location.DeviceID,
			Latitude:  location.Latitude,
			Longitude: location.Longitude,
			Speed:     location.Speed,
			Accuracy:  location.Accuracy,
			Timestamp: location.Time,
		}
		_ = s.cache.SetDeviceLocation(ctx, cached)
	}

	return location, nil
}

func (s *Service) GetLocationHistory(ctx context.Context, params *telemetry.LocationQueryParams) (*telemetry.LocationHistory, error) {
	if params.StartTime.IsZero() {
		params.StartTime = time.Now().Add(-24 * time.Hour)
	}
	if params.EndTime.IsZero() {
		params.EndTime = time.Now()
	}
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.Limit > 1000 {
		params.Limit = 1000
	}

	return s.repo.GetLocationHistory(ctx, params)
}

func (s *Service) RecordHeartbeat(ctx context.Context, hb *telemetry.Heartbeat) error {
	if err := ValidateHeartbeat(hb); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if err := s.repo.RecordHeartbeat(ctx, hb); err != nil {
		return err
	}

	go s.updateHeartbeatCacheAndPublish(ctx, hb)

	return nil
}

func (s *Service) updateHeartbeatCacheAndPublish(ctx context.Context, hb *telemetry.Heartbeat) {
	if s.cache != nil {
		status := &redis.DeviceStatus{
			DeviceID:       hb.DeviceID,
			IsOnline:       true,
			BatteryLevel:   hb.BatteryLevel,
			SignalStrength: hb.SignalStrength,
			LastHeartbeat:  hb.Timestamp,
		}
		if err := s.cache.SetDeviceStatus(ctx, status); err != nil {
			log.Printf("Failed to cache device status from heartbeat: %v", err)
		}
	}

	if s.pubsub != nil {
		update := &redis.DeviceStatusUpdate{
			DeviceID: hb.DeviceID,
			IsOnline: true,
			LastSeen: hb.Timestamp.Unix(),
		}
		if err := s.pubsub.PublishDeviceStatusUpdate(ctx, update); err != nil {
			log.Printf("Failed to publish device status update: %v", err)
		}
	}

	// Publish heartbeat to RabbitMQ
	if s.publisher != nil {
		if err := s.publisher.PublishDeviceHeartbeat(ctx, hb.DeviceID, hb.BatteryLevel, hb.SignalStrength, hb.Timestamp); err != nil {
			log.Printf("Failed to publish device heartbeat: %v", err)
		}
	}
}
