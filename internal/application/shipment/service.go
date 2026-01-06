package shipment

import (
	"cargo-tracking-ingestion/internal/domain/shipment"
	"cargo-tracking-ingestion/internal/infrastructure/rabbitmq"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	repo "cargo-tracking-ingestion/internal/infrastructure/timescale/shipment"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo      *repo.Repository
	cache     *redis.Cache
	pubsub    *redis.PubSub
	publisher *rabbitmq.Publisher
}

func NewService(
	repo *repo.Repository,
	cache *redis.Cache,
	pubsub *redis.PubSub,
	publisher *rabbitmq.Publisher,
) *Service {
	return &Service{
		repo:      repo,
		cache:     cache,
		pubsub:    pubsub,
		publisher: publisher,
	}
}

func (s *Service) CreateTrackingPoint(ctx context.Context, point *shipment.TrackingPoint) error {
	if point.IngestedAt.IsZero() {
		point.IngestedAt = time.Now()
	}

	if err := ValidateTrackingPoint(point); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Insert to database
	if err := s.repo.InsertTrackingPoint(ctx, point); err != nil {
		return err
	}

	// Update cache and publish updates asynchronously
	go s.updateCacheAndPublish(ctx, point)

	return nil
}

func (s *Service) updateCacheAndPublish(ctx context.Context, point *shipment.TrackingPoint) {
	// Update cache if location is available
	if point.HasLocation() && s.cache != nil {
		location := &redis.DeviceLocation{
			DeviceID:  point.DeviceID,
			Latitude:  *point.Latitude,
			Longitude: *point.Longitude,
			Speed:     point.Speed,
			Accuracy:  point.Accuracy,
			Timestamp: point.Time,
		}
		if err := s.cache.SetDeviceLocation(ctx, location); err != nil {
			log.Printf("Failed to cache device location: %v", err)
		}
	}

	// Publish shipment update to Redis PubSub
	if s.pubsub != nil {
		update := &redis.ShipmentUpdate{
			ShipmentID: point.ShipmentID,
			Status:     "",
			Timestamp:  point.Time.Unix(),
		}
		if point.Status != nil {
			update.Status = *point.Status
		}
		if point.HasLocation() {
			update.Location = &struct {
				Latitude  float64 `json:"latitude"`
				Longitude float64 `json:"longitude"`
			}{
				Latitude:  *point.Latitude,
				Longitude: *point.Longitude,
			}
		}
		update.ETAMinutes = point.ETAMinutes

		if err := s.pubsub.PublishShipmentUpdate(ctx, update); err != nil {
			log.Printf("Failed to publish shipment update: %v", err)
		}
	}

	// Publish shipment update to RabbitMQ
	if s.publisher != nil {
		rmqUpdate := &rabbitmq.ShipmentUpdateMessage{
			ShipmentID:            point.ShipmentID,
			DeviceID:              point.DeviceID,
			Status:                "",
			ETAMinutes:            point.ETAMinutes,
			DistanceToDestination: point.DistanceToDestination,
		}
		if point.Status != nil {
			rmqUpdate.Status = *point.Status
		}
		if point.HasLocation() {
			rmqUpdate.Location = &rabbitmq.Location{
				Latitude:  *point.Latitude,
				Longitude: *point.Longitude,
				Accuracy:  point.Accuracy,
				Timestamp: point.Time,
			}
		}

		if err := s.publisher.PublishShipmentUpdate(ctx, rmqUpdate); err != nil {
			log.Printf("Failed to publish shipment update to RabbitMQ: %v", err)
		}
	}
}

func (s *Service) GetTrackingHistory(ctx context.Context, params *shipment.TrackingQueryParams) (*shipment.TrackingHistory, error) {
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

	return s.repo.GetTrackingHistory(ctx, params)
}

func (s *Service) GetLiveTracking(ctx context.Context, shipmentID uuid.UUID) (*shipment.LiveTracking, error) {
	latestPoint, err := s.repo.GetLatestTrackingPoint(ctx, shipmentID)
	if err != nil {
		return nil, err
	}

	// Get device IDs from cache or database
	var deviceIDs []uuid.UUID
	if s.cache != nil {
		deviceIDs, err = s.cache.GetShipmentDevices(ctx, shipmentID)
		if err != nil {
			deviceIDs, err = s.repo.GetShipmentDevices(ctx, shipmentID)
			if err != nil {
				return nil, err
			}
		}
	} else {
		deviceIDs, err = s.repo.GetShipmentDevices(ctx, shipmentID)
		if err != nil {
			return nil, err
		}
	}

	// Get device statuses from cache
	activeDevices := make([]shipment.DeviceStatus, 0, len(deviceIDs))
	if s.cache != nil {
		for _, deviceID := range deviceIDs {
			status, err := s.cache.GetDeviceStatus(ctx, deviceID)
			if err != nil || status == nil {
				continue
			}

			activeDevices = append(activeDevices, shipment.DeviceStatus{
				DeviceID:       deviceID,
				IsOnline:       status.IsOnline,
				LastSeen:       status.LastHeartbeat,
				BatteryLevel:   status.BatteryLevel,
				SignalStrength: status.SignalStrength,
			})
		}
	}

	liveTracking := &shipment.LiveTracking{
		ShipmentID:    shipmentID,
		ActiveDevices: activeDevices,
		LastUpdate:    time.Now(),
		Status:        "unknown",
	}

	if latestPoint != nil {
		if latestPoint.HasLocation() {
			liveTracking.CurrentLocation = &shipment.Location{
				Latitude:  *latestPoint.Latitude,
				Longitude: *latestPoint.Longitude,
				Timestamp: latestPoint.Time,
			}
		}

		if latestPoint.Status != nil {
			liveTracking.Status = *latestPoint.Status
		}
		liveTracking.ETAMinutes = latestPoint.ETAMinutes
		liveTracking.DistanceToDestination = latestPoint.DistanceToDestination
		liveTracking.LastUpdate = latestPoint.Time
	}

	return liveTracking, nil
}
