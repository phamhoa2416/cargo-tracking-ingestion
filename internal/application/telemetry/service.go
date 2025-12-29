package telemetry

import (
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"cargo-tracking-ingestion/internal/worker"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo          *timescale.Repository
	batchWriter   *worker.BatchWriter
	eventDetector *worker.EventDetector
}

func NewService(
	repo *timescale.Repository,
	batchWriter *worker.BatchWriter,
	eventDetector *worker.EventDetector,
) *Service {
	return &Service{
		repo:          repo,
		batchWriter:   batchWriter,
		eventDetector: eventDetector,
	}
}

func (s *Service) IngestTelemetry(ctx context.Context, t *telemetry.Telemetry) error {
	if err := ValidateTelemetry(t); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	if t.Time.IsZero() {
		t.Time = time.Now()
	}

	s.batchWriter.Add(t)
	s.eventDetector.Process(t)

	return nil
}

func (s *Service) IngestTelemetryBatch(ctx context.Context, batch *[]telemetry.Telemetry) error {
	if len(*batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	if len(*batch) > 100 {
		return fmt.Errorf("batch size exceeds limit of 100")
	}

	for i := range *batch {
		t := &(*batch)[i]
		if err := ValidateTelemetry(t); err != nil {
			return fmt.Errorf("validation failed at index %d: %w", i, err)
		}

		if t.Time.IsZero() {
			t.Time = time.Now()
		}

		s.batchWriter.Add(t)
		s.eventDetector.Process(t)
	}

	return nil
}

func (s *Service) GetLatestLocation(ctx context.Context, deviceId uuid.UUID) (*telemetry.Location, error) {
	return s.repo.GetLatestLocation(ctx, deviceId)
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

	return s.repo.RecordHeartbeat(ctx, hb)
}
