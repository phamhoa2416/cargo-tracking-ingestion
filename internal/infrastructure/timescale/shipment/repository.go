package shipment

import (
	"cargo-tracking-ingestion/internal/domain/shipment"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Repository struct {
	client *timescale.Client
}

func NewRepository(client *timescale.Client) *Repository {
	return &Repository{client: client}
}

func (r *Repository) InsertTrackingPoint(ctx context.Context, point *shipment.TrackingPoint) error {
	query := `
		INSERT INTO shipment_tracking (
			time, ingested_at, shipment_id, device_id,
			latitude, longitude, status, eta_minutes,
			distance_to_destination, speed, accuracy
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT (shipment_id, device_id, time) DO NOTHING
	`

	_, err := r.client.Pool().Exec(ctx, query,
		point.Time, point.IngestedAt, point.ShipmentID, point.DeviceID,
		point.Latitude, point.Longitude, point.Status, point.ETAMinutes,
		point.DistanceToDestination, point.Speed, point.Accuracy,
	)

	return err
}

func (r *Repository) GetTrackingHistory(ctx context.Context, params *shipment.TrackingQueryParams) (*shipment.TrackingHistory, error) {
	query := `
		SELECT time, ingested_at, shipment_id, device_id,
		       latitude, longitude, status, eta_minutes,
		       distance_to_destination, speed, accuracy
		FROM shipment_tracking
		WHERE shipment_id = $1
		  AND time >= $2
		  AND time <= $3
		ORDER BY time DESC
		LIMIT $4 OFFSET $5
	`

	rows, err := r.client.Pool().Query(ctx, query,
		params.ShipmentID,
		params.StartTime,
		params.EndTime,
		params.Limit,
		params.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query tracking history failed: %w", err)
	}
	defer rows.Close()

	var points []shipment.TrackingPoint
	for rows.Next() {
		var point shipment.TrackingPoint
		err := rows.Scan(
			&point.Time, &point.IngestedAt, &point.ShipmentID, &point.DeviceID,
			&point.Latitude, &point.Longitude, &point.Status, &point.ETAMinutes,
			&point.DistanceToDestination, &point.Speed, &point.Accuracy,
		)
		if err != nil {
			return nil, fmt.Errorf("scan tracking point failed: %w", err)
		}
		points = append(points, point)
	}

	// Get total count
	countQuery := `
		SELECT COUNT(*)
		FROM shipment_tracking
		WHERE shipment_id = $1
		  AND time >= $2
		  AND time <= $3
	`

	var total int
	err = r.client.Pool().QueryRow(ctx, countQuery,
		params.ShipmentID,
		params.StartTime,
		params.EndTime,
	).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("count tracking history failed: %w", err)
	}

	return &shipment.TrackingHistory{
		ShipmentID: params.ShipmentID,
		Points:     points,
		Total:      total,
	}, nil
}

func (r *Repository) GetLatestTrackingPoint(ctx context.Context, shipmentID uuid.UUID) (*shipment.TrackingPoint, error) {
	query := `
		SELECT time, ingested_at, shipment_id, device_id,
		       latitude, longitude, status, eta_minutes,
		       distance_to_destination, speed, accuracy
		FROM shipment_tracking
		WHERE shipment_id = $1
		ORDER BY time DESC
		LIMIT 1
	`

	var point shipment.TrackingPoint
	err := r.client.Pool().QueryRow(ctx, query, shipmentID).Scan(
		&point.Time, &point.IngestedAt, &point.ShipmentID, &point.DeviceID,
		&point.Latitude, &point.Longitude, &point.Status, &point.ETAMinutes,
		&point.DistanceToDestination, &point.Speed, &point.Accuracy,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest tracking point failed: %w", err)
	}

	return &point, nil
}

func (r *Repository) GetShipmentDevices(ctx context.Context, shipmentID uuid.UUID) ([]uuid.UUID, error) {
	query := `
		SELECT DISTINCT device_id
		FROM shipment_tracking
		WHERE shipment_id = $1
		  AND time >= NOW() - INTERVAL '7 days'
		ORDER BY device_id
	`

	rows, err := r.client.Pool().Query(ctx, query, shipmentID)
	if err != nil {
		return nil, fmt.Errorf("get shipment devices failed: %w", err)
	}
	defer rows.Close()

	var deviceIDs []uuid.UUID
	for rows.Next() {
		var deviceID uuid.UUID
		if err := rows.Scan(&deviceID); err != nil {
			return nil, fmt.Errorf("scan device id failed: %w", err)
		}
		deviceIDs = append(deviceIDs, deviceID)
	}

	return deviceIDs, nil
}
