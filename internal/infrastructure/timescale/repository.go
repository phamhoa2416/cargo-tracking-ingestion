package timescale

import (
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Repository struct {
	client *Client
}

func NewRepository(client *Client) *Repository {
	return &Repository{client: client}
}

func (r *Repository) InsertTelemetry(ctx context.Context, t *telemetry.Telemetry) error {
	query := `
		INSERT INTO device_telemetry (
			time, device_id, hardware_uid,
			temperature, humidity, pressure,
			latitude, longitude, altitude, speed, heading, accuracy,
			battery_level, signal_strength,
			is_moving, event_type, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
	`

	_, err := r.client.Pool().Exec(ctx, query,
		t.Time, t.DeviceID, t.HardwareUID,
		t.Temperature, t.Humidity, t.Pressure,
		t.Latitude, t.Longitude, t.Altitude, t.Speed, t.Heading, t.Accuracy,
		t.BatteryLevel, t.SignalStrength,
		t.IsMoving, t.EventType, t.RawPayload,
	)

	return err
}

func (r *Repository) BatchInsertTelemetry(ctx context.Context, telemetries []*telemetry.Telemetry) error {
	if len(telemetries) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO device_telemetry (
			time, device_id, hardware_uid,
			temperature, humidity, pressure,
			latitude, longitude, altitude, speed, heading, accuracy,
			battery_level, signal_strength,
			is_moving, event_type, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
	`
	for _, t := range telemetries {
		batch.Queue(query,
			t.Time, t.DeviceID, t.HardwareUID,
			t.Temperature, t.Humidity, t.Pressure,
			t.Latitude, t.Longitude, t.Altitude, t.Speed, t.Heading, t.Accuracy,
			t.BatteryLevel, t.SignalStrength,
			t.IsMoving, t.EventType, t.RawPayload,
		)
	}

	br := r.client.Pool().SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(telemetries); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("batch insert failed at index %d: %w", i, err)
		}
	}

	return nil
}

func (r *Repository) InsertEvent(ctx context.Context, e *event.Event) error {
	query := `
		INSERT INTO device_events (
			time, ingested_at, device_id, hardware_uid,
			event_type, severity, description, metadata,
			acknowledged, acknowledged_at, acknowledged_by
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
	`

	_, err := r.client.Pool().Exec(ctx, query,
		e.Time, e.IngestedAt, e.DeviceID, e.HardwareUID,
		e.EventType, e.Severity, e.Description, e.Metadata,
		e.Acknowledged, e.AcknowledgedAt, e.AcknowledgedBy,
	)

	return err
}

func (r *Repository) BatchInsertEvents(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO device_events (
			time, ingested_at, device_id, hardware_uid,
			event_type, severity, description, metadata,
			acknowledged, acknowledged_at, acknowledged_by
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
	`

	for _, e := range events {
		batch.Queue(query,
			e.Time, e.IngestedAt, e.DeviceID, e.HardwareUID,
			e.EventType, e.Severity, e.Description, e.Metadata,
			e.Acknowledged, e.AcknowledgedAt, e.AcknowledgedBy,
		)
	}

	br := r.client.Pool().SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(events); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("batch insert events failed at index %d: %w", i, err)
		}
	}

	return nil
}

func (r *Repository) GetLatestLocation(ctx context.Context, deviceId uuid.UUID) (*telemetry.Location, error) {
	query := `
		SELECT device_id, time, latitude, longitude, altitude, speed, heading, accuracy
		FROM device_telemetry
		WHERE device_id = $1
		  AND latitude IS NOT NULL
		  AND longitude IS NOT NULL
		ORDER BY time DESC
		LIMIT 1
	`

	var location telemetry.Location
	err := r.client.Pool().QueryRow(ctx, query, deviceId).Scan(
		&location.DeviceID, &location.Time, &location.Latitude, &location.Longitude, &location.Altitude,
		&location.Speed, &location.Heading, &location.Accuracy,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &location, nil
}

func (r *Repository) GetLocationHistory(ctx context.Context, params *telemetry.LocationQueryParams) (*telemetry.LocationHistory, error) {
	query := `
		SELECT device_id, time, latitude, longitude, altitude, speed, heading, accuracy
		FROM device_telemetry
		WHERE device_id = $1
		  AND latitude IS NOT NULL
		  AND longitude IS NOT NULL
		  AND time >= $2
		  AND time <= $3
		ORDER BY time DESC
		LIMIT $4 OFFSET $5
	`

	rows, err := r.client.Pool().Query(ctx, query,
		params.DeviceID,
		params.StartTime,
		params.EndTime,
		params.Limit,
		params.Offset,
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []telemetry.Location
	for rows.Next() {
		var location telemetry.Location
		err := rows.Scan(
			&location.DeviceID, &location.Time, &location.Latitude, &location.Longitude,
			&location.Altitude, &location.Speed, &location.Heading, &location.Accuracy,
		)
		if err != nil {
			return nil, err
		}

		locations = append(locations, location)
	}

	countQuery := `
		SELECT COUNT(*)
		FROM device_telemetry
		WHERE device_id = $1
		  AND latitude IS NOT NULL
		  AND longitude IS NOT NULL
		  AND time >= $2
		  AND time <= $3
	`

	var total int
	err = r.client.Pool().QueryRow(ctx, countQuery,
		params.DeviceID,
		params.StartTime,
		params.EndTime,
	).Scan(&total)
	if err != nil {
		return nil, err
	}

	return &telemetry.LocationHistory{
		DeviceID:  params.DeviceID,
		Locations: locations,
		Total:     total,
	}, nil
}

func (r *Repository) RecordHeartbeat(ctx context.Context, h *telemetry.Heartbeat) error {
	query := `
		INSERT INTO device_heartbeats (
			device_id, hardware_uid, timestamp,
			battery_level, signal_strength, firmware_version
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
	`

	_, err := r.client.Pool().Exec(ctx, query,
		h.DeviceID, h.HardwareUID, h.Timestamp,
		h.BatteryLevel, h.SignalStrength, h.FirmwareVersion,
	)

	return err
}

func (r *Repository) GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error) {
	whereClause := "WHERE device_id = $1 AND time >= $2 AND time <= $3"
	args := []interface{}{params.DeviceID, params.StartTime, params.EndTime}
	argIdx := 4

	if params.EventType != nil {
		whereClause += fmt.Sprintf(" AND event_type = $%d", argIdx)
		args = append(args, *params.EventType)
		argIdx++
	}

	if params.Severity != nil {
		whereClause += fmt.Sprintf(" AND severity = $%d", argIdx)
		args = append(args, *params.Severity)
		argIdx++
	}

	if params.Acknowledged != nil {
		whereClause += fmt.Sprintf(" AND acknowledged = $%d", argIdx)
		args = append(args, *params.Acknowledged)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT time, ingested_at, device_id, hardware_uid,
		       event_type, severity, description, metadata,
		       acknowledged, acknowledged_at, acknowledged_by
		FROM device_events
		%s
		ORDER BY time DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, params.Limit, params.Offset)

	rows, err := r.client.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []event.Event
	for rows.Next() {
		var e event.Event
		err := rows.Scan(
			&e.Time, &e.IngestedAt, &e.DeviceID, &e.HardwareUID,
			&e.EventType, &e.Severity, &e.Description, &e.Metadata,
			&e.Acknowledged, &e.AcknowledgedAt, &e.AcknowledgedBy,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	countQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM device_events
		%s
	`, whereClause)

	var total int
	err = r.client.Pool().QueryRow(ctx, countQuery, args[:len(args)-2]...).Scan(&total)
	if err != nil {
		return nil, err
	}

	return &event.List{
		Events: events,
		Total:  total,
	}, nil
}
