package telemetry

import (
	"cargo-tracking-ingestion/internal/domain/event"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"cargo-tracking-ingestion/internal/infrastructure/timescale"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Repository struct {
	client *timescale.Client
}

func NewRepository(client *timescale.Client) *Repository {
	return &Repository{client: client}
}

func (r *Repository) InsertTelemetry(ctx context.Context, t *telemetry.Telemetry) error {
	query := `
		INSERT INTO device_telemetry (
			time, device_id, hardware_uid,
			temperature, humidity, co2, light,
			latitude, longitude, speed, accuracy, lean,
			battery_level, signal_strength,
			is_moving, event_type, raw_payload
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (device_id, hardware_uid, time) DO NOTHING
		`

	_, err := r.client.Pool().Exec(ctx, query,
		t.Time, t.DeviceID, t.HardwareUID,
		t.Temperature, t.Humidity, t.CO2, t.Light,
		t.Latitude, t.Longitude, t.Speed, t.Accuracy, t.Lean,
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
			temperature, humidity, co2, light,
			latitude, longitude, speed, accuracy, lean,
			battery_level, signal_strength,
			is_moving, event_type, raw_payload
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
		)
		ON CONFLICT (device_id, hardware_uid, time) DO NOTHING
	`
	for _, t := range telemetries {
		batch.Queue(query,
			t.Time, t.DeviceID, t.HardwareUID,
			t.Temperature, t.Humidity, t.CO2, t.Light,
			t.Latitude, t.Longitude, t.Speed, t.Accuracy, t.Lean,
			t.BatteryLevel, t.SignalStrength,
			t.IsMoving, t.EventType, t.RawPayload,
		)
	}

	br := r.client.Pool().SendBatch(ctx, batch)
	defer func(br pgx.BatchResults) {
		err := br.Close()
		if err != nil {
			fmt.Printf("error closing batch results: %v\n", err)
		}
	}(br)

	for i := 0; i < len(telemetries); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert telemetry failed at %d: %w", i, err)
		}
	}

	return nil
}

func (r *Repository) InsertEvent(ctx context.Context, e *event.Event) error {
	query := `
		INSERT INTO device_events (
			time, ingested_at, device_id, hardware_uid,
			event_type, severity, description, metadata,
			acknowledged, acknowledged_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (device_id, time, event_type) DO NOTHING
	`

	_, err := r.client.Pool().Exec(ctx, query,
		e.Time, e.IngestedAt, e.DeviceID, e.HardwareUID,
		e.Type, e.Severity, e.Description, e.Metadata,
		e.Acknowledged, e.AcknowledgedAt,
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
			acknowledged, acknowledged_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (device_id, time, event_type) DO NOTHING
	`

	for _, e := range events {
		batch.Queue(query,
			e.Time, e.IngestedAt, e.DeviceID, e.HardwareUID,
			e.Type, e.Severity, e.Description, e.Metadata,
			e.Acknowledged, e.AcknowledgedAt,
		)
	}

	br := r.client.Pool().SendBatch(ctx, batch)
	defer func(br pgx.BatchResults) {
		err := br.Close()
		if err != nil {
			fmt.Printf("error closing batch results: %v\n", err)
		}
	}(br)

	for i := 0; i < len(events); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert events failed at %d: %w", i, err)
		}
	}

	return nil
}

func (r *Repository) GetLatestTelemetry(ctx context.Context, deviceID uuid.UUID) (*telemetry.Telemetry, error) {
	fmt.Printf("[REPO] GetLatestTelemetry called for device: %s\n", deviceID)

	query := `
		SELECT device_id, time, hardware_uid,
		       temperature, humidity, co2, light,
		       latitude, longitude, speed, accuracy, lean,
		       battery_level, signal_strength,
		       is_moving, event_type, raw_payload
		FROM device_telemetry
		WHERE device_id = $1
		ORDER BY time DESC
		LIMIT 1
	`

	var t telemetry.Telemetry
	err := r.client.Pool().QueryRow(ctx, query, deviceID).Scan(
		&t.DeviceID, &t.Time, &t.HardwareUID,
		&t.Temperature, &t.Humidity, &t.CO2, &t.Light,
		&t.Latitude, &t.Longitude, &t.Speed, &t.Accuracy, &t.Lean,
		&t.BatteryLevel, &t.SignalStrength,
		&t.IsMoving, &t.EventType, &t.RawPayload,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		fmt.Printf("[REPO] No telemetry rows found for device: %s\n", deviceID)
		return nil, nil
	}

	if err != nil {
		fmt.Printf("[REPO] Error scanning telemetry for device %s: %v\n", deviceID, err)
		return nil, fmt.Errorf("failed to get latest telemetry: %w", err)
	}

	fmt.Printf("[REPO] Scanned telemetry for device %s:\n", deviceID)
	fmt.Printf("  - time: %v\n", t.Time)
	fmt.Printf("  - temperature: %v (nil=%v)\n", t.Temperature, t.Temperature == nil)
	if t.Temperature != nil {
		fmt.Printf("    *value: %f\n", *t.Temperature)
	}
	fmt.Printf("  - humidity: %v (nil=%v)\n", t.Humidity, t.Humidity == nil)
	if t.Humidity != nil {
		fmt.Printf("    *value: %f\n", *t.Humidity)
	}
	fmt.Printf("  - co2: %v (nil=%v)\n", t.CO2, t.CO2 == nil)
	if t.CO2 != nil {
		fmt.Printf("    *value: %f\n", *t.CO2)
	}
	fmt.Printf("  - light: %v (nil=%v)\n", t.Light, t.Light == nil)
	if t.Light != nil {
		fmt.Printf("    *value: %f\n", *t.Light)
	}
	fmt.Printf("  - latitude: %v (nil=%v)\n", t.Latitude, t.Latitude == nil)
	if t.Latitude != nil {
		fmt.Printf("    *value: %f\n", *t.Latitude)
	}
	fmt.Printf("  - longitude: %v (nil=%v)\n", t.Longitude, t.Longitude == nil)
	if t.Longitude != nil {
		fmt.Printf("    *value: %f\n", *t.Longitude)
	}
	fmt.Printf("  - battery_level: %v (nil=%v)\n", t.BatteryLevel, t.BatteryLevel == nil)
	if t.BatteryLevel != nil {
		fmt.Printf("    *value: %d\n", *t.BatteryLevel)
	}
	fmt.Printf("  - signal_strength: %v (nil=%v)\n", t.SignalStrength, t.SignalStrength == nil)
	if t.SignalStrength != nil {
		fmt.Printf("    *value: %d\n", *t.SignalStrength)
	}
	fmt.Printf("  - is_moving: %v (nil=%v)\n", t.IsMoving, t.IsMoving == nil)
	if t.IsMoving != nil {
		fmt.Printf("    *value: %v\n", *t.IsMoving)
	}

	return &t, nil
}

func (r *Repository) GetLatestLocation(ctx context.Context, deviceID uuid.UUID) (*telemetry.Location, error) {
	query := `
		SELECT device_id, time, latitude, longitude, speed, accuracy
		FROM device_telemetry
		WHERE device_id = $1
		  AND latitude IS NOT NULL
		  AND longitude IS NOT NULL
		ORDER BY time DESC
		LIMIT 1
	`

	var loc telemetry.Location
	err := r.client.Pool().QueryRow(ctx, query, deviceID).Scan(
		&loc.DeviceID, &loc.Time,
		&loc.Latitude, &loc.Longitude,
		&loc.Speed, &loc.Accuracy,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get latest location: %w", err)
	}

	return &loc, nil
}

func (r *Repository) GetLocationHistory(
	ctx context.Context,
	params *telemetry.LocationQueryParams,
) (*telemetry.LocationHistory, error) {
	query := `
		SELECT device_id, time, latitude, longitude, speed, accuracy
		FROM device_telemetry
		WHERE device_id = $1
		  AND latitude IS NOT NULL
		  AND longitude IS NOT NULL
		  AND time >= $2 AND time <= $3
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
		return nil, fmt.Errorf("query location history failed: %w", err)
	}
	defer rows.Close()

	var locations []telemetry.Location
	for rows.Next() {
		var loc telemetry.Location
		if err := rows.Scan(
			&loc.DeviceID,
			&loc.Time,
			&loc.Latitude,
			&loc.Longitude,
			&loc.Speed,
			&loc.Accuracy,
		); err != nil {
			return nil, fmt.Errorf("scan location row failed: %w", err)
		}
		locations = append(locations, loc)
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
		return nil, fmt.Errorf("count location history failed: %w", err)
	}

	return &telemetry.LocationHistory{
		DeviceID:  params.DeviceID,
		Locations: locations,
		Total:     total,
	}, nil
}

func (r *Repository) GetEvents(ctx context.Context, params *event.QueryParams) (*event.List, error) {
	baseWhere := "WHERE device_id = $1 AND time >= $2 AND time <= $3"
	args := []interface{}{params.DeviceID, params.StartTime, params.EndTime}
	placeholderIdx := 4

	buildCondition := func(condition string) string {
		defer func() { placeholderIdx++ }()
		return fmt.Sprintf(" %s $%d", condition, placeholderIdx)
	}

	conditions := []string{baseWhere}

	if params.EventType != nil {
		conditions = append(conditions, buildCondition("AND event_type ="))
		args = append(args, *params.EventType)
	}

	if params.Severity != nil {
		conditions = append(conditions, buildCondition("AND severity ="))
		args = append(args, *params.Severity)
	}

	if params.Acknowledged != nil {
		conditions = append(conditions, buildCondition("AND acknowledged ="))
		args = append(args, *params.Acknowledged)
	}

	whereClause := ""
	if len(conditions) > 1 {
		whereClause = strings.Join(conditions, " ")
	} else {
		whereClause = conditions[0]
	}

	query := fmt.Sprintf(`
		SELECT time, ingested_at, device_id, hardware_uid,
		       event_type, severity, description, metadata,
		       acknowledged, acknowledged_at
		FROM device_events
		%s
		ORDER BY time DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, placeholderIdx, placeholderIdx+1)

	args = append(args, params.Limit, params.Offset)

	rows, err := r.client.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events failed: %w", err)
	}
	defer rows.Close()

	var events []event.Event
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(
			&e.Time, &e.IngestedAt, &e.DeviceID, &e.HardwareUID,
			&e.Type, &e.Severity, &e.Description, &e.Metadata,
			&e.Acknowledged, &e.AcknowledgedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event failed: %w", err)
		}
		events = append(events, e)
	}

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM device_events %s`, whereClause)
	countArgs := args[:len(args)-2]

	var total int
	if err := r.client.Pool().QueryRow(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count events failed: %w", err)
	}

	return &event.List{Events: events, Total: total}, nil
}

func (r *Repository) RecordHeartbeat(ctx context.Context, h *telemetry.Heartbeat) error {
	query := `
		INSERT INTO device_heartbeats (
			time, device_id, hardware_uid, 
		    battery_level, signal_strength, firmware_version
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
		ON CONFLICT (device_id, hardware_uid, time) DO NOTHING
	`

	_, err := r.client.Pool().Exec(ctx, query,
		h.Timestamp, h.DeviceID, h.HardwareUID,
		h.BatteryLevel, h.SignalStrength, h.FirmwareVersion,
	)
	return err
}
