package telemetry

import (
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func ValidateTelemetry(t *telemetry.Telemetry) error {
	if t.DeviceID == uuid.Nil {
		return fmt.Errorf("device_id is required and cannot be zero UUID")
	}

	if t.HardwareUID == uuid.Nil {
		return fmt.Errorf("hardware_uid is required and cannot be zero UUID")
	}

	now := time.Now()
	if t.Time.After(now.Add(1 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 hour in the future")
	}
	if t.Time.Before(now.Add(-365 * 24 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 year in the past")
	}

	// Validate GPS coordinates if provided
	if (t.Latitude != nil && t.Longitude == nil) ||
		(t.Latitude == nil && t.Longitude != nil) {
		if *t.Latitude < -90 || *t.Latitude > 90 {
			return fmt.Errorf("latitude must be between -90 and 90")
		}

		if *t.Longitude < -180 || *t.Longitude > 180 {
			return fmt.Errorf("longitude must be between -180 and 180")
		}

		return fmt.Errorf("latitude and longitude must be provided together")
	}

	// Validate temperature if provided
	if t.Temperature != nil {
		if *t.Temperature < -100 || *t.Temperature > 100 {
			return fmt.Errorf("temperature out of reasonable range")
		}
	}

	// Validate humidity if provided
	if t.Humidity != nil {
		if *t.Humidity < 0 || *t.Humidity > 100 {
			return fmt.Errorf("humidity must be between 0 and 100")
		}
	}

	// Validate battery level if provided
	if t.BatteryLevel != nil {
		if *t.BatteryLevel < 0 || *t.BatteryLevel > 100 {
			return fmt.Errorf("battery_level must be between 0 and 100")
		}
	}

	// Validate speed if provided
	if t.Speed != nil {
		if *t.Speed < 0 {
			return fmt.Errorf("speed cannot be negative")
		}
	}

	// Validate heading if provided
	if t.Heading != nil {
		if *t.Heading < 0 || *t.Heading > 360 {
			return fmt.Errorf("heading must be between 0 and 360")
		}
	}

	// Validate accuracy if provided
	if t.Accuracy != nil {
		if *t.Accuracy < 0 {
			return fmt.Errorf("accuracy cannot be negative")
		}
	}

	return nil
}

func ValidateHeartbeat(h *telemetry.Heartbeat) error {
	if h.DeviceID == uuid.Nil {
		return fmt.Errorf("device_id is required and cannot be zero UUID")
	}

	if h.HardwareUID == uuid.Nil {
		return fmt.Errorf("hardware_uid is required and cannot be zero UUID")
	}

	if h.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}

	now := time.Now()
	if h.Timestamp.After(now.Add(1 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 hour in the future")
	}
	if h.Timestamp.Before(now.Add(-365 * 24 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 year in the past")
	}

	if h.BatteryLevel != nil {
		if *h.BatteryLevel < 0 || *h.BatteryLevel > 100 {
			return fmt.Errorf("battery_level must be between 0 and 100")
		}
	}

	return nil
}
