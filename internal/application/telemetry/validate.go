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
	if t.Latitude != nil || t.Longitude != nil {
		// Both must be provided together
		if t.Latitude == nil || t.Longitude == nil {
			return fmt.Errorf("latitude and longitude must be provided together")
		}

		// Validate latitude range
		if *t.Latitude < -90 || *t.Latitude > 90 {
			return fmt.Errorf("latitude must be between -90 and 90")
		}

		// Validate longitude range
		if *t.Longitude < -180 || *t.Longitude > 180 {
			return fmt.Errorf("longitude must be between -180 and 180")
		}
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

	// Validate CO2 if provided (typical range: 0-10000 ppm)
	if t.CO2 != nil {
		if *t.CO2 < 0 || *t.CO2 > 10000 {
			return fmt.Errorf("co2 must be between 0 and 10000 ppm")
		}
	}

	// Validate light if provided (must be non-negative)
	if t.Light != nil {
		if *t.Light < 0 {
			return fmt.Errorf("light cannot be negative")
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

	// Validate accuracy if provided
	if t.Accuracy != nil {
		if *t.Accuracy < 0 {
			return fmt.Errorf("accuracy cannot be negative")
		}
	}

	// Validate lean if provided (angle typically -180 to 180 degrees)
	if t.Lean != nil {
		if *t.Lean < -180 || *t.Lean > 180 {
			return fmt.Errorf("lean must be between -180 and 180 degrees")
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
