package shipment

import (
	"cargo-tracking-ingestion/internal/domain/shipment"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func ValidateTrackingPoint(point *shipment.TrackingPoint) error {
	if point.ShipmentID == uuid.Nil {
		return fmt.Errorf("shipment_id is required and cannot be zero UUID")
	}

	if point.DeviceID == uuid.Nil {
		return fmt.Errorf("device_id is required and cannot be zero UUID")
	}

	now := time.Now()
	if point.Time.After(now.Add(1 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 hour in the future")
	}
	if point.Time.Before(now.Add(-365 * 24 * time.Hour)) {
		return fmt.Errorf("timestamp cannot be more than 1 year in the past")
	}

	// Validate GPS coordinates if provided
	if (point.Latitude != nil && point.Longitude == nil) ||
		(point.Latitude == nil && point.Longitude != nil) {
		return fmt.Errorf("latitude and longitude must be provided together")
	}

	if point.Latitude != nil {
		if *point.Latitude < -90 || *point.Latitude > 90 {
			return fmt.Errorf("latitude must be between -90 and 90")
		}
	}

	if point.Longitude != nil {
		if *point.Longitude < -180 || *point.Longitude > 180 {
			return fmt.Errorf("longitude must be between -180 and 180")
		}
	}

	// Validate speed if provided
	if point.Speed != nil {
		if *point.Speed < 0 {
			return fmt.Errorf("speed cannot be negative")
		}
	}

	// Validate heading if provided
	if point.Heading != nil {
		if *point.Heading < 0 || *point.Heading > 360 {
			return fmt.Errorf("heading must be between 0 and 360")
		}
	}

	// Validate accuracy if provided
	if point.Accuracy != nil {
		if *point.Accuracy < 0 {
			return fmt.Errorf("accuracy cannot be negative")
		}
	}

	// Validate ETA if provided
	if point.ETAMinutes != nil {
		if *point.ETAMinutes < 0 {
			return fmt.Errorf("eta_minutes cannot be negative")
		}
	}

	// Validate distance if provided
	if point.DistanceToDestination != nil {
		if *point.DistanceToDestination < 0 {
			return fmt.Errorf("distance_to_destination cannot be negative")
		}
	}

	return nil
}
