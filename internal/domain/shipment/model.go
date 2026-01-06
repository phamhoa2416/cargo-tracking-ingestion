package shipment

import (
	"time"

	"github.com/google/uuid"
)

type TrackingPoint struct {
	Time                  time.Time `json:"time"`
	IngestedAt            time.Time `json:"ingested_at"`
	ShipmentID            uuid.UUID `json:"shipment_id"`
	DeviceID              uuid.UUID `json:"device_id"`
	Latitude              *float64  `json:"latitude,omitempty"`
	Longitude             *float64  `json:"longitude,omitempty"`
	Status                *string   `json:"status,omitempty"`
	ETAMinutes            *int      `json:"eta_minutes,omitempty"`
	DistanceToDestination *float64  `json:"distance_to_destination,omitempty"`
	Speed                 *float64  `json:"speed,omitempty"`
	Accuracy              *float64  `json:"accuracy,omitempty"`
}

type TrackingHistory struct {
	ShipmentID uuid.UUID       `json:"shipment_id"`
	Points     []TrackingPoint `json:"points"`
	Total      int             `json:"total"`
}

type LiveTracking struct {
	ShipmentID            uuid.UUID      `json:"shipment_id"`
	CurrentLocation       *Location      `json:"current_location,omitempty"`
	Status                string         `json:"status"`
	ETAMinutes            *int           `json:"eta_minutes,omitempty"`
	DistanceToDestination *float64       `json:"distance_to_destination,omitempty"`
	ActiveDevices         []DeviceStatus `json:"active_devices"`
	LastUpdate            time.Time      `json:"last_update"`
}

type Location struct {
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Accuracy  *float64  `json:"accuracy,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type DeviceStatus struct {
	DeviceID       uuid.UUID `json:"device_id"`
	IsOnline       bool      `json:"is_online"`
	LastSeen       time.Time `json:"last_seen"`
	BatteryLevel   *int      `json:"battery_level,omitempty"`
	SignalStrength *int      `json:"signal_strength,omitempty"`
}

type TrackingQueryParams struct {
	ShipmentID uuid.UUID `form:"shipment_id" binding:"required"`
	StartTime  time.Time `form:"start_time"`
	EndTime    time.Time `form:"end_time"`
	Limit      int       `form:"limit" binding:"omitempty,min=1,max=1000"`
	Offset     int       `form:"offset" binding:"omitempty,min=0"`
}

type RouteInfo struct {
	ShipmentID        uuid.UUID  `json:"shipment_id"`
	Origin            Location   `json:"origin"`
	Destination       Location   `json:"destination"`
	CurrentLocation   *Location  `json:"current_location,omitempty"`
	PlannedRoute      []Location `json:"planned_route,omitempty"`
	ActualRoute       []Location `json:"actual_route"`
	DeviationDistance *float64   `json:"deviation_distance_km,omitempty"`
	ProgressPercent   float64    `json:"progress_percent"`
}

func (tp *TrackingPoint) HasLocation() bool {
	return tp.Latitude != nil && tp.Longitude != nil
}
