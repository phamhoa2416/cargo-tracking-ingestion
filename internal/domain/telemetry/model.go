package telemetry

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Telemetry struct {
	DeviceID    uuid.UUID `json:"device_id"`
	Time        time.Time `json:"time"`
	HardwareUID uuid.UUID `json:"hardware_uid"`

	Temperature *float64 `json:"temperature,omitempty"`
	Humidity    *float64 `json:"humidity,omitempty"`
	Pressure    *float64 `json:"pressure,omitempty"`

	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
	Altitude  *float64 `json:"altitude,omitempty"`
	Speed     *float64 `json:"speed,omitempty"`
	Heading   *float64 `json:"heading,omitempty"`
	Accuracy  *float64 `json:"accuracy,omitempty"`

	BatteryLevel   *int  `json:"battery_level,omitempty"`
	SignalStrength *int  `json:"signal_strength,omitempty"`
	IsMoving       *bool `json:"is_moving,omitempty"`

	EventType  *string         `json:"event_type,omitempty"`
	RawPayload json.RawMessage `json:"raw_payload,omitempty"`
}

type Heartbeat struct {
	DeviceID        uuid.UUID `json:"device_id" binding:"required"`
	HardwareUID     uuid.UUID `json:"hardware_uid" binding:"required"`
	Timestamp       time.Time `json:"timestamp" binding:"required"`
	BatteryLevel    *int      `json:"battery_level,omitempty"`
	SignalStrength  *int      `json:"signal_strength,omitempty"`
	FirmwareVersion string    `json:"firmware_version,omitempty"`
}

type Location struct {
	DeviceID  uuid.UUID `json:"device_id"`
	Time      time.Time `json:"time"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  *float64  `json:"altitude,omitempty"`
	Speed     *float64  `json:"speed,omitempty"`
	Heading   *float64  `json:"heading,omitempty"`
	Accuracy  *float64  `json:"accuracy,omitempty"`
}

type LocationHistory struct {
	DeviceID  uuid.UUID  `json:"device_id"`
	Locations []Location `json:"locations"`
	Total     int        `json:"total"`
}

type MQTTTelemetryPayload struct {
	DeviceID    string                 `json:"device_id"`
	HardwareUID string                 `json:"hardware_uid"`
	Timestamp   int64                  `json:"timestamp"`
	Data        map[string]interface{} `json:"data"`
}

type MQTTHeartbeatPayload struct {
	DeviceID        string `json:"device_id"`
	HardwareUID     string `json:"hardware_uid"`
	Timestamp       int64  `json:"timestamp"`
	BatteryLevel    *int   `json:"battery_level,omitempty"`
	SignalStrength  *int   `json:"signal_strength,omitempty"`
	FirmwareVersion string `json:"firmware_version,omitempty"`
}

type LocationQueryParams struct {
	DeviceID  uuid.UUID `form:"device_id" binding:"required"`
	StartTime time.Time `form:"start_time"`
	EndTime   time.Time `form:"end_time"`
	Limit     int       `form:"limit" binding:"omitempty,min=1,max=1000"`
	Offset    int       `form:"offset" binding:"omitempty,min=0"`
}
