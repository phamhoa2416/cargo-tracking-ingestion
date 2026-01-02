package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Type string
type Severity string

const (
	LowBattery    Type = "LOW_BATTERY"
	PoorSignal    Type = "POOR_SIGNAL"
	GeofenceEnter Type = "GEOFENCE_ENTER"
	GeofenceExit  Type = "GEOFENCE_EXIT"
	TempHigh      Type = "TEMP_HIGH"
	TempLow       Type = "TEMP_LOW"
	HumidityHigh  Type = "HUMIDITY_HIGH"
	DeviceOffline Type = "DEVICE_OFFLINE"
	DeviceOnline  Type = "DEVICE_ONLINE"
	Stopped       Type = "STOPPED"
	Moving        Type = "MOVING"

	SeverityInfo     Severity = "INFO"
	SeverityWarning  Severity = "WARNING"
	SeverityCritical Severity = "CRITICAL"
)

type Event struct {
	Time       time.Time `json:"time"`
	IngestedAt time.Time `json:"ingested_at"`

	DeviceID    uuid.UUID  `json:"device_id"`
	HardwareUID *uuid.UUID `json:"hardware_uid,omitempty"`

	Type     Type     `json:"type"`
	Severity Severity `json:"severity"`

	Description *string         `json:"description,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`

	Acknowledged   bool       `json:"acknowledged"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
}

type QueryParams struct {
	DeviceID     uuid.UUID `form:"device_id" binding:"required"`
	StartTime    time.Time `form:"start_time"`
	EndTime      time.Time `form:"end_time"`
	EventType    *Type     `form:"event_type"`
	Severity     *Severity `form:"severity"`
	Acknowledged *bool     `form:"acknowledged"`
	Limit        int       `form:"limit" binding:"omitempty,min=1,max=1000"`
	Offset       int       `form:"offset" binding:"omitempty,min=0"`
}

type List struct {
	Events []Event `json:"events"`
	Total  int     `json:"total"`
}

/* ---------- Metadata ---------- */

type LowBatteryMetadata struct {
	BatteryLevel int `json:"battery_level"`
	Threshold    int `json:"threshold"`
}

type TemperatureMetadata struct {
	Temperature float64 `json:"temperature"`
	Threshold   float64 `json:"threshold"`
	Unit        string  `json:"unit"`
}

type GeofenceMetadata struct {
	GeofenceID   uuid.UUID `json:"geofence_id"`
	GeofenceName string    `json:"geofence_name"`
	Latitude     float64   `json:"latitude"`
	Longitude    float64   `json:"longitude"`
}

type SignalMetadata struct {
	SignalStrength int `json:"signal_strength"`
	Threshold      int `json:"threshold"`
}

func NewEvent(deviceID uuid.UUID, eventType Type, severity Severity) *Event {
	now := time.Now()
	return &Event{
		Time:         now,
		IngestedAt:   now,
		DeviceID:     deviceID,
		Type:         eventType,
		Severity:     severity,
		Acknowledged: false,
	}
}

func (e *Event) WithHardwareUID(uid uuid.UUID) *Event {
	e.HardwareUID = &uid
	return e
}

func (e *Event) WithDescription(desc string) *Event {
	e.Description = &desc
	return e
}

func (e *Event) WithMetadata(v interface{}) (*Event, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	e.Metadata = data
	return e, nil
}

func (e *Event) Acknowledge() {
	now := time.Now()
	e.Acknowledged = true
	e.AcknowledgedAt = &now
}
