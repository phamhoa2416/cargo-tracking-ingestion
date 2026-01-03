package rabbitmq

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/event"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	client *Client
	config *config.RabbitMQConfig
}

func NewPublisher(client *Client, config *config.RabbitMQConfig) *Publisher {
	return &Publisher{
		client: client,
		config: config,
	}
}

type EventMessage struct {
	MessageID   string                 `json:"message_id"`
	Timestamp   time.Time              `json:"timestamp"`
	Source      string                 `json:"source"`
	EventType   string                 `json:"event_type"`
	DeviceID    uuid.UUID              `json:"device_id"`
	Severity    string                 `json:"severity"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	EventTime   time.Time              `json:"event_time"`
}

type DeviceUpdateMessage struct {
	MessageID      string     `json:"message_id"`
	Timestamp      time.Time  `json:"timestamp"`
	UpdateType     string     `json:"update_type"`
	DeviceID       uuid.UUID  `json:"device_id"`
	IsOnline       *bool      `json:"is_online,omitempty"`
	LastSeen       *time.Time `json:"last_seen,omitempty"`
	BatteryLevel   *int       `json:"battery_level,omitempty"`
	SignalStrength *int       `json:"signal_strength,omitempty"`
	Location       *Location  `json:"location,omitempty"`
}

type Location struct {
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  *float64  `json:"altitude,omitempty"`
	Accuracy  *float64  `json:"accuracy,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type ShipmentUpdateMessage struct {
	MessageID             string    `json:"message_id"`
	Timestamp             time.Time `json:"timestamp"`
	ShipmentID            uuid.UUID `json:"shipment_id"`
	DeviceID              uuid.UUID `json:"device_id"`
	Status                string    `json:"status"`
	Location              *Location `json:"location,omitempty"`
	ETAMinutes            *int      `json:"eta_minutes,omitempty"`
	DistanceToDestination *float64  `json:"distance_to_destination,omitempty"`
}

func (p *Publisher) PublishEvent(ctx context.Context, e *event.Event) error {
	messageID := uuid.New().String()

	msg := EventMessage{
		MessageID:   messageID,
		Timestamp:   time.Now(),
		Source:      "cargo-tracking-ingestion",
		EventType:   string(e.Type),
		DeviceID:    e.DeviceID,
		Severity:    string(e.Severity),
		Description: "",
		EventTime:   e.Time,
	}

	if e.Description != nil {
		msg.Description = *e.Description
	}

	if e.Metadata != nil {
		var metadata map[string]interface{}
		if err := json.Unmarshal(e.Metadata, &metadata); err == nil {
			msg.Metadata = metadata
		} else {
			log.Printf("Failed to unmarshal event metadata for device %s: %v", e.DeviceID, err)
		}
	}

	routingKey := fmt.Sprintf("event.%s", e.Type)
	return p.publish(ctx, routingKey, messageID, msg)
}

func (p *Publisher) PublishEventBatch(ctx context.Context, events []*event.Event) error {
	var lastErr error
	for _, e := range events {
		if err := p.PublishEvent(ctx, e); err != nil {
			log.Printf("Failed to publish event %s for device %s: %v", e.Type, e.DeviceID, err)
			lastErr = err
		}
	}
	return lastErr
}

func (p *Publisher) PublishDeviceHeartbeat(ctx context.Context, deviceID uuid.UUID, batteryLevel, signalStrength *int, timestamp time.Time) error {
	messageID := uuid.New().String()
	isOnline := true

	msg := DeviceUpdateMessage{
		MessageID:      messageID,
		Timestamp:      time.Now(),
		UpdateType:     "heartbeat",
		DeviceID:       deviceID,
		IsOnline:       &isOnline,
		LastSeen:       &timestamp,
		BatteryLevel:   batteryLevel,
		SignalStrength: signalStrength,
	}

	return p.publish(ctx, "device.heartbeat", messageID, msg)
}

func (p *Publisher) PublishDeviceStatus(ctx context.Context, deviceID uuid.UUID, isOnline bool, lastSeen time.Time) error {
	messageID := uuid.New().String()

	msg := DeviceUpdateMessage{
		MessageID:  messageID,
		Timestamp:  time.Now(),
		UpdateType: "status",
		DeviceID:   deviceID,
		IsOnline:   &isOnline,
		LastSeen:   &lastSeen,
	}

	return p.publish(ctx, "device.status", messageID, msg)
}

func (p *Publisher) PublishDeviceLocation(ctx context.Context, deviceID uuid.UUID, location *Location) error {
	messageID := uuid.New().String()

	msg := DeviceUpdateMessage{
		MessageID:  messageID,
		Timestamp:  time.Now(),
		UpdateType: "location",
		DeviceID:   deviceID,
		Location:   location,
	}

	return p.publish(ctx, "device.location", messageID, msg)
}

func (p *Publisher) PublishShipmentUpdate(ctx context.Context, update *ShipmentUpdateMessage) error {
	update.MessageID = uuid.New().String()
	update.Timestamp = time.Now()
	return p.publish(ctx, "shipment.tracking", update.MessageID, update)
}

func (p *Publisher) publish(ctx context.Context, routingKey, messageID string, message interface{}) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         payload,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		MessageId:    messageID,
	}

	err = p.client.Publish(ctx, p.config.Exchange, routingKey, publishing)
	if err != nil {
		log.Printf("Failed to publish to routing key %s: %v", routingKey, err)
		return err
	}

	return nil
}
