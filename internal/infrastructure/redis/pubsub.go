package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	channelTelemetryUpdates = "telemetry:updates"
	channelEventAlerts      = "event:alerts"
	channelDeviceStatus     = "device:status"
	channelShipmentUpdates  = "shipment:updates"
)

type PubSub struct {
	client *Client
}

func NewPubSub(client *Client) *PubSub {
	return &PubSub{client: client}
}

type TelemetryUpdate struct {
	DeviceID    uuid.UUID              `json:"device_id"`
	Timestamp   int64                  `json:"timestamp"`
	HasLocation bool                   `json:"has_location"`
	Data        map[string]interface{} `json:"data"`
}

type EventAlert struct {
	DeviceID    uuid.UUID `json:"device_id"`
	EventType   string    `json:"event_type"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   int64     `json:"timestamp"`
}

type DeviceStatusUpdate struct {
	DeviceID uuid.UUID `json:"device_id"`
	IsOnline bool      `json:"is_online"`
	LastSeen int64     `json:"last_seen"`
}

type ShipmentUpdate struct {
	ShipmentID uuid.UUID `json:"shipment_id"`
	Status     string    `json:"status"`
	Location   *struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"location,omitempty"`
	ETAMinutes *int  `json:"eta_minutes,omitempty"`
	Timestamp  int64 `json:"timestamp"`
}

func (ps *PubSub) PublishTelemetryUpdate(ctx context.Context, update *TelemetryUpdate) error {
	data, err := json.Marshal(update)
	if err != nil {
		return err
	}

	return ps.client.Client().Publish(ctx, channelTelemetryUpdates, data).Err()
}

func (ps *PubSub) PublishEventAlert(ctx context.Context, alert *EventAlert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	return ps.client.Client().Publish(ctx, channelEventAlerts, data).Err()
}

func (ps *PubSub) PublishDeviceStatusUpdate(ctx context.Context, status *DeviceStatusUpdate) error {
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}

	return ps.client.Client().Publish(ctx, channelDeviceStatus, data).Err()
}

func (ps *PubSub) PublishShipmentUpdate(ctx context.Context, update *ShipmentUpdate) error {
	data, err := json.Marshal(update)
	if err != nil {
		return err
	}

	return ps.client.Client().Publish(ctx, channelShipmentUpdates, data).Err()
}

func (ps *PubSub) SubscribeTelemetryUpdates(ctx context.Context, handler func(update *TelemetryUpdate)) error {
	go func() {
		if err := ps.subscribe(ctx, channelTelemetryUpdates, func(msg *redis.Message) {
			var update TelemetryUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
				log.Printf("Failed to unmarshal telemetry update: %v", err)
				return
			}
			handler(&update)
		}); err != nil && ctx.Err() == nil {
			log.Printf("Telemetry subscription stopped: %v", err)
		}
	}()

	return nil
}

func (ps *PubSub) SubscribeEventAlerts(ctx context.Context, handler func(*EventAlert)) error {
	go func() {
		if err := ps.subscribe(ctx, channelEventAlerts, func(msg *redis.Message) {
			var alert EventAlert
			if err := json.Unmarshal([]byte(msg.Payload), &alert); err != nil {
				log.Printf("Failed to unmarshal event alert: %v", err)
				return
			}
			handler(&alert)
		}); err != nil && ctx.Err() == nil {
			log.Printf("Event alert subscription stopped: %v", err)
		}
	}()

	return nil
}

func (ps *PubSub) SubscribeDeviceStatus(ctx context.Context, handler func(*DeviceStatusUpdate)) error {
	go func() {
		if err := ps.subscribe(ctx, channelDeviceStatus, func(msg *redis.Message) {
			var status DeviceStatusUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &status); err != nil {
				log.Printf("Failed to unmarshal device status: %v", err)
				return
			}
			handler(&status)
		}); err != nil && ctx.Err() == nil {
			log.Printf("Device status subscription stopped: %v", err)
		}
	}()

	return nil
}

func (ps *PubSub) SubscribeShipmentUpdates(ctx context.Context, handler func(*ShipmentUpdate)) error {
	go func() {
		if err := ps.subscribe(ctx, channelShipmentUpdates, func(msg *redis.Message) {
			var update ShipmentUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
				log.Printf("Failed to unmarshal shipment update: %v", err)
				return
			}
			handler(&update)
		}); err != nil && ctx.Err() == nil {
			log.Printf("Shipment update subscription stopped: %v", err)
		}
	}()

	return nil
}

func (ps *PubSub) subscribe(ctx context.Context, channel string, handler func(message *redis.Message)) error {
	pubsub := ps.client.Client().Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("channel closed")
			}
			if msg == nil {
				continue
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Panic in handler: %v", r)
					}
				}()
				handler(msg)
			}()
		}
	}
}
