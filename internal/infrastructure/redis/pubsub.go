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

func (ps *PubSub) publish(ctx context.Context, channel string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ps.client.Client().Publish(ctx, channel, data).Err()
}

func (ps *PubSub) PublishTelemetryUpdate(ctx context.Context, update *TelemetryUpdate) error {
	return ps.publish(ctx, channelTelemetryUpdates, update)
}

func (ps *PubSub) PublishEventAlert(ctx context.Context, alert *EventAlert) error {
	return ps.publish(ctx, channelEventAlerts, alert)
}

func (ps *PubSub) PublishDeviceStatusUpdate(ctx context.Context, status *DeviceStatusUpdate) error {
	return ps.publish(ctx, channelDeviceStatus, status)
}

func (ps *PubSub) PublishShipmentUpdate(ctx context.Context, update *ShipmentUpdate) error {
	return ps.publish(ctx, channelShipmentUpdates, update)
}

func (ps *PubSub) Subscribe(ctx context.Context, channel string, handler func(*redis.Message)) {
	go func() {
		if err := ps.subscribe(ctx, channel, handler); err != nil && ctx.Err() == nil {
			log.Printf("Redis subscription stopped [%s]: %v", channel, err)
		}
	}()
}

func (ps *PubSub) subscribe(ctx context.Context, channel string, handler func(*redis.Message)) error {
	pubsub := ps.client.Client().Subscribe(ctx, channel)
	defer func(pubsub *redis.PubSub) {
		err := pubsub.Close()
		if err != nil {
			log.Printf("Failed to close redis pubsub for channel %s: %v", channel, err)
		}
	}(pubsub)

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("redis channel closed: %s", channel)
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic in redis handler [%s]: %v", channel, r)
					}
				}()
				handler(msg)
			}()
		}
	}
}
