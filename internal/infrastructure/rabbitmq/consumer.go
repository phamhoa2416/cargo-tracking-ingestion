package rabbitmq

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	client *Client
	config *config.RabbitMQConfig
	cache  *redis.Cache

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewConsumer(ctx context.Context, client *Client, config *config.RabbitMQConfig, cache *redis.Cache) *Consumer {
	cctx, cancel := context.WithCancel(ctx)
	return &Consumer{
		client: client,
		config: config,
		cache:  cache,
		ctx:    cctx,
		cancel: cancel,
	}
}

type DeviceConfigUpdateMessage struct {
	MessageID     string                 `json:"message_id"`
	DeviceID      uuid.UUID              `json:"device_id"`
	HardwareUID   uuid.UUID              `json:"hardware_uid"`
	Name          string                 `json:"name"`
	Type          string                 `json:"type"`
	ShipmentID    *uuid.UUID             `json:"shipment_id,omitempty"`
	IsActive      bool                   `json:"is_active"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
}

type ShipmentAssignmentMessage struct {
	MessageID  string    `json:"message_id"`
	ShipmentID uuid.UUID `json:"shipment_id"`
	DeviceID   uuid.UUID `json:"device_id"`
	Action     string    `json:"action"`
}

func (c *Consumer) Start() error {
	msgs, err := c.client.Consume(
		c.config.DeviceUpdateQueue,
		c.config.CustomerTag,
	)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		err := c.consume(msgs)
		if err != nil {
			log.Printf("Error consuming messages: %v", err)
		}
	}()

	log.Printf("RabbitMQ consumer started on queue: %s", c.config.DeviceUpdateQueue)
	return nil
}

func (c *Consumer) consume(msgs <-chan amqp.Delivery) error {
	log.Printf("Started consuming from queue: %s", c.config.DeviceUpdateQueue)

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				log.Println("Delivery channel closed, waiting for reconnect")
				return nil
			}
			c.handleMessage(msg)
		}
	}
}

func (c *Consumer) handleMessage(msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
			_ = msg.Nack(false, true)
		}
	}()

	switch msg.RoutingKey {
	case "device.config.update":
		c.handleDeviceConfigUpdate(msg)
	case "device.shipment.assignment":
		c.handleShipmentAssignment(msg)
	case "device.heartbeat":
		c.handleDeviceHeartbeat(msg)
	case "device.location":
		c.handleDeviceLocation(msg)
	default:
		log.Printf("Unknown routing key: %s", msg.RoutingKey)
		_ = msg.Ack(false)
	}
}

func (c *Consumer) handleDeviceConfigUpdate(msg amqp.Delivery) {
	var update DeviceConfigUpdateMessage
	if err := json.Unmarshal(msg.Body, &update); err != nil {
		log.Printf("Invalid device config update: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	// Update cache
	deviceInfo := &redis.DeviceInfo{
		DeviceID:    update.DeviceID,
		HardwareUID: update.HardwareUID,
		Name:        update.Name,
		Type:        update.Type,
		ShipmentID:  update.ShipmentID,
		IsActive:    update.IsActive,
	}

	if err := c.cache.SetDeviceInfo(c.ctx, deviceInfo); err != nil {
		log.Printf("Failed to update device cache: %v", err)
		_ = msg.Nack(false, true) // Requeue
		return
	}

	// If assigned to shipment, update shipment-device mapping
	if update.ShipmentID != nil {
		_ = c.cache.AddShipmentDevice(c.ctx, *update.ShipmentID, update.DeviceID)
	}

	_ = msg.Ack(false)
}

func (c *Consumer) handleShipmentAssignment(msg amqp.Delivery) {
	var assignment ShipmentAssignmentMessage
	if err := json.Unmarshal(msg.Body, &assignment); err != nil {
		log.Printf("Failed to unmarshal shipment assignment: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	log.Printf("Received shipment assignment: %s %s to shipment %s",
		assignment.Action, assignment.DeviceID, assignment.ShipmentID)

	switch assignment.Action {
	case "assign":
		if err := c.cache.AddShipmentDevice(c.ctx, assignment.ShipmentID, assignment.DeviceID); err != nil {
			log.Printf("Failed to assign device to shipment: %v", err)
			_ = msg.Nack(false, true)
			return
		}

		deviceInfo, err := c.cache.GetDeviceInfo(c.ctx, assignment.DeviceID)
		if err != nil {
			log.Printf("Failed to get device info for update: %v", err)
		} else if deviceInfo != nil {
			deviceInfo.ShipmentID = &assignment.ShipmentID
			if err := c.cache.SetDeviceInfo(c.ctx, deviceInfo); err != nil {
				log.Printf("Failed to update device info with shipment: %v", err)
			}
		}

	case "unassign":
		if err := c.cache.RemoveShipmentDevice(c.ctx, assignment.ShipmentID, assignment.DeviceID); err != nil {
			log.Printf("Failed to unassign device from shipment: %v", err)
			_ = msg.Nack(false, true)
			return
		}

		deviceInfo, err := c.cache.GetDeviceInfo(c.ctx, assignment.DeviceID)
		if err != nil {
			log.Printf("Failed to get device info for update: %v", err)
		} else if deviceInfo != nil {
			deviceInfo.ShipmentID = nil
			if err := c.cache.SetDeviceInfo(c.ctx, deviceInfo); err != nil {
				log.Printf("Failed to update device info: %v", err)
			}
		}

	default:
		log.Printf("Unknown shipment action: %s", assignment.Action)
		_ = msg.Nack(false, false)
		return
	}

	_ = msg.Ack(false)
	log.Printf("Successfully processed shipment assignment for device: %s", assignment.DeviceID)
}

func (c *Consumer) handleDeviceHeartbeat(msg amqp.Delivery) {
	var update struct {
		MessageID      string     `json:"message_id"`
		Timestamp      time.Time  `json:"timestamp"`
		UpdateType     string     `json:"update_type"`
		DeviceID       uuid.UUID  `json:"device_id"`
		IsOnline       *bool      `json:"is_online,omitempty"`
		LastSeen       *time.Time `json:"last_seen,omitempty"`
		BatteryLevel   *int       `json:"battery_level,omitempty"`
		SignalStrength *int       `json:"signal_strength,omitempty"`
	}

	if err := json.Unmarshal(msg.Body, &update); err != nil {
		log.Printf("Invalid device heartbeat message: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	if c.cache == nil {
		_ = msg.Ack(false)
		return
	}

	status := &redis.DeviceStatus{
		DeviceID:       update.DeviceID,
		IsOnline:       true,
		BatteryLevel:   update.BatteryLevel,
		SignalStrength: update.SignalStrength,
		LastHeartbeat:  update.Timestamp,
	}

	if update.LastSeen != nil {
		status.LastHeartbeat = *update.LastSeen
	} else {
		status.LastHeartbeat = update.Timestamp
	}

	if err := c.cache.SetDeviceStatus(c.ctx, status); err != nil {
		log.Printf("Failed to update device status from heartbeat: %v", err)
		_ = msg.Nack(false, true) // Requeue on error
		return
	}

	_ = msg.Ack(false)
	log.Printf("Processed heartbeat for device: %s", update.DeviceID)
}

func (c *Consumer) handleDeviceLocation(msg amqp.Delivery) {
	var update struct {
		MessageID  string    `json:"message_id"`
		Timestamp  time.Time `json:"timestamp"`
		UpdateType string    `json:"update_type"`
		DeviceID   uuid.UUID `json:"device_id"`
		Location   *struct {
			Latitude  float64   `json:"latitude"`
			Longitude float64   `json:"longitude"`
			Accuracy  *float64  `json:"accuracy,omitempty"`
			Timestamp time.Time `json:"timestamp"`
		} `json:"location,omitempty"`
	}

	if err := json.Unmarshal(msg.Body, &update); err != nil {
		log.Printf("Invalid device location message: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	if update.Location == nil {
		log.Printf("Location data is missing in device location message for device: %s", update.DeviceID)
		_ = msg.Ack(false)
		return
	}

	if c.cache == nil {
		_ = msg.Ack(false)
		return
	}

	location := &redis.DeviceLocation{
		DeviceID:  update.DeviceID,
		Latitude:  update.Location.Latitude,
		Longitude: update.Location.Longitude,
		Accuracy:  update.Location.Accuracy,
		Timestamp: update.Location.Timestamp,
	}

	if err := c.cache.SetDeviceLocation(c.ctx, location); err != nil {
		log.Printf("Failed to update device location: %v", err)
		_ = msg.Nack(false, true) // Requeue on error
		return
	}

	_ = msg.Ack(false)
	log.Printf("Processed location update for device: %s", update.DeviceID)
}

func (c *Consumer) Stop() {
	log.Println("Stopping RabbitMQ consumer...")
	c.cancel()
	c.wg.Wait()
	log.Println("RabbitMQ consumer stopped")
}
