package rabbitmq

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/infrastructure/redis"
	"context"
	"encoding/json"
	"log"
	"sync"

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

func NewConsumer(client *Client, config *config.RabbitMQConfig, cache *redis.Cache) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		client: client,
		config: config,
		cache:  cache,
		ctx:    ctx,
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

func (c *Consumer) ConsumeDeviceUpdates(ctx context.Context) error {
	c.wg.Add(1)
	go c.consumeLoop(ctx)
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Consumer context cancelled, stopping consume loop")
			return
		case <-ctx.Done():
			log.Println("Parent context cancelled, stopping consume loop")
			return
		default:
		}

		// Start consuming
		if err := c.startConsuming(ctx); err != nil {
			log.Printf("Error in consumer: %v", err)
		}

		// Wait for reconnect signal or context cancellation
		select {
		case <-c.ctx.Done():
			return
		case <-ctx.Done():
			return
		case <-c.client.ReconnectNotify():
			log.Println("Received reconnect notification, restarting consumer...")
		}
	}
}

func (c *Consumer) startConsuming(ctx context.Context) error {
	msgs, err := c.client.Consume(c.config.DeviceUpdateQueue, "ingestion-service-consumer")
	if err != nil {
		return err
	}

	log.Printf("Started consuming from queue: %s", c.config.DeviceUpdateQueue)

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				log.Println("Message channel closed, will attempt to reconnect")
				return nil
			}
			c.handleDeviceUpdate(ctx, msg)
		}
	}
}

func (c *Consumer) handleDeviceUpdate(ctx context.Context, msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in handleDeviceUpdate: %v", r)
			_ = msg.Nack(false, true)
		}
	}()

	switch msg.RoutingKey {
	case "device.config.update":
		c.handleDeviceConfigUpdate(ctx, msg)
	case "device.shipment.assignment":
		c.handleShipmentAssignment(ctx, msg)
	default:
		log.Printf("Unknown routing key: %s", msg.RoutingKey)
		_ = msg.Ack(false)
	}
}

func (c *Consumer) handleDeviceConfigUpdate(ctx context.Context, msg amqp.Delivery) {
	var update DeviceConfigUpdateMessage
	if err := json.Unmarshal(msg.Body, &update); err != nil {
		log.Printf("Failed to unmarshal device config update: %v", err)
		_ = msg.Nack(false, false) // Don't requeue invalid messages
		return
	}

	log.Printf("Received device config update for device: %s", update.DeviceID)

	// Update cache
	deviceInfo := &redis.DeviceInfo{
		DeviceID:    update.DeviceID,
		HardwareUID: update.HardwareUID,
		Name:        update.Name,
		Type:        update.Type,
		ShipmentID:  update.ShipmentID,
		IsActive:    update.IsActive,
	}

	if err := c.cache.SetDeviceInfo(ctx, deviceInfo); err != nil {
		log.Printf("Failed to update device cache: %v", err)
		_ = msg.Nack(false, true) // Requeue
		return
	}

	// If assigned to shipment, update shipment-device mapping
	if update.ShipmentID != nil {
		if err := c.cache.AddShipmentDevice(ctx, *update.ShipmentID, update.DeviceID); err != nil {
			log.Printf("Failed to add device to shipment: %v", err)
			// Don't fail the whole message for this, but log the error
		}
	}

	_ = msg.Ack(false)
	log.Printf("Successfully processed device config update for: %s", update.DeviceID)
}

func (c *Consumer) handleShipmentAssignment(ctx context.Context, msg amqp.Delivery) {
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
		if err := c.cache.AddShipmentDevice(ctx, assignment.ShipmentID, assignment.DeviceID); err != nil {
			log.Printf("Failed to assign device to shipment: %v", err)
			_ = msg.Nack(false, true)
			return
		}

		deviceInfo, err := c.cache.GetDeviceInfo(ctx, assignment.DeviceID)
		if err != nil {
			log.Printf("Failed to get device info for update: %v", err)
			// Continue - the main operation succeeded
		} else if deviceInfo != nil {
			deviceInfo.ShipmentID = &assignment.ShipmentID
			if err := c.cache.SetDeviceInfo(ctx, deviceInfo); err != nil {
				log.Printf("Failed to update device info with shipment: %v", err)
				// Continue - the main operation succeeded
			}
		}

	case "unassign":
		if err := c.cache.RemoveShipmentDevice(ctx, assignment.ShipmentID, assignment.DeviceID); err != nil {
			log.Printf("Failed to unassign device from shipment: %v", err)
			_ = msg.Nack(false, true)
			return
		}

		deviceInfo, err := c.cache.GetDeviceInfo(ctx, assignment.DeviceID)
		if err != nil {
			log.Printf("Failed to get device info for update: %v", err)
		} else if deviceInfo != nil {
			deviceInfo.ShipmentID = nil
			if err := c.cache.SetDeviceInfo(ctx, deviceInfo); err != nil {
				log.Printf("Failed to update device info: %v", err)
			}
		}

	default:
		log.Printf("Unknown shipment assignment action: %s", assignment.Action)
		_ = msg.Nack(false, false)
		return
	}

	_ = msg.Ack(false)
	log.Printf("Successfully processed shipment assignment for device: %s", assignment.DeviceID)
}

func (c *Consumer) Stop() {
	log.Println("Stopping RabbitMQ consumer...")
	c.cancel()
	c.wg.Wait()
	log.Println("RabbitMQ consumer stopped")
}
