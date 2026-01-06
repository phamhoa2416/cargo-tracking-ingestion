package mqtt

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type MessageHandler func(payload []byte, topic string) error

type Client struct {
	client mqtt.Client
	config *config.MQTTConfig

	connected atomic.Bool

	mu               sync.RWMutex
	telemetryHandler MessageHandler
	heartbeatHandler MessageHandler
}

func NewClient(config *config.MQTTConfig) *Client {
	return &Client{
		config: config,
	}
}

func (c *Client) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port)).
		SetClientID(c.config.ClientID).
		SetUsername(c.config.Username).
		SetPassword(c.config.Password).
		SetCleanSession(c.config.CleanSession).
		SetKeepAlive(c.config.KeepAlive).
		SetConnectTimeout(c.config.ConnectTimeout).
		SetAutoReconnect(c.config.AutoReconnect).
		SetMaxReconnectInterval(c.config.MaxReconnectDelay)

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("[MQTT] connection lost: %v", err)
		c.connected.Store(false)
	})

	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		log.Println("[MQTT] connected")
		c.connected.Store(true)

		if err := c.subscribe(); err != nil {
			log.Printf("[MQTT] resubscribe failed: %v", err)
		}
	})

	c.client = mqtt.NewClient(opts)

	token := c.client.Connect()
	if !token.WaitTimeout(c.config.ConnectTimeout) {
		return fmt.Errorf("mqtt connect timeout")
	}

	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt connect failed: %w", err)
	}

	log.Printf("Connected to MQTT broker at %s:%d", c.config.Broker, c.config.Port)

	return c.subscribe()
}

func (c *Client) subscribe() error {
	if err := c.subscribeTopic(
		c.config.TelemetryTopic,
		func(_ mqtt.Client, message mqtt.Message) {
			c.mu.RLock()
			handler := c.telemetryHandler
			c.mu.RUnlock()

			if handler == nil {
				return
			}

			if err := handler(message.Payload(), message.Topic()); err != nil {
				log.Printf("[MQTT] telemetry handler error: %v", err)
			}
		},
	); err != nil {
		return err
	}

	if err := c.subscribeTopic(
		c.config.HeartbeatTopic,
		func(_ mqtt.Client, message mqtt.Message) {
			c.mu.RLock()
			handler := c.heartbeatHandler
			c.mu.RUnlock()

			if handler == nil {
				return
			}

			if err := handler(message.Payload(), message.Topic()); err != nil {
				log.Printf("[MQTT] heartbeat handler error: %v", err)
			}
		},
	); err != nil {
		return err
	}

	return nil
}

func (c *Client) subscribeTopic(topic string, handler mqtt.MessageHandler) error {
	token := c.client.Subscribe(topic, byte(c.config.QoS), handler)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("subscribe timeout: %s", topic)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("subscribe failed (%s): %w", topic, err)
	}

	log.Printf("[MQTT] subscribed to %s", topic)
	return nil
}

func (c *Client) SetTelemetryHandler(h MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.telemetryHandler = h
}

func (c *Client) SetHeartbeatHandler(h MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartbeatHandler = h
}

func (c *Client) Publish(topic string, payload any) error {
	if !c.IsConnected() {
		return fmt.Errorf("mqtt not connected")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload failed: %w", err)
	}

	token := c.client.Publish(topic, byte(c.config.QoS), false, data)
	if !token.WaitTimeout(c.config.ConnectTimeout) {
		return fmt.Errorf("publish timeout")
	}

	return token.Error()
}

func (c *Client) PublishCommand(deviceID uuid.UUID, command any) error {
	topic := fmt.Sprintf("device/%s/command", deviceID)
	return c.Publish(topic, command)
}

func (c *Client) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
		c.connected.Store(false)
		log.Println("[MQTT] disconnected")
	}
}

func (c *Client) IsConnected() bool {
	return c.client != nil &&
		c.client.IsConnected() &&
		c.connected.Load()
}

func ParseTelemetryPayload(payload []byte) (*telemetry.Telemetry, error) {
	var raw telemetry.MQTTTelemetryPayload
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("invalid telemetry payload: %w", err)
	}

	deviceID, err := uuid.Parse(raw.DeviceID)
	if err != nil {
		return nil, fmt.Errorf("invalid device_id: %w", err)
	}

	hardwareUID, err := uuid.Parse(raw.HardwareUID)
	if err != nil {
		return nil, fmt.Errorf("invalid hardware_uid: %w", err)
	}

	t := &telemetry.Telemetry{
		Time:        time.UnixMilli(raw.Timestamp),
		DeviceID:    deviceID,
		HardwareUID: hardwareUID,
	}

	data := raw.Data
	assignFloat := func(key string, target **float64) {
		if v, ok := data[key].(float64); ok {
			*target = &v
		}
	}

	assignInt := func(key string, target **int) {
		if v, ok := data[key].(float64); ok {
			i := int(v)
			*target = &i
		}
	}

	assignFloat("temperature", &t.Temperature)
	assignFloat("humidity", &t.Humidity)
	assignFloat("co2", &t.CO2)
	assignFloat("light", &t.Light)
	assignFloat("latitude", &t.Latitude)
	assignFloat("longitude", &t.Longitude)
	assignFloat("speed", &t.Speed)
	assignFloat("accuracy", &t.Accuracy)
	assignFloat("lean", &t.Lean)
	assignInt("battery_level", &t.BatteryLevel)
	assignInt("signal_strength", &t.SignalStrength)

	if v, ok := data["is_moving"].(bool); ok {
		t.IsMoving = &v
	}
	if v, ok := data["event_type"].(string); ok {
		t.EventType = &v
	}

	if rawPayload, err := json.Marshal(data); err == nil {
		t.RawPayload = rawPayload
	}

	return t, nil
}

func ParseHeartbeatPayload(payload []byte) (*telemetry.Heartbeat, error) {
	var raw telemetry.MQTTHeartbeatPayload
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("invalid heartbeat payload: %w", err)
	}

	deviceID, err := uuid.Parse(raw.DeviceID)
	if err != nil {
		return nil, err
	}

	hardwareUID, err := uuid.Parse(raw.HardwareUID)
	if err != nil {
		return nil, err
	}

	return &telemetry.Heartbeat{
		DeviceID:        deviceID,
		HardwareUID:     hardwareUID,
		Timestamp:       time.UnixMilli(raw.Timestamp),
		BatteryLevel:    raw.BatteryLevel,
		SignalStrength:  raw.SignalStrength,
		FirmwareVersion: raw.FirmwareVersion,
	}, nil
}
