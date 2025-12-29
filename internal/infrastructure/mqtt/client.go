package mqtt

import (
	"cargo-tracking-ingestion/internal/config"
	"cargo-tracking-ingestion/internal/domain/telemetry"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type MessageHandler func(payload []byte, topic string) error

type Client struct {
	client           mqtt.Client
	config           *config.MQTTConfig
	telemetryHandler MessageHandler
	heartbeatHandler MessageHandler
	connected        bool
}

func NewClient(config *config.MQTTConfig) *Client {
	return &Client{
		config:    config,
		connected: false,
	}
}

func (c *Client) Connect(ctx context.Context) error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port))
	opts.SetClientID(c.config.ClientID)
	opts.SetUsername(c.config.Username)
	opts.SetPassword(c.config.Password)
	opts.SetCleanSession(c.config.CleanSession)
	opts.SetKeepAlive(c.config.KeepAlive)
	opts.SetConnectTimeout(c.config.ConnectTimeout)
	opts.SetAutoReconnect(c.config.AutoReconnect)
	opts.SetMaxReconnectInterval(c.config.MaxReconnectDelay)

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.Printf("Connection lost: %v", err)
		c.connected = false
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("MQTT connected successfully")
		c.connected = true

		if err := c.subscribe(); err != nil {
			log.Printf("Failed to subscribe on reconnect: %v", err)
		}
	})

	c.client = mqtt.NewClient(opts)

	token := c.client.Connect()
	if !token.WaitTimeout(c.config.ConnectTimeout) {
		return fmt.Errorf("connection timeout")
	}

	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	log.Printf("Connected to MQTT broker at %s:%d", c.config.Broker, c.config.Port)

	return c.subscribe()
}

func (c *Client) subscribe() error {
	token := c.client.Subscribe(c.config.TelemetryTopic, byte(c.config.QoS), c.telemetryMessageHandler)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("telemetry subscription timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to subscribe to telemetry: %w", err)
	}
	log.Printf("Subscribe to telemetry topic: %s", c.config.TelemetryTopic)

	token = c.client.Subscribe(c.config.HeartbeatTopic, byte(c.config.QoS), c.heartbeatMessageHandler)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("heartbeat subscription timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to subscribe to heartbeat: %w", err)
	}
	log.Printf("Subscribed to heartbeat topic: %s", c.config.HeartbeatTopic)

	return nil
}

func (c *Client) telemetryMessageHandler(client mqtt.Client, msg mqtt.Message) {
	if c.telemetryHandler != nil {
		if err := c.telemetryHandler(msg.Payload(), msg.Topic()); err != nil {
			log.Printf("Error handling telemetry message: %v", err)
		}
	}
}

func (c *Client) heartbeatMessageHandler(client mqtt.Client, msg mqtt.Message) {
	if c.heartbeatHandler != nil {
		if err := c.heartbeatHandler(msg.Payload(), msg.Topic()); err != nil {
			log.Printf("Error handling heartbeat message: %v", err)
		}
	}
}

func (c *Client) SetTelemetryHandler(handler MessageHandler) {
	c.telemetryHandler = handler
}

func (c *Client) SetHeartbeatHandler(handler MessageHandler) {
	c.heartbeatHandler = handler
}

func (c *Client) Publish(topic string, payload interface{}) error {
	if !c.connected {
		return fmt.Errorf("mqtt client not connected")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	token := c.client.Publish(topic, byte(c.config.QoS), false, data)
	if !token.WaitTimeout(c.config.ConnectTimeout) {
		return fmt.Errorf("connection timeout")
	}

	return token.Error()
}

func (c *Client) PublishCommand(deviceId uuid.UUID, command interface{}) error {
	topic := fmt.Sprintf("device/%s/command", deviceId.String())
	return c.Publish(topic, command)
}

func (c *Client) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
		log.Println("MQTT client disconnected")
	}
}

func (c *Client) IsConnected() bool {
	return c.connected && c.client != nil && c.client.IsConnected()
}

func ParseTelemetryPayload(payload []byte) (*telemetry.Telemetry, error) {
	var mqttPayload telemetry.MQTTTelemetryPayload
	if err := json.Unmarshal(payload, &mqttPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	deviceID, err := uuid.Parse(mqttPayload.DeviceID)
	if err != nil {
		return nil, fmt.Errorf("invalid device_id: %w", err)
	}

	hardwareUID, err := uuid.Parse(mqttPayload.HardwareUID)
	if err != nil {
		return nil, fmt.Errorf("invalid hardware_uid: %w", err)
	}

	t := &telemetry.Telemetry{
		Time:        time.UnixMilli(mqttPayload.Timestamp),
		DeviceID:    deviceID,
		HardwareUID: hardwareUID,
	}

	data := mqttPayload.Data

	if v, ok := data["temperature"].(float64); ok {
		t.Temperature = &v
	}
	if v, ok := data["humidity"].(float64); ok {
		t.Humidity = &v
	}
	if v, ok := data["pressure"].(float64); ok {
		t.Pressure = &v
	}
	if v, ok := data["latitude"].(float64); ok {
		t.Latitude = &v
	}
	if v, ok := data["longitude"].(float64); ok {
		t.Longitude = &v
	}
	if v, ok := data["altitude"].(float64); ok {
		t.Altitude = &v
	}
	if v, ok := data["speed"].(float64); ok {
		t.Speed = &v
	}
	if v, ok := data["heading"].(float64); ok {
		t.Heading = &v
	}
	if v, ok := data["accuracy"].(float64); ok {
		t.Accuracy = &v
	}
	if v, ok := data["battery_level"].(float64); ok {
		battery := int(v)
		t.BatteryLevel = &battery
	}
	if v, ok := data["signal_strength"].(float64); ok {
		signal := int(v)
		t.SignalStrength = &signal
	}
	if v, ok := data["is_moving"].(bool); ok {
		t.IsMoving = &v
	}
	if v, ok := data["event_type"].(string); ok {
		t.EventType = &v
	}

	rawData, _ := json.Marshal(data)
	t.RawPayload = rawData

	return t, nil
}

func ParseHeartbeatPayload(payload []byte) (*telemetry.Heartbeat, error) {
	var mqttPayload telemetry.MQTTHeartbeatPayload
	if err := json.Unmarshal(payload, &mqttPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal heartbeat: %w", err)
	}

	deviceID, err := uuid.Parse(mqttPayload.DeviceID)
	if err != nil {
		return nil, fmt.Errorf("invalid device_id: %w", err)
	}

	hardwareUID, err := uuid.Parse(mqttPayload.HardwareUID)
	if err != nil {
		return nil, fmt.Errorf("invalid hardware_uid: %w", err)
	}

	return &telemetry.Heartbeat{
		DeviceID:        deviceID,
		HardwareUID:     hardwareUID,
		Timestamp:       time.UnixMilli(mqttPayload.Timestamp),
		BatteryLevel:    mqttPayload.BatteryLevel,
		SignalStrength:  mqttPayload.SignalStrength,
		FirmwareVersion: mqttPayload.FirmwareVersion,
	}, nil
}
