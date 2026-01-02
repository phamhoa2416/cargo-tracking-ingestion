package rabbitmq

import (
	"cargo-tracking-ingestion/internal/config"
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *config.RabbitMQConfig

	closed bool
}

func NewClient(config *config.RabbitMQConfig) (*Client, error) {
	client := &Client{
		config: config,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) connect() error {
	conn, err := amqp.Dial(c.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	err = channel.ExchangeDeclare(
		c.config.Exchange,
		"topic",
		c.config.Durable,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	_, err = channel.QueueDeclare(
		c.config.EventQueue,
		c.config.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare event queue: %w", err)
	}

	_, err = channel.QueueDeclare(
		c.config.DeviceUpdateQueue,
		c.config.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare device update queue: %w", err)
	}

	err = channel.QueueBind(
		c.config.EventQueue,
		"event.*",
		c.config.Exchange,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to bind event queue: %w", err)
	}

	err = channel.QueueBind(
		c.config.DeviceUpdateQueue,
		"device.*",
		c.config.Exchange,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to bind device update queue: %w", err)
	}

	err = channel.Qos(c.config.PrefetchCount, 0, false)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	c.conn = conn
	c.channel = channel

	log.Println("Connected to RabbitMQ")

	return nil
}

func (c *Client) Channel() *amqp.Channel {
	return c.channel
}

func (c *Client) IsConnected() bool {
	return c.conn != nil && !c.conn.IsClosed()
}

func (c *Client) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	if c.channel == nil {
		return fmt.Errorf("channel is not available")
	}

	return c.channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
}

func (c *Client) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	if c.channel == nil {
		return nil, fmt.Errorf("channel is not available")
	}

	return c.channel.Consume(
		queue,
		consumer,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

func (c *Client) Close() error {
	c.closed = true
	channel := c.channel
	conn := c.conn

	var errs []error
	if channel != nil {
		if err := channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close channel: %w", err))
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (c *Client) HealthCheck(ctx context.Context) error {
	if !c.IsConnected() {
		return fmt.Errorf("RabbitMQ connection is not available")
	}
	return nil
}
