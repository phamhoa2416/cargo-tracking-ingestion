package rabbitmq

import (
	"cargo-tracking-ingestion/internal/config"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	config         *config.RabbitMQConfig
	reconnectDelay time.Duration

	mu            sync.RWMutex
	closed        bool
	reconnectChan chan struct{}
}

func NewClient(config *config.RabbitMQConfig) (*Client, error) {
	client := &Client{
		config:         config,
		reconnectDelay: 5 * time.Second,
		reconnectChan:  make(chan struct{}),
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	// Start single reconnect handler goroutine
	go client.handleReconnect()

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

	c.mu.Lock()
	c.conn = conn
	c.channel = channel
	c.mu.Unlock()

	log.Println("Connected to RabbitMQ")

	return nil
}

func (c *Client) handleReconnect() {
	for {
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			time.Sleep(c.reconnectDelay)
			continue
		}

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		reason, ok := <-notifyClose
		if !ok {
			c.mu.RLock()
			closed := c.closed
			c.mu.RUnlock()
			if closed {
				return
			}
		}

		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		log.Printf("RabbitMQ connection closed: %v. Reconnecting...", reason)

		// Reconnect loop
		for {
			c.mu.RLock()
			if c.closed {
				c.mu.RUnlock()
				return
			}
			c.mu.RUnlock()

			time.Sleep(c.reconnectDelay)

			err := c.connect()
			if err == nil {
				log.Println("RabbitMQ reconnected successfully")
				c.notifyReconnect()
				break
			}

			log.Printf("RabbitMQ reconnect failed: %v. Retrying...", err)
		}
	}
}

func (c *Client) notifyReconnect() {
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

func (c *Client) ReconnectNotify() <-chan struct{} {
	return c.reconnectChan
}

func (c *Client) Channel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.conn.IsClosed()
}

func (c *Client) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	c.mu.RLock()
	channel := c.channel
	c.mu.RUnlock()

	if channel == nil {
		return fmt.Errorf("channel is not available")
	}

	return channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
}

func (c *Client) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	c.mu.RLock()
	channel := c.channel
	c.mu.RUnlock()

	if channel == nil {
		return nil, fmt.Errorf("channel is not available")
	}

	return channel.Consume(
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
	c.mu.Lock()
	c.closed = true
	channel := c.channel
	conn := c.conn
	c.mu.Unlock()

	// Close reconnect channel
	close(c.reconnectChan)

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
