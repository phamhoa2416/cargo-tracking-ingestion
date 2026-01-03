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
	config *config.RabbitMQConfig

	conn           *amqp.Connection
	pubChannel     *amqp.Channel
	consumeChannel *amqp.Channel

	mu           sync.RWMutex
	reconnecting bool
}

func NewClient(config *config.RabbitMQConfig) (*Client, error) {
	client := &Client{
		config: config,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	go client.handleReconnect()
	return client, nil
}

func (c *Client) connect() error {
	log.Println("Connecting to RabbitMQ...")

	conn, err := amqp.Dial(c.config.URL)
	if err != nil {
		return fmt.Errorf("rabbitmq dial failed: %w", err)
	}

	pubCh, err := conn.Channel()
	if err != nil {
		err := conn.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to open publish channel: %w", err)
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		err := pubCh.Close()
		if err != nil {
			return err
		}
		err = conn.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("open consume channel failed: %w", err)
	}

	if err := c.setupTopology(pubCh); err != nil {
		_ = pubCh.Close()
		_ = consumeCh.Close()
		_ = conn.Close()
		return err
	}

	if err := consumeCh.Qos(c.config.PrefetchCount, 0, false); err != nil {
		_ = pubCh.Close()
		_ = consumeCh.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.pubChannel = pubCh
	c.consumeChannel = consumeCh
	c.mu.Unlock()

	log.Println("RabbitMQ connected successfully")
	return nil
}

func (c *Client) setupTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		c.config.Exchange,
		"topic",
		c.config.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		c.config.EventQueue,
		c.config.Durable,
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare event queue: %w", err)
	}

	if _, err := ch.QueueDeclare(
		c.config.DeviceUpdateQueue,
		c.config.Durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare device update queue: %w", err)
	}

	if err := ch.QueueBind(c.config.EventQueue, "event.*", c.config.Exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind event queue: %w", err)
	}

	if err := ch.QueueBind(c.config.DeviceUpdateQueue, "device.*", c.config.Exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind device update queue: %w", err)
	}

	return nil
}

func (c *Client) handleReconnect() {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			time.Sleep(3 * time.Second)
			continue
		}

		notifyClose := make(chan *amqp.Error, 1)
		conn.NotifyClose(notifyClose)

		err := <-notifyClose
		if err == nil {
			return
		}

		log.Printf("RabbitMQ connection lost: %v. Starting reconnection...", err)

		for attempt := 0; attempt < 10; attempt++ {
			if c.isClosed() {
				return
			}

			backoff := time.Duration(1<<uint(attempt))*500*time.Millisecond + 2*time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff)

			if err := c.connect(); err == nil {
				log.Println("RabbitMQ reconnected successfully")
				break
			} else {
				log.Printf("Reconnect attempt %d failed: %v", attempt+1, err)
			}
		}

		if !c.isClosed() {
			log.Println("Failed to reconnect after multiple attempts. Will retry later...")
		}
	}
}

func (c *Client) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	c.mu.RLock()
	ch := c.pubChannel
	c.mu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return fmt.Errorf("publish channel not available (connection may be down)")
	}

	return ch.PublishWithContext(ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
}

func (c *Client) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	c.mu.RLock()
	ch := c.consumeChannel
	c.mu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return nil, fmt.Errorf("consume channel not available")
	}

	return ch.Consume(
		queue,
		consumer,
		false, // auto-ack = false → cần ack thủ công
		false,
		false,
		false,
		nil,
	)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error

	if c.pubChannel != nil {
		if err := c.pubChannel.Close(); err != nil {
			errs = append(errs, err)
		}
		c.pubChannel = nil
	}

	if c.consumeChannel != nil {
		if err := c.consumeChannel.Close(); err != nil {
			errs = append(errs, err)
		}
		c.consumeChannel = nil
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		c.conn = nil
	}

	if len(errs) > 0 {
		log.Printf("Errors during RabbitMQ close: %v", errs)
		return fmt.Errorf("errors during close: %v", errs)
	}

	log.Println("RabbitMQ client closed gracefully")
	return nil
}

func (c *Client) ConnectionCheck(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil || c.conn.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	if c.pubChannel == nil || c.pubChannel.IsClosed() {
		return fmt.Errorf("publish channel is unavailable")
	}
	if c.consumeChannel == nil || c.consumeChannel.IsClosed() {
		return fmt.Errorf("consume channel is unavailable")
	}
	return nil
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn == nil
}
