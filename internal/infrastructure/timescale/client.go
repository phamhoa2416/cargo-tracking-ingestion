package timescale

import (
	"cargo-tracking-ingestion/internal/config"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	pool *pgxpool.Pool
}

func NewClient(config *config.DatabaseConfig) (*Client, error) {
	poolConfig, err := pgxpool.ParseConfig(config.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse database DSN failed: %w", err)
	}

	poolConfig.MaxConns = int32(config.MaxOpenConns)
	if config.MaxIdleConns > 0 {
		if config.MaxIdleConns > config.MaxOpenConns {
			poolConfig.MinConns = int32(config.MaxOpenConns)
		} else {
			poolConfig.MinConns = int32(config.MaxIdleConns)
		}
	}
	poolConfig.MaxConnLifetime = config.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.ConnMaxIdleTime
	poolConfig.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	return &Client{pool: pool}, nil
}

func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

func (c *Client) HealthCheck(ctx context.Context) error {
	if c.pool == nil {
		return fmt.Errorf("database pool not initialized")
	}
	return c.pool.Ping(ctx)
}
