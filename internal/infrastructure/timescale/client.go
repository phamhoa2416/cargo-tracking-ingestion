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
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(config.MaxOpenConns)
	poolConfig.MinConns = int32(config.MaxIdleConns)
	poolConfig.MaxConnLifetime = config.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.ConnMaxIdleTime

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
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
	return c.pool.Ping(ctx)
}
