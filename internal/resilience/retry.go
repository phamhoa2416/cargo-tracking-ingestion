package resilience

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type RetryConfig struct {
	MaxAttempts       int
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	BackoffMultiplier float64
	Jitter            bool
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       4,
		InitialDelay:      200 * time.Millisecond,
		MaxDelay:          10 * time.Second,
		BackoffMultiplier: 2.0,
		Jitter:            true,
	}
}

// Do execute a function with retry logic using exponential backoff
func Do(ctx context.Context, config RetryConfig, fn func() error) error {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 1
	}
	var lastErr error

	for attempt := 0; attempt < config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < config.MaxAttempts-1 {
			delay := calculateDelay(config, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return fmt.Errorf("retry failed after %d attempts: %w", config.MaxAttempts, lastErr)
}

func calculateDelay(config RetryConfig, attempt int) time.Duration {
	exp := math.Pow(config.BackoffMultiplier, float64(attempt))
	delay := float64(config.InitialDelay) * exp

	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}

	if config.Jitter {
		jitter := delay * 0.5 * rand.Float64()
		delay = delay*0.75 + jitter
	}

	return time.Duration(delay)
}
