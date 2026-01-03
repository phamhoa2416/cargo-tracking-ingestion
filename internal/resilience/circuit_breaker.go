package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
)

type Config struct {
	FailureThreshold uint32        // Number of failures before opening
	Timeout          time.Duration // Time to wait before attempting to close
}

func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		Timeout:          60 * time.Second,
	}
}

// CircuitBreaker implements a simplified circuit breaker pattern (Closed/Open only)
type CircuitBreaker struct {
	config          Config
	state           State
	failureCount    uint32
	lastFailureTime time.Time
	mu              sync.RWMutex
}

func New(config Config) *CircuitBreaker {
	if config.FailureThreshold == 0 {
		config.FailureThreshold = DefaultConfig().FailureThreshold
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultConfig().Timeout
	}
	return &CircuitBreaker{
		config: config,
		state:  StateClosed,
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	cb.mu.Lock()
	if cb.state == StateOpen {
		if time.Since(cb.lastFailureTime) < cb.config.Timeout {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker is open")
		}
		cb.state = StateClosed
		cb.failureCount = 0
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.lastFailureTime = time.Now()
		cb.failureCount++
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.state = StateOpen
			cb.failureCount = 0
		}
		return err
	}

	// Success - reset failure count
	cb.failureCount = 0
	return nil
}

func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
	cb.lastFailureTime = time.Time{}
}

func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}
