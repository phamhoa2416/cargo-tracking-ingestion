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
	StateHalfOpen
)

type Config struct {
	FailureThreshold uint32        // Number of failures before opening
	SuccessThreshold uint32        // Number of successes in half-open to close
	Timeout          time.Duration // Time to wait before attempting half-open
	MaxRequests      uint32        // Max requests in half-open state
}

func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          60 * time.Second,
		MaxRequests:      1,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config           Config
	state            State
	failureCount     uint32
	successCount     uint32
	halfOpenRequests uint32
	lastFailureTime  time.Time
	mu               sync.RWMutex
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
	switch cb.state {

	case StateOpen:
		if time.Since(cb.lastFailureTime) < cb.config.Timeout {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker is open")
		}
		cb.state = StateHalfOpen
		cb.halfOpenRequests = 0
		cb.successCount = 0

	case StateHalfOpen:
		if cb.halfOpenRequests >= cb.config.MaxRequests {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker half-open: max requests limit reached")
		}
		cb.halfOpenRequests++
	default:
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.lastFailureTime = time.Now()
		if cb.state == StateHalfOpen {
			cb.state = StateOpen
			cb.resetCounters()
		} else if cb.state == StateClosed {
			cb.failureCount++
			if cb.failureCount >= cb.config.FailureThreshold {
				cb.state = StateOpen
				cb.resetCounters()
			}
		}
		return err
	}

	if cb.state == StateHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.state = StateClosed
			cb.resetCounters()
		}
	} else if cb.state == StateClosed {
		cb.failureCount = 0
	}

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
	cb.resetCounters()
	cb.lastFailureTime = time.Time{}
}

func (cb *CircuitBreaker) resetCounters() {
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenRequests = 0
}

func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}
