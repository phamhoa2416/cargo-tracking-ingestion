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
	FailureThreshold int           // Number of failures before opening
	SuccessThreshold int           // Number of successes in half-open to close
	Timeout          time.Duration // Time to wait before attempting half-open
	MaxRequests      int           // Max requests in half-open state
}

func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		MaxRequests:      3,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config       Config
	state        State
	failures     int
	successes    int
	halfOpenReqs int
	lastFailure  time.Time
	mu           sync.RWMutex
}

func New(config Config) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  StateClosed,
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	cb.mu.Lock()

	// Check if we should transition from open to half-open
	if cb.state == StateOpen {
		if time.Since(cb.lastFailure) >= cb.config.Timeout {
			cb.state = StateHalfOpen
			cb.halfOpenReqs = 0
			cb.successes = 0
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker half-open: max requests reached")
		}
	}

	// Check half-open request limit
	if cb.state == StateHalfOpen {
		if cb.halfOpenReqs >= cb.config.MaxRequests {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker half-open request limit reached")
		}
		cb.halfOpenReqs++
	}

	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.onFailure()
		return err
	}

	cb.onSuccess()
	return nil
}

func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == StateHalfOpen {
		// Transition back to open on failure in half-open
		cb.state = StateOpen
		cb.halfOpenReqs = 0
		cb.successes = 0
	} else if cb.state == StateClosed && cb.failures >= cb.config.FailureThreshold {
		// Transition to open if threshold reached
		cb.state = StateOpen
		cb.failures = 0
	}
}

func (cb *CircuitBreaker) onSuccess() {
	if cb.state == StateHalfOpen {
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			// Transition to closed
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
			cb.halfOpenReqs = 0
		}
	} else if cb.state == StateClosed {
		// Reset failure count on success
		cb.failures = 0
	}
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
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenReqs = 0
}

func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}
