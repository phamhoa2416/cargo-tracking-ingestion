package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type limiterEntry struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

type RateLimiter struct {
	limiters map[string]*limiterEntry
	mu       sync.RWMutex
	rate     rate.Limit
	burst    int
	maxSize  int // Maximum number of limiters to keep
}

func NewRateLimiter(requestsPerSecond, burst int) *RateLimiter {
	rl := &RateLimiter{
		limiters: make(map[string]*limiterEntry),
		rate:     rate.Limit(requestsPerSecond),
		burst:    burst,
		maxSize:  10000, // Default max 10k unique limiters
	}

	go rl.cleanup()

	return rl
}

func (rl *RateLimiter) getLimiter(key string) *rate.Limiter {
	now := time.Now()

	rl.mu.RLock()
	entry, exists := rl.limiters[key]
	rl.mu.RUnlock()

	if exists {
		// Update last access time
		rl.mu.Lock()
		entry.lastAccess = now
		rl.mu.Unlock()
		return entry.limiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock
	entry, exists = rl.limiters[key]
	if !exists {
		// Check if we need to evict before adding new entry
		if len(rl.limiters) >= rl.maxSize {
			rl.evictLRU()
		}

		limiter := rate.NewLimiter(rl.rate, rl.burst)
		entry = &limiterEntry{
			limiter:    limiter,
			lastAccess: now,
		}
		rl.limiters[key] = entry
	}

	return entry.limiter
}

// evictLRU removes the least recently used limiter
func (rl *RateLimiter) evictLRU() {
	if len(rl.limiters) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, entry := range rl.limiters {
		if first || entry.lastAccess.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.lastAccess
			first = false
		}
	}

	if oldestKey != "" {
		delete(rl.limiters, oldestKey)
	}
}

func (rl *RateLimiter) Limit() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.ClientIP()
		if userID, exists := c.Get("user_id"); exists {
			key = userID.(string)
		}

		limiter := rl.getLimiter(key)
		if !limiter.Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}

		c.Next()
	}
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		keysToDelete := make([]string, 0)
		now := time.Now()

		for key, entry := range rl.limiters {
			// Remove inactive limiters (full tokens and not accessed in last 5 minutes)
			if entry.limiter.Tokens() >= float64(rl.burst) &&
				now.Sub(entry.lastAccess) > 5*time.Minute {
				keysToDelete = append(keysToDelete, key)
			}
		}

		for _, key := range keysToDelete {
			delete(rl.limiters, key)
		}

		// If still over max size, evict LRU
		if len(rl.limiters) > rl.maxSize {
			for len(rl.limiters) > rl.maxSize {
				rl.evictLRU()
			}
		}

		rl.mu.Unlock()
	}
}
