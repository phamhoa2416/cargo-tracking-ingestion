package middleware

import (
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
	"net/http"
	"sync"
)

type RateLimiter struct {
	limiters sync.Map
	rate     rate.Limit
	burst    int
}

func NewRateLimiter(requestsPerSecond float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:  rate.Limit(requestsPerSecond),
		burst: burst,
	}
}

func (rl *RateLimiter) Limit() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.ClientIP()

		limiterI, _ := rl.limiters.LoadOrStore(key, rate.NewLimiter(rl.rate, rl.burst))
		limiter := limiterI.(*rate.Limiter)

		if !limiter.Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}

		c.Next()
	}
}
