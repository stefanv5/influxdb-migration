package engine

import (
	"errors"
	"sync"
	"time"
)

var ErrRateLimitExceeded = errors.New("rate limit exceeded")

type RateLimiter struct {
	mu       sync.Mutex
	rate     float64
	burst    int
	tokens   float64
	lastTime time.Time
}

func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastTime: time.Now(),
	}
}

func (r *RateLimiter) Allow(points int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastTime).Seconds()
	r.tokens += elapsed * r.rate

	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}

	r.lastTime = now

	if r.tokens >= float64(points) {
		r.tokens -= float64(points)
		return true
	}

	return false
}

func (r *RateLimiter) Wait(points int) {
	for !r.Allow(points) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *RateLimiter) WaitWithDeadline(points int, deadline time.Time) error {
	for !r.Allow(points) {
		if time.Now().After(deadline) {
			return ErrRateLimitExceeded
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}
