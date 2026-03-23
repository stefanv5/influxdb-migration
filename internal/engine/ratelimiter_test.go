package engine

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_Allow(t *testing.T) {
	limiter := NewRateLimiter(100, 10)

	if !limiter.Allow(5) {
		t.Error("Expected to allow 5 points when burst is 10")
	}

	limiter.mu.Lock()
	if limiter.tokens != 5 {
		t.Errorf("Expected 5 tokens remaining, got %f", limiter.tokens)
	}
	limiter.mu.Unlock()
}

func TestRateLimiter_Deny(t *testing.T) {
	limiter := NewRateLimiter(1, 5)

	limiter.Allow(5)

	if limiter.Allow(1) {
		t.Error("Expected to deny 1 point when tokens are exhausted")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	limiter := NewRateLimiter(1000, 5)

	limiter.Allow(5)

	time.Sleep(10 * time.Millisecond)

	if limiter.Allow(1) {
		t.Log("Tokens were refilled as expected")
	} else {
		t.Error("Expected to allow after refill")
	}
}

func TestRateLimiter_Burst(t *testing.T) {
	limiter := NewRateLimiter(1, 100)

	if !limiter.Allow(100) {
		t.Error("Expected to allow burst of 100")
	}

	if limiter.Allow(1) {
		t.Error("Expected to deny after burst exhausted")
	}
}

func TestRateLimiter_CapAtBurst(t *testing.T) {
	limiter := NewRateLimiter(10000, 50)

	limiter.Allow(50)

	limiter.mu.Lock()
	if limiter.tokens > 50 {
		t.Errorf("Expected tokens to be capped at 50, got %f", limiter.tokens)
	}
	limiter.mu.Unlock()
}

func TestRateLimiter_Concurrent(t *testing.T) {
	limiter := NewRateLimiter(1000, 100)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				limiter.Allow(1)
			}
		}()
	}

	wg.Wait()
}

func TestRateLimiter_Wait(t *testing.T) {
	limiter := NewRateLimiter(100, 5)

	limiter.Allow(5)

	done := make(chan bool)
	go func() {
		limiter.Wait(1)
		done <- true
	}()

	select {
	case <-done:
		t.Log("Wait completed")
	case <-time.After(100 * time.Millisecond):
		t.Error("Wait timed out unexpectedly")
	}
}

func TestRateLimiter_WaitWithDeadline(t *testing.T) {
	limiter := NewRateLimiter(100, 5)

	limiter.Allow(5)

	deadline := time.Now().Add(10 * time.Millisecond)
	err := limiter.WaitWithDeadline(1, deadline)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestRateLimiter_WaitWithDeadline_Timeout(t *testing.T) {
	limiter := NewRateLimiter(1, 1)

	limiter.Allow(1)

	deadline := time.Now().Add(1 * time.Millisecond)
	err := limiter.WaitWithDeadline(1, deadline)

	if err != ErrRateLimitExceeded {
		t.Errorf("Expected ErrRateLimitExceeded, got %v", err)
	}
}
