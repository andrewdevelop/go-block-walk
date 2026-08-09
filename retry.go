package idx

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

var retryableErrors = []string{
	"429", "rate limit", "timeout", "context deadline",
	"connection refused", "network", "temporary",
}

var nonRetryableErrors = []string{
	"401", "unauthorized", "forbidden",
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	for _, ne := range nonRetryableErrors {
		if strings.Contains(errStr, ne) {
			return false
		}
	}
	for _, re := range retryableErrors {
		if strings.Contains(errStr, re) {
			return true
		}
	}
	return false
}

// WithRetry runs fn, retrying on retryable errors with exponential backoff
// up to cfg.MaxAttempts, subject to limiter and cb. Non-retryable errors and
// a cancelled/expired ctx return immediately.
func WithRetry(ctx context.Context, cfg RetryConfig, fn func(context.Context) error, limiter *RateLimiter, cb *CircuitBreaker) error {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := limiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiter wait failed: %w", err)
		}

		err := cb.Execute(func() error {
			return fn(ctx)
		})
		if err == nil {
			return nil
		}
		lastErr = err

		if !isRetryable(err) {
			return err
		}
		if attempt >= maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(calculateBackoff(attempt, cfg)):
		}
	}

	return fmt.Errorf("%w: %d attempts exceeded, last error: %w", ErrRetriesExhausted, maxAttempts, lastErr)
}

func calculateBackoff(attempt int, cfg RetryConfig) time.Duration {
	delay := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt-1))
	if cfg.MaxDelay > 0 && delay > float64(cfg.MaxDelay) {
		delay = float64(cfg.MaxDelay)
	}
	if cfg.Jitter {
		delay += rand.Float64() * 0.5 * delay
	}
	return time.Duration(delay)
}
