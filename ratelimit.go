package idx

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter wraps golang.org/x/time/rate behind an Enabled flag so callers
// can configure "no limiting" without special-casing a nil limiter. Safe for
// concurrent use — golang.org/x/time/rate.Limiter is itself goroutine-safe.
type RateLimiter struct {
	limiter *rate.Limiter
	enabled bool
}

func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	rl := &RateLimiter{enabled: cfg.Enabled}
	if cfg.Enabled {
		rl.limiter = rate.NewLimiter(rate.Limit(cfg.RPS), cfg.Burst)
	}
	return rl
}

func (rl *RateLimiter) Allow() bool {
	if !rl.enabled {
		return true
	}
	return rl.limiter.Allow()
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	if !rl.enabled {
		return nil
	}
	return rl.limiter.Wait(ctx)
}

func (rl *RateLimiter) WaitN(ctx context.Context, n int) error {
	if !rl.enabled {
		return nil
	}
	return rl.limiter.WaitN(ctx, n)
}

func (rl *RateLimiter) Reserve() *rate.Reservation {
	if !rl.enabled {
		return &rate.Reservation{}
	}
	return rl.limiter.Reserve()
}

func (rl *RateLimiter) ReserveN(now time.Time, n int) *rate.Reservation {
	if !rl.enabled {
		return &rate.Reservation{}
	}
	return rl.limiter.ReserveN(now, n)
}

func (rl *RateLimiter) SetLimit(r rate.Limit) {
	if rl.enabled {
		rl.limiter.SetLimit(r)
	}
}

func (rl *RateLimiter) SetBurst(burst int) {
	if rl.enabled {
		rl.limiter.SetBurst(burst)
	}
}

func (rl *RateLimiter) Tokens() float64 {
	if !rl.enabled {
		return 0
	}
	return rl.limiter.Tokens()
}

func (rl *RateLimiter) IsEnabled() bool {
	return rl.enabled
}

func (rl *RateLimiter) Burst() int {
	if !rl.enabled {
		return 0
	}
	return rl.limiter.Burst()
}

func (rl *RateLimiter) Limit() rate.Limit {
	if !rl.enabled {
		return rate.Inf
	}
	return rl.limiter.Limit()
}
