package idx_test

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestRateLimiter_Disabled(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: false})

	if !rl.Allow() {
		t.Fatal("disabled limiter should always allow")
	}
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("disabled limiter Wait should never error: %v", err)
	}
	if rl.IsEnabled() {
		t.Fatal("expected IsEnabled() == false")
	}
	if rl.Burst() != 0 {
		t.Fatalf("expected Burst() == 0 when disabled, got %d", rl.Burst())
	}
}

func TestRateLimiter_EnabledLimitsBurst(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: true, RPS: 1, Burst: 2})

	if !rl.Allow() {
		t.Fatal("expected first call to be allowed (burst)")
	}
	if !rl.Allow() {
		t.Fatal("expected second call to be allowed (burst)")
	}
	if rl.Allow() {
		t.Fatal("expected third call to be denied once burst is exhausted")
	}
}

func TestRateLimiter_WaitBlocksUntilTokenAvailable(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: true, RPS: 100, Burst: 1})

	// Drain the single burst token.
	if !rl.Allow() {
		t.Fatal("expected first call to be allowed")
	}

	start := time.Now()
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed <= 0 {
		t.Fatalf("expected Wait to block for a positive duration, got %v", elapsed)
	}
}

func TestRateLimiter_WaitRespectsContextCancellation(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: true, RPS: 0.001, Burst: 1})
	rl.Allow() // drain the burst

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := rl.Wait(ctx); err == nil {
		t.Fatal("expected Wait to return an error once the context deadline is exceeded")
	}
}

func TestRateLimiter_DisabledNoOpMethods(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: false})

	if err := rl.WaitN(context.Background(), 5); err != nil {
		t.Fatalf("WaitN: unexpected error %v", err)
	}
	if r := rl.Reserve(); r == nil {
		t.Fatal("Reserve: expected a non-nil (zero-value) reservation")
	}
	if r := rl.ReserveN(time.Now(), 3); r == nil {
		t.Fatal("ReserveN: expected a non-nil (zero-value) reservation")
	}
	if rl.Tokens() != 0 {
		t.Fatalf("Tokens: expected 0 when disabled, got %v", rl.Tokens())
	}
	if rl.Limit() != rate.Inf {
		t.Fatalf("Limit: expected rate.Inf when disabled, got %v", rl.Limit())
	}

	// SetLimit/SetBurst on a disabled limiter must not panic.
	rl.SetLimit(rate.Limit(10))
	rl.SetBurst(10)
}

func TestRateLimiter_EnabledMethods(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{Enabled: true, RPS: 5, Burst: 5})

	if err := rl.WaitN(context.Background(), 2); err != nil {
		t.Fatalf("WaitN: unexpected error %v", err)
	}
	if r := rl.Reserve(); r == nil || !r.OK() {
		t.Fatal("Reserve: expected an OK reservation")
	}
	if r := rl.ReserveN(time.Now(), 1); r == nil {
		t.Fatal("ReserveN: expected a non-nil reservation")
	}
	if rl.Tokens() < 0 {
		t.Fatalf("expected a non-negative token count, got %v", rl.Tokens())
	}

	rl.SetLimit(rate.Limit(100))
	if rl.Limit() != rate.Limit(100) {
		t.Fatalf("expected SetLimit to update Limit(), got %v", rl.Limit())
	}

	rl.SetBurst(42)
	if rl.Burst() != 42 {
		t.Fatalf("expected SetBurst to update Burst(), got %v", rl.Burst())
	}
}
