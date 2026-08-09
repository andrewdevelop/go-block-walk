package idx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/andrewdevelop/go-block-walk"
)

func noLimiter() *RateLimiter    { return NewRateLimiter(RateLimitConfig{Enabled: false}) }
func noBreaker() *CircuitBreaker { return NewCircuitBreaker(CircuitBreakerConfig{Enabled: false}) }

// TestWithRetry_ClassifiesErrorsForRetry exercises WithRetry's error
// classification (idx's unexported isRetryable) indirectly: for each error
// message, it asserts whether fn got called more than once (retried) or
// exactly once (treated as non-retryable / fatal).
func TestWithRetry_ClassifiesErrorsForRetry(t *testing.T) {
	cases := []struct {
		name        string
		errMsg      string
		wantRetried bool
	}{
		{"429", "429 too many requests", true},
		{"rate limit", "rate limit exceeded", true},
		{"timeout", "i/o timeout", true},
		{"connection refused", "dial tcp: connection refused", true},
		{"unauthorized wins over retryable", "401 unauthorized: rate limit info", false},
		{"forbidden", "403 forbidden", false},
		{"unrelated error", "execution reverted", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			_ = WithRetry(context.Background(), RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond}, func(ctx context.Context) error {
				calls++
				return errors.New(tc.errMsg)
			}, noLimiter(), noBreaker())

			gotRetried := calls > 1
			if gotRetried != tc.wantRetried {
				t.Errorf("%q: got %d call(s) (retried=%v), want retried=%v", tc.errMsg, calls, gotRetried, tc.wantRetried)
			}
		})
	}
}

// TestWithRetry_BackoffGrowsExponentiallyAndCapsAtMaxDelay observes the
// wall-clock gaps between attempts to indirectly verify the exponential
// backoff schedule (idx's unexported calculateBackoff), with generous
// bounds to keep the test robust against scheduler jitter.
func TestWithRetry_BackoffGrowsExponentiallyAndCapsAtMaxDelay(t *testing.T) {
	var timestamps []time.Time
	cfg := RetryConfig{MaxAttempts: 4, BaseDelay: 15 * time.Millisecond, MaxDelay: 35 * time.Millisecond, Jitter: false}

	_ = WithRetry(context.Background(), cfg, func(ctx context.Context) error {
		timestamps = append(timestamps, time.Now())
		return errors.New("timeout")
	}, noLimiter(), noBreaker())

	if len(timestamps) != 4 {
		t.Fatalf("expected 4 attempts, got %d", len(timestamps))
	}

	gaps := []time.Duration{
		timestamps[1].Sub(timestamps[0]), // ~15ms (base)
		timestamps[2].Sub(timestamps[1]), // ~30ms (2x base)
		timestamps[3].Sub(timestamps[2]), // capped at ~35ms
	}
	bounds := [][2]time.Duration{
		{8 * time.Millisecond, 30 * time.Millisecond},
		{20 * time.Millisecond, 55 * time.Millisecond},
		{20 * time.Millisecond, 60 * time.Millisecond},
	}
	for i, gap := range gaps {
		if gap < bounds[i][0] || gap > bounds[i][1] {
			t.Errorf("gap %d: got %v, want within [%v, %v]", i+1, gap, bounds[i][0], bounds[i][1])
		}
	}
}

func TestWithRetry_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 3}, func(ctx context.Context) error {
		calls++
		return nil
	}, noLimiter(), noBreaker())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls)
	}
}

func TestWithRetry_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond}, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("timeout")
		}
		return nil
	}, noLimiter(), noBreaker())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestWithRetry_StopsImmediatelyOnNonRetryableError(t *testing.T) {
	calls := 0
	wantErr := errors.New("401 unauthorized")
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond}, func(ctx context.Context) error {
		calls++
		return wantErr
	}, noLimiter(), noBreaker())

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the original error to be returned unwrapped-comparable, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call for a non-retryable error, got %d", calls)
	}
}

func TestWithRetry_ExhaustsMaxAttempts(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond}, func(ctx context.Context) error {
		calls++
		return errors.New("timeout")
	}, noLimiter(), noBreaker())

	if !errors.Is(err, ErrRetriesExhausted) {
		t.Fatalf("expected ErrRetriesExhausted after exhausting all attempts, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 calls, got %d", calls)
	}
}

func TestWithRetry_ExhaustedErrorWrapsBothSentinelAndCause(t *testing.T) {
	cause := errors.New("timeout: upstream unreachable")
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond}, func(ctx context.Context) error {
		return cause
	}, noLimiter(), noBreaker())

	if !errors.Is(err, ErrRetriesExhausted) {
		t.Fatalf("expected errors.Is(err, ErrRetriesExhausted) to hold, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("expected errors.Is(err, cause) to hold so callers can still inspect the underlying failure, got %v", err)
	}
}

func TestWithRetry_RespectsContextCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	err := WithRetry(ctx, RetryConfig{MaxAttempts: 5, BaseDelay: 50 * time.Millisecond}, func(ctx context.Context) error {
		calls++
		if calls == 1 {
			cancel()
		}
		return errors.New("timeout")
	}, noLimiter(), noBreaker())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call before cancellation interrupted the backoff wait, got %d", calls)
	}
}

func TestWithRetry_ZeroMaxAttemptsStillTriesOnce(t *testing.T) {
	calls := 0
	err := WithRetry(context.Background(), RetryConfig{MaxAttempts: 0}, func(ctx context.Context) error {
		calls++
		return nil
	}, noLimiter(), noBreaker())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected MaxAttempts<=0 to be treated as 1 attempt, got %d calls", calls)
	}
}
