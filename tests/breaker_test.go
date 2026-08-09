package idx_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestCircuitBreaker_DisabledAlwaysExecutes(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{Enabled: false})

	wantErr := errors.New("boom")
	for i := 0; i < 10; i++ {
		if err := cb.Execute(func() error { return wantErr }); !errors.Is(err, wantErr) {
			t.Fatalf("expected error to pass through unchanged, got %v", err)
		}
	}
	if !cb.IsAvailable() {
		t.Fatal("a disabled breaker should always report available")
	}
}

func TestCircuitBreaker_IgnoresFilteredOutErrors(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Enabled:          true,
		Threshold:        3,
		Timeout:          time.Minute,
		HalfOpenMaxCalls: 1,
	})

	// "not found" style errors aren't in the failure filter set, so they
	// should never count towards the trip threshold no matter how many occur.
	for i := 0; i < 20; i++ {
		_ = cb.Execute(func() error { return errors.New("not found") })
	}
	if !cb.IsAvailable() {
		t.Fatal("breaker should not trip on filtered-out errors")
	}
}

func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Enabled:          true,
		Threshold:        3,
		Timeout:          time.Minute,
		HalfOpenMaxCalls: 1,
	})

	// "timeout" is a default filtered failure; three in a row is a 100%
	// failure ratio, crossing both the count and ratio trip conditions.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return errors.New("request timeout") })
	}

	if cb.IsAvailable() {
		t.Fatal("expected breaker to be open (unavailable) after crossing the failure threshold")
	}
	if cb.State() != gobreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_SuccessDoesNotTrip(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Enabled:   true,
		Threshold: 1,
		Timeout:   time.Minute,
	})

	for i := 0; i < 20; i++ {
		if err := cb.Execute(func() error { return nil }); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if !cb.IsAvailable() {
		t.Fatal("breaker should remain available after only successes")
	}
}

func TestCircuitBreaker_CustomFilterErrors(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Enabled:          true,
		Threshold:        1,
		Timeout:          time.Minute,
		HalfOpenMaxCalls: 1,
		FilterErrors:     []string{"custom upstream failure"},
	})

	_ = cb.Execute(func() error { return errors.New("custom upstream failure") })

	if cb.IsAvailable() {
		t.Fatal("expected a custom filtered error to trip the breaker")
	}
}
