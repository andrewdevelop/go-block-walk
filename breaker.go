package idx

import (
	"strings"

	"github.com/sony/gobreaker"
)

// CircuitBreaker wraps sony/gobreaker with an Enabled flag and a
// configurable set of error substrings that count as breaker-tripping
// failures (as opposed to e.g. an application-level "not found" that
// shouldn't punish the provider). Safe for concurrent use.
type CircuitBreaker struct {
	cb      *gobreaker.CircuitBreaker
	enabled bool
	filters map[string]bool
}

var defaultBreakerFilters = []string{
	"429", "401", "unauthorized", "rate limit", "forbidden",
	"context deadline", "connection refused", "timeout",
	// 5xx / server-side failures: without these, IsSuccessful treats them as
	// "not a filtered failure" and gobreaker never counts them, so a
	// provider returning nothing but 500s burns retries forever without
	// ever tripping the breaker or losing health score.
	"500", "502", "503", "504",
	"internal error", "internal server error",
	"service unavailable", "bad gateway", "gateway timeout",
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	filters := make(map[string]bool, len(defaultBreakerFilters)+len(cfg.FilterErrors))
	for _, f := range defaultBreakerFilters {
		filters[f] = true
	}
	for _, f := range cfg.FilterErrors {
		filters[strings.ToLower(f)] = true
	}

	settings := gobreaker.Settings{
		Name:        "rpc-provider",
		MaxRequests: uint32(cfg.HalfOpenMaxCalls),
		Interval:    cfg.Timeout,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests == 0 {
				return false
			}
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return int(counts.TotalFailures) >= cfg.Threshold && failureRatio >= 0.5
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			return !isFilteredFailure(err, filters)
		},
	}

	return &CircuitBreaker{
		cb:      gobreaker.NewCircuitBreaker(settings),
		enabled: cfg.Enabled,
		filters: filters,
	}
}

func isFilteredFailure(err error, filters map[string]bool) bool {
	errStr := strings.ToLower(err.Error())
	for substr := range filters {
		if strings.Contains(errStr, substr) {
			return true
		}
	}
	return false
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.enabled {
		return fn()
	}

	_, err := cb.cb.Execute(func() (interface{}, error) {
		return nil, fn()
	})
	return err
}

func (cb *CircuitBreaker) State() gobreaker.State {
	return cb.cb.State()
}

func (cb *CircuitBreaker) IsAvailable() bool {
	if !cb.enabled {
		return true
	}
	return cb.State() != gobreaker.StateOpen
}
