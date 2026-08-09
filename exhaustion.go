package idx

// ExhaustionTracker counts consecutive sync attempts where the RPC provider
// pool had no available provider — every circuit breaker open, including
// the last remaining provider — and reports when that streak crosses a
// threshold. Below the threshold the indexer just keeps retrying on the
// next tick; past it, the caller decides what to do (see
// IndexerConfig.MaxConsecutiveProviderExhaustion and WithOnExhausted).
type ExhaustionTracker struct {
	threshold   int
	consecutive int
}

func NewExhaustionTracker(threshold int) *ExhaustionTracker {
	if threshold < 1 {
		threshold = 1
	}
	return &ExhaustionTracker{threshold: threshold}
}

// Observe records one sync attempt's outcome and reports whether the pool
// has now been exhausted for `threshold` consecutive attempts in a row.
func (t *ExhaustionTracker) Observe(providerAvailable bool) bool {
	if providerAvailable {
		t.consecutive = 0
		return false
	}
	t.consecutive++
	return t.consecutive >= t.threshold
}

// Consecutive returns the current streak of attempts with no available provider.
func (t *ExhaustionTracker) Consecutive() int {
	return t.consecutive
}
