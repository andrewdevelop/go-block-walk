package idx

import (
	"errors"
	"strings"
)

// isLocalDecodeError reports whether err is caused by go-ethereum being
// unable to decode a transaction type locally (e.g. a chain upgrade
// introducing a tx type — such as EIP-4844 blobs — ahead of the
// go-ethereum version this binary was built with), rather than a
// provider-side failure. Callers use this to distinguish "the provider is
// fine, we just can't parse this block yet" from a real fault: Pool skips
// penalizing the provider's health score for it, and Indexer skips the
// block (logging a warning) instead of treating it as a transient error to
// retry forever.
//
// Kept as a single shared classifier — Pool and Indexer used to each have
// their own copy of this exact substring check, risking the two silently
// diverging.
func isLocalDecodeError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "transaction type not supported")
}

// isRangeTooLargeError reports whether err indicates the RPC provider that
// served an eth_getLogs call rejected it because the requested block range
// exceeded what that specific provider allows (wording varies: "range too
// large", "block range exceeds ...", "range limit exceeded", "too many
// blocks in range", ...). This can happen even when the caller sized the
// chunk from Pool.MaxLogBlockRange, because the provider that actually ends
// up serving the request is resolved independently (and can differ, e.g.
// under a failover racing the chunk-sizing call) — see
// Indexer.syncBlocksBatched, which reacts to this by splitting the chunk
// and retrying instead of failing the whole sync.
func isRangeTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "range") {
		return false
	}
	for _, kw := range []string{"large", "limit", "exceed", "too many"} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// ErrAlreadyRunning is returned by Indexer.Start if the indexer is already running.
var ErrAlreadyRunning = errors.New("idx: indexer already running")

// ErrIndexerStopped is returned by Indexer.Start once Stop has been called.
// An Indexer is one-shot: Stop permanently closes its pool and dispatcher,
// so there's nothing left for a subsequent Start to resume — build a new
// Indexer (and Pool) instead of restarting this one.
var ErrIndexerStopped = errors.New("idx: indexer already stopped")

// ErrNoAvailableProvider is returned by ProviderPool RPC calls when no
// provider in the pool can currently serve the request — every provider is
// unhealthy, circuit-broken, or quota-exhausted.
var ErrNoAvailableProvider = errors.New("idx: no available provider")

// ErrQuotaExceeded is returned by a Provider's RPC methods when its request
// quota (see QuotaConfig) is currently exhausted. It is not retried.
var ErrQuotaExceeded = errors.New("idx: provider quota exceeded")

// ErrRetriesExhausted is returned once a provider's configured
// RetryConfig.MaxAttempts have all been spent on a retryable error without
// success. Check errors.Is(err, ErrRetriesExhausted) to distinguish "we
// tried and every attempt failed" from other failure modes (e.g. a
// non-retryable error, which is returned unwrapped).
var ErrRetriesExhausted = errors.New("idx: retry attempts exhausted")

// ErrProviderPoolExhausted is the domain signal that every provider in the
// pool has been unavailable (down, circuit-broken, or quota/retries
// exhausted — see ErrNoAvailableProvider and ErrRetriesExhausted) for
// IndexerConfig.MaxConsecutiveProviderExhaustion consecutive sync attempts
// in a row. Unlike a single failed sync, this means the pool is unlikely to
// recover on its own by simply waiting for the next tick: it's the cue for
// the host application to restart, e.g. exit the process and let a
// container orchestrator restart it with fresh circuit breakers. See
// WithOnExhausted.
var ErrProviderPoolExhausted = errors.New("idx: provider pool exhausted")
