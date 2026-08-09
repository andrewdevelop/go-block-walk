package idx

import "errors"

// ErrAlreadyRunning is returned by Indexer.Start if the indexer is already running.
var ErrAlreadyRunning = errors.New("idx: indexer already running")

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
