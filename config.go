package idx

import "time"

// RateLimitConfig configures the token-bucket rate limiter applied to a
// provider's outgoing RPC calls.
type RateLimitConfig struct {
	Enabled bool
	RPS     float64
	Burst   int
}

// CircuitBreakerConfig configures the circuit breaker guarding a provider.
// FilterErrors lists additional substrings (matched case-insensitively
// against the error message) that should count as a breaker-tripping
// failure, on top of the built-in set (429, 401, unauthorized, forbidden,
// rate limit, timeout, connection refused, context deadline).
type CircuitBreakerConfig struct {
	Enabled          bool
	Threshold        int
	Timeout          time.Duration
	HalfOpenMaxCalls int
	FilterErrors     []string
}

// RetryConfig configures the retry/backoff behaviour for a provider's RPC calls.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      bool
}

// CreditsConfig models providers that meter usage in "compute units" per
// call (e.g. Alchemy) rather than, or in addition to, a flat request count.
type CreditsConfig struct {
	Enabled    bool
	PerRequest int64
	Limit      int64
	Period     time.Duration
}

// QuotaConfig bounds how many requests (and, optionally, credits) a
// provider may serve within a rolling period. A zero Limit means unlimited.
type QuotaConfig struct {
	Limit   int64
	Period  time.Duration
	Credits *CreditsConfig
}

// ProviderConfig describes a single upstream RPC endpoint.
type ProviderConfig struct {
	Name           string
	URL            string
	APIKey         string
	Priority       int
	Quota          QuotaConfig
	RateLimit      RateLimitConfig
	CircuitBreaker CircuitBreakerConfig
	Retry          RetryConfig

	// RequestTimeout bounds a single physical RPC attempt (one per retry, so
	// a short timeout doesn't starve MaxAttempts — see Provider.withRetry).
	// Applied via context.WithTimeout around the call passed to the
	// underlying EthClient. Zero disables it (the caller's context, if any
	// deadline at all, is the only bound) — matching how go-ethereum's
	// ethclient itself has no built-in per-request timeout. Not applied to
	// SubscribeNewHead, which is a long-lived subscription, not a
	// request/response call.
	RequestTimeout time.Duration

	// Dial optionally overrides how the provider establishes its RPC
	// client. Defaults to dialing URL (+APIKey) over JSON-RPC via
	// go-ethereum's ethclient. Tests inject a fake EthClient through this.
	Dial DialFunc

	// MaxLogBlockRange overrides PoolConfig.MaxLogBlockRange for this one
	// provider — set it when a provider's own eth_getLogs range limit
	// differs from the rest of the pool (e.g. a higher-tier provider that
	// accepts a much larger range than the others). Zero (the default)
	// means "no override": Pool.MaxLogBlockRange falls back to the
	// pool-wide value whenever this provider is the one currently active.
	MaxLogBlockRange int
}

// PoolConfig configures a Pool of providers.
type PoolConfig struct {
	Providers []ProviderConfig

	// UpdateInterval controls how often the pool recalculates provider
	// health scores. Defaults to 30s if zero.
	UpdateInterval time.Duration

	// MaxLogBlockRange bounds how many blocks a single eth_getLogs call may
	// span when the indexer batches a backfill. It's the pool-wide default,
	// used for any provider that doesn't set its own
	// ProviderConfig.MaxLogBlockRange — see that field to give a specific
	// provider a different limit (e.g. a higher-tier provider that accepts
	// a much larger range than the rest of the pool). Must be a positive
	// number of blocks; defaults to DefaultMaxLogBlockRange (1, i.e. no
	// batching benefit) if zero or negative, so opting into real batching
	// is a deliberate choice.
	MaxLogBlockRange int

	// MaxParallelJobAttempts bounds how many times a single sub-range job
	// may be retried (against any available provider, not just the one that
	// first failed it) during one Pool.LogsByBlockRangeParallel round before
	// that job — and the whole round — is given up on. Guards against a
	// sub-range that every provider rejects for a reason that doesn't trip
	// its circuit breaker or exhaust its quota (so it keeps looking
	// "available" and keeps being handed the job) bouncing between
	// providers forever. Zero or negative (the default) means "use twice
	// the number of providers taking part in that round" — i.e. every
	// provider gets on average two independent chances at any given job
	// before it's abandoned — recomputed per round since availability
	// changes over time. Set this explicitly only if that default is wrong
	// for your setup (e.g. many low-priority providers you'd rather fail
	// fast past than cycle through).
	MaxParallelJobAttempts int

	// RangeTooLargeFilters lists additional substrings (matched
	// case-insensitively against the error message, on top of always
	// requiring "range" to appear somewhere in it) that mark an
	// eth_getLogs error as "the provider rejected this range as too large",
	// merged with the package's built-in keyword set — the same
	// merge-with-defaults pattern as CircuitBreakerConfig.FilterErrors. Use
	// this for a provider whose wording the built-in set misses (e.g.
	// dRPC's free-plan limit reads "ranges over 10000 blocks are not
	// supported on free plan" — no "large"/"limit"/"exceed"/"too many"
	// keyword, so add "not supported" and/or "free plan" here). Recognizing
	// this error is what lets Indexer.syncBlocksBatched split the chunk and
	// retry instead of failing the whole sync; see isRangeTooLargeError and
	// RangeTooLargeClassifier.
	RangeTooLargeFilters []string
}

// IndexerConfig configures an Indexer's polling behaviour. It carries no
// provider/storage wiring — those are supplied to NewIndexer directly as a
// ProviderPool, a ChainStorage and a BlockchainEventDispatcher, so any
// implementation can be plugged in.
//
// Whether a sync uses the batched eth_getLogs path or the sequential,
// one-block-at-a-time path isn't configured here — it's derived entirely
// from PoolConfig.MaxLogBlockRange: 1 (the default) means sequential,
// anything greater means batched.
type IndexerConfig struct {
	Chain         string
	StartBlock    uint64
	BlockInterval time.Duration

	// MaxConsecutiveProviderExhaustion is how many consecutive sync attempts
	// may find no available provider before OnExhausted (see WithOnExhausted)
	// is invoked. Defaults to 5 if zero.
	MaxConsecutiveProviderExhaustion int

	// ProviderExhaustionRestartDelay is how long the sync loop blocks before
	// invoking OnExhausted once MaxConsecutiveProviderExhaustion is hit.
	// Defaults to 15s if zero.
	ProviderExhaustionRestartDelay time.Duration

	// DebugMode re-processes from genesis every tick instead of resuming
	// from the persisted last block. Intended for local development only.
	DebugMode bool

	// ReorgDepth opts into reorg detection beyond a full chain reset: the
	// Indexer keeps the last ReorgDepth processed block hashes in memory
	// and, each sync, re-verifies the chain's current hash at lastBlock
	// still matches. A mismatch means a reorg happened at or below
	// lastBlock (same height or a few blocks deep, not just a full
	// devnet-style reset) — the Indexer rewinds up to ReorgDepth blocks and
	// reprocesses them, relying on idempotent listeners for correctness.
	// Zero (the default) disables this check entirely, preserving prior
	// behaviour. The tracked hashes are in-memory only, not persisted, so
	// this only protects a continuously running process — a fresh restart
	// has nothing to compare against until it has processed ReorgDepth more
	// blocks.
	ReorgDepth int
}
