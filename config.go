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
}

// PoolConfig configures a Pool of providers.
type PoolConfig struct {
	Providers []ProviderConfig

	// UpdateInterval controls how often the pool recalculates provider
	// health scores. Defaults to 30s if zero.
	UpdateInterval time.Duration

	// MaxLogBlockRange bounds how many blocks a single eth_getLogs call may
	// span when the indexer batches a backfill, applied uniformly to every
	// provider in the pool — RPC providers differ in what range they'll
	// actually accept, so pick a value your least permissive provider
	// supports. Must be a positive number of blocks; defaults to
	// DefaultMaxLogBlockRange (1, i.e. no batching benefit) if zero or
	// negative, so opting into real batching is a deliberate choice.
	MaxLogBlockRange int
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
