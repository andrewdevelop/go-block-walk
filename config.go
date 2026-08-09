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
	Name             string
	URL              string
	APIKey           string
	Priority         int
	MaxLogBlockRange int
	Quota            QuotaConfig
	RateLimit        RateLimitConfig
	CircuitBreaker   CircuitBreakerConfig
	Retry            RetryConfig

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
}

// IndexerConfig configures an Indexer's polling behaviour. It carries no
// provider/storage wiring — those are supplied to NewIndexer directly as a
// ProviderPool, a Storage and a BlockchainEventDispatcher, so any
// implementation can be plugged in.
type IndexerConfig struct {
	Chain             string
	StartBlock        uint64
	BlockInterval     time.Duration
	BatchLagThreshold uint64

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
}
