# go-block-walk

A reusable, thread-safe EVM blockchain indexer for Go: a pool of RPC
providers with health scoring, rate limiting, circuit breaking, retries
and quota tracking, feeding a block-by-block (or batched) sync loop that
dispatches decoded logs to your listeners and persists progress through a
small `Storage` interface.

No concrete storage backend is bundled — implement `Storage` against your
own database, or use the included in-memory `Memory` store for tests, demos,
and local development.

The public API is Go package `idx`, imported from module path
`github.com/andrewdevelop/go-block-walk`:

```go
import idx "github.com/andrewdevelop/go-block-walk"
```

## Features

- **Provider pool** (`Pool`) — dials multiple RPC endpoints, ranks them by a
  live health score, and automatically routes around unhealthy providers.
- **Per-provider resilience** — token-bucket rate limiting, circuit
  breaking ([sony/gobreaker](https://github.com/sony/gobreaker)), and
  exponential backoff with jitter, all independently configurable.
- **Quota tracking** — bounds requests and/or "compute unit" style credits
  (e.g. Alchemy) per provider within a rolling period.
- **Indexer** (`Indexer`) — polls for new blocks on a timer or on demand
  (`Nudge`), automatically switching between a low-latency sequential path
  and a chunked `eth_getLogs` batch path once it falls behind by more than
  `BatchLagThreshold` blocks.
- **Pluggable storage** — the `Storage` interface is the only thing you
  need to implement to back the indexer with your own database. `Memory`
  ships a fully thread-safe in-memory implementation.
- **Domain errors** — `ErrNoAvailableProvider`, `ErrRetriesExhausted`,
  `ErrQuotaExceeded`, and `ErrProviderPoolExhausted` are exported sentinels
  you can match with `errors.Is`, instead of parsing error strings.
- **Everything is an interface** — `ProviderPool`, `RPCProvider`,
  `Storage`, `BlockchainListener`, `BlockchainEventDispatcher` — so any
  piece can be swapped or faked independently in your own tests.

## Install

```sh
go get github.com/andrewdevelop/go-block-walk
```

Requires Go 1.25+.

## Quick start

```go
package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	idx "github.com/andrewdevelop/go-block-walk"
)

// myListener handles decoded logs as the indexer discovers them.
type myListener struct{}

func (myListener) HandleLog(l types.Log, blockTimestamp uint64) error {
	log.Printf("log from tx %s at block %d (t=%d)", l.TxHash, l.BlockNumber, blockTimestamp)
	return nil
}

func main() {
	pool := idx.NewPool(idx.PoolConfig{
		Providers: []idx.ProviderConfig{
			{
				Name:     "primary",
				URL:      "https://eth-mainnet.example.com",
				Priority: 1,
				RateLimit: idx.RateLimitConfig{Enabled: true, RPS: 20, Burst: 40},
				CircuitBreaker: idx.CircuitBreakerConfig{
					Enabled: true, Threshold: 5, Timeout: 30 * time.Second, HalfOpenMaxCalls: 1,
				},
				Retry: idx.RetryConfig{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, Jitter: true},
			},
			{
				Name:     "fallback",
				URL:      "https://eth-mainnet.fallback.example.com",
				Priority: 2,
			},
		},
	})
	defer pool.Close()

	storage := idx.NewMemory() // swap for your own Storage implementation in production
	dispatcher := idx.NewEventDispatcher([]idx.BlockchainListener{myListener{}})

	indexer, err := idx.NewIndexer(
		idx.IndexerConfig{
			Chain:             "ethereum",
			StartBlock:        18_000_000,
			BlockInterval:     5 * time.Second,
			BatchLagThreshold: 100,
		},
		pool, storage, dispatcher,
		idx.WithLogger(slog.Default()),
		// Signal for a restart once every provider has been unavailable for
		// several consecutive sync attempts in a row (see "Handling
		// provider pool exhaustion" below).
		idx.WithOnExhausted(func(err error) {
			log.Println(err)
			os.Exit(1) // let your process supervisor / container restart policy handle it
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := indexer.Start(); err != nil {
		log.Fatal(err)
	}
	defer indexer.Stop()

	// Wake the indexer immediately instead of waiting for the next tick,
	// e.g. after your API receives a client's "I just sent a tx" hint.
	indexer.Nudge()

	select {} // run until the process is killed
}
```

## Architecture

```
                 ┌─────────────┐
   RPC calls     │   Provider   │  rate limit → circuit breaker → retry/backoff → quota
  ┌─────────────▶│  (1 per URL) │
  │              └─────────────┘
  │                     ▲
┌─────┐         health score ranking
│ Pool│◀──────────────────┘
└─────┘
  ▲
  │ ProviderPool interface
  │
┌────────┐   BlockchainEventDispatcher   ┌───────────────────┐
│ Indexer│──────────────────────────────▶│  your listeners    │
└────────┘                               └───────────────────┘
  │
  │ Storage interface
  ▼
┌──────────────┐
│ Memory (impl)│  ← or your own Postgres/SQLite/... implementation
└──────────────┘
```

- **`Provider`** wraps a single JSON-RPC endpoint (via an `EthClient`
  interface — satisfied by `*ethclient.Client`, or a fake in tests) with a
  rate limiter, circuit breaker, retry loop and quota manager.
- **`Pool`** holds several `Provider`s, periodically re-ranks them by health
  score (`PoolConfig.UpdateInterval`, default 30s), and always routes each
  call to the best currently-available one.
- **`Indexer`** depends only on the `ProviderPool`, `Storage` and
  `BlockchainEventDispatcher` interfaces — never on `Pool`/`Memory`
  directly — so you can inject fakes in your own tests exactly like this
  module's own test suite does.

## Handling provider pool exhaustion

If the pool has had **no available provider** (every circuit breaker open,
every quota exhausted) for `IndexerConfig.MaxConsecutiveProviderExhaustion`
consecutive sync attempts (default 5), the indexer treats that as unlikely
to self-heal by simply waiting for the next tick. It waits
`ProviderExhaustionRestartDelay` (default 15s), then calls the callback
registered via `idx.WithOnExhausted`, passing an error that wraps
`idx.ErrProviderPoolExhausted`:

```go
idx.WithOnExhausted(func(err error) {
	if errors.Is(err, idx.ErrProviderPoolExhausted) {
		log.Println("indexer: restarting due to provider exhaustion:", err)
		os.Exit(1) // e.g. docker-compose `restart: unless-stopped` brings it back with fresh breakers
	}
})
```

The default callback is a no-op — the loop just keeps retrying on the next
tick — so opting into a restart is a deliberate choice, not baked-in
`os.Exit` behaviour.

## Custom storage backends

Implement the `Storage` interface (see `ports.go`) against your database:

```go
type Storage interface {
	GetLastBlock(ctx context.Context, chain string) (uint64, error)
	SetLastBlock(ctx context.Context, chain string, blockNum uint64) error

	GetProviderScore(ctx context.Context, provider string) (*ProviderScore, error)
	SetProviderScore(ctx context.Context, provider string, score, penalty float64) error
	GetAllProviderScores(ctx context.Context) ([]ProviderScore, error)

	GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error)
	SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error

	SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error
	GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error)
	IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error)

	Close() error
}
```

`SaveIndexedEvent` must be idempotent under a duplicate `(chain,
block_number, log_index)` key — `Memory` treats a duplicate insert as a
silent no-op (mirroring an SQL `ON CONFLICT DO NOTHING` upsert), so a
concurrent-safe SQL backend should do the same.

## Testing

All tests live under [`tests/`](tests/) as a **black-box** suite
(`package idx_test`), exercising only this module's exported API — Go
requires unexported-access ("white-box") tests to live beside the package
they test, so keeping test files in their own directory means testing
through the public surface only, the same way any consumer of this module
would.

```sh
go test ./...                                   # run everything
go test ./... -race                             # with the race detector
go test ./tests/... -coverpkg=./... -cover       # coverage of the idx package itself
```

## Project layout

```
.
├── ports.go        interfaces + data types (RPCProvider, ProviderPool, Storage, ...)
├── config.go        config structs (RateLimitConfig, CircuitBreakerConfig, RetryConfig, ...)
├── errors.go        exported sentinel errors
├── ethclient.go      EthClient interface + default go-ethereum dial
├── provider.go       Provider + QuotaManager
├── pool.go           Pool
├── ratelimit.go       RateLimiter
├── breaker.go        CircuitBreaker
├── retry.go          WithRetry / backoff
├── dispatcher.go       EventDispatcher
├── exhaustion.go       ExhaustionTracker
├── nudge.go          NudgeSignal
├── indexer.go        Indexer (the sync loop)
├── memory.go          Memory (in-memory Storage)
└── tests/            black-box test suite (package idx_test)
```
