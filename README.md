# go-block-walk

A reusable, thread-safe EVM blockchain indexer for Go: a pool of RPC
providers with health scoring, rate limiting, circuit breaking, retries
and quota tracking, feeding a block-by-block (or batched) sync loop that
dispatches decoded logs to your listeners and persists progress through a
small `ChainStorage` interface.

No concrete storage backend is bundled — implement `ChainStorage` against
your own database (that alone is a complete Indexer backend — persisting
provider health scores and quota usage is optional, see
[Custom storage backends](#custom-storage-backends)), or use the included
in-memory `Memory` store for tests, demos, and local development.

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
  (`Nudge`). Uses a low-latency, one-block-at-a-time sync path by default;
  set `PoolConfig.MaxLogBlockRange > 1` and it switches to a chunked
  `eth_getLogs` batch path instead — the same setting that bounds chunk
  size also decides which path runs.
- **Pluggable, segregated storage** — `ChainStorage` (sync progress +
  indexed events) is the only thing you need to implement to back the
  indexer with your own database. `ScoreStorage` and `QuotaStorage` (health
  scores / quota bookkeeping) are separate, optional interfaces — the
  Indexer detects them via a type assertion and simply skips persisting
  that data if your storage doesn't implement them. `Memory` implements all
  three.
- **Domain errors** — `ErrNoAvailableProvider`, `ErrRetriesExhausted`,
  `ErrQuotaExceeded`, and `ErrProviderPoolExhausted` are exported sentinels
  you can match with `errors.Is`, instead of parsing error strings.
- **Everything is an interface** — `ProviderPool`, `RPCProvider`,
  `ChainStorage`, `ScoreStorage`, `QuotaStorage`, `BlockchainListener`,
  `BlockchainEventDispatcher` — so any piece can be swapped or faked
  independently in your own tests.

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
		// How many blocks a single eth_getLogs call may span when the
		// indexer batches a backfill, applied to every provider below.
		// Defaults to 1 (no real batching) if left unset.
		MaxLogBlockRange: 2000,
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

	storage := idx.NewMemory() // swap for your own ChainStorage implementation in production
	dispatcher := idx.NewEventDispatcher([]idx.BlockchainListener{myListener{}})

	indexer, err := idx.NewIndexer(
		idx.IndexerConfig{
			Chain:         "ethereum",
			StartBlock:    18_000_000,
			BlockInterval: 5 * time.Second,
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
  │ ChainStorage interface (+ optional ScoreStorage, QuotaStorage)
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
  call to the best currently-available one. It also owns
  `PoolConfig.MaxLogBlockRange` — one eth_getLogs chunk size applied
  uniformly to every provider (default 1); there's no per-provider override.
- **`Indexer`** depends only on the `ProviderPool`, `ChainStorage` and
  `BlockchainEventDispatcher` interfaces — never on `Pool`/`Memory`
  directly — so you can inject fakes in your own tests exactly like this
  module's own test suite does. It persists provider health scores and
  quota usage too, but only if the `ChainStorage` you hand it also happens
  to implement `ScoreStorage`/`QuotaStorage` (detected via a type
  assertion) — neither is required.

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

Persistence is split into three interfaces (see `ports.go`) instead of one
monolithic `Storage`, so implementing a backend only costs you what you
actually use:

```go
// Required — the only thing NewIndexer needs.
type ChainStorage interface {
	GetLastBlock(ctx context.Context, chain string) (uint64, error)
	SetLastBlock(ctx context.Context, chain string, blockNum uint64) error

	SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error
	GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error)
	IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error)

	Close() error
}

// Optional — implement it and the Indexer persists provider health scores
// as a side effect of syncing; skip it and that's simply not tracked.
type ScoreStorage interface {
	GetProviderScore(ctx context.Context, provider string) (*ProviderScore, error)
	SetProviderScore(ctx context.Context, provider string, score, penalty float64) error
	GetAllProviderScores(ctx context.Context) ([]ProviderScore, error)
}

// Optional — same deal, for provider quota usage.
type QuotaStorage interface {
	GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error)
	SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error
}

// Storage = ChainStorage + ScoreStorage + QuotaStorage, provided purely as
// a "my backend does all three" shorthand. Memory implements it.
type Storage interface {
	ChainStorage
	ScoreStorage
	QuotaStorage
}
```

`NewIndexer` takes a `ChainStorage`; at construction it type-asserts that
value against `ScoreStorage` and `QuotaStorage` and persists whichever it
finds. A store backing only `ChainStorage` — no score/quota tracking at
all — is a complete, valid Indexer backend (see
`tests/storage_segregation_test.go`'s `chainOnlyStorage` for a minimal
example). `Pool.PersistQuotaUsage` likewise only asks for `QuotaStorage`.

`Memory` mirrors this split internally rather than being one 200-line type:
it's a thin composition of three independent, independently testable
pieces, each with its own lock —

```go
type Memory struct {
	*MemoryChainStorage
	*MemoryScoreStorage
	*MemoryQuotaStorage
}
```

— so `idx.NewMemoryChainStorage()` alone is a real, usable `ChainStorage`
if that's genuinely all you want in memory (e.g. in a test), without
allocating score/quota bookkeeping you'll never touch.

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

Most files are focused unit tests (one component at a time). One test,
[`tests/integration_test.go`](tests/integration_test.go)'s
`TestIntegration_FullPipeline`, wires up a real `Pool` + `Provider` +
`Memory` + `Indexer` + `EventDispatcher` together (only the JSON-RPC
transport is faked) and drives the whole thing through a realistic
lifecycle in one run: failover past a dead provider, transient errors
recovered by retry, a burst of blocks forcing the chunked batch path,
health-score/quota persistence, and finally a permanently broken provider
tripping its circuit breaker until the pool is exhausted and
`ErrProviderPoolExhausted` fires through `WithOnExhausted`.

```sh
go test ./...                                   # run everything
go test ./... -race                             # with the race detector
go test ./tests/... -coverpkg=./... -cover       # coverage of the idx package itself
go test ./tests/... -run TestIntegration -v      # just the end-to-end pipeline test
```

...or via the [Makefile](Makefile): `make test`, `make test-race`, `make cover`.

### Opt-in: testing against a real Hardhat node

[`tests/hardhat_test.go`](tests/hardhat_test.go) talks to an actual
JSON-RPC node instead of a fake — real dial, real `eth_blockNumber` /
`eth_getLogs` / etc. `TestHardhat_IndexerTracksRealChain` goes further than
just watching the chain head move: it deploys a tiny contract whose init
code unconditionally emits a log, then asserts the `Indexer`'s registered
`BlockchainListener` actually received *that* log (matching `TxHash` and
contract `Address`) — proof the full dispatch pipeline works against a real
node, not just that polling advances. It's built only with `-tags hardhat`,
so it's invisible to `go test ./...` and CI's default run, and skips (not
fails) if no node is reachable — this is an opt-in, manual test, not
something every contributor needs to run.

Start a Hardhat node yourself first (this repo doesn't manage it):

```sh
npx hardhat node   # listens on :8545 by default
```

Then, in another terminal:

```sh
make test-hardhat        # HARDHAT_RPC_URL defaults to http://127.0.0.1:8545
make test-hardhat-race   # same, with -race
make test-all            # the fast suite + the Hardhat suite, both with -race
```

Point it at a different node with `HARDHAT_RPC_URL=http://host:port make test-hardhat`.

## Project layout

```
.
├── ports.go        interfaces + data types (RPCProvider, ProviderPool, ChainStorage, ScoreStorage, QuotaStorage, ...)
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
├── memory.go          Memory (composes the three pieces below into the full Storage)
├── memorychain.go     MemoryChainStorage (in-memory ChainStorage)
├── memoryscore.go     MemoryScoreStorage (in-memory ScoreStorage)
├── memoryquota.go     MemoryQuotaStorage (in-memory QuotaStorage)
├── Makefile          build/test/cover/test-hardhat targets
└── tests/            black-box test suite (package idx_test)
    └── hardhat_test.go  opt-in, real-node test (build tag "hardhat")
```
