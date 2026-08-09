# AGENTS.md

Guidance for coding agents (Claude Code, Copilot, etc.) working in this
repository. See `README.md` for user-facing documentation.

## What this is

`go-block-walk` (Go package `idx`, module
`github.com/andrewdevelop/go-block-walk`) is a reusable, thread-safe EVM
blockchain indexer library: an RPC provider pool with health scoring, rate
limiting, circuit breaking, retries and quota tracking, feeding a
block-by-block sync loop. It ships **no concrete storage backend** —
persistence is split into `ChainStorage` (required), `ScoreStorage` and
`QuotaStorage` (both optional), all interfaces; `Memory` is the only
implementation in this repo (it implements all three, i.e. the composite
`Storage`), and it exists for tests/demos, not production persistence.

## Layout

The package is intentionally **flat** — every source file lives directly in
the repo root as `package idx`, no subpackages:

| File | Contents |
|---|---|
| `ports.go` | All exported interfaces (`RPCProvider`, `ProviderPool`, `ChainStorage`, `ScoreStorage`, `QuotaStorage`, `Storage`, `BlockchainListener`, `BlockchainEventDispatcher`) and their data types |
| `config.go` | Config structs (`RateLimitConfig`, `CircuitBreakerConfig`, `RetryConfig`, `QuotaConfig`, `ProviderConfig`, `PoolConfig`, `IndexerConfig`) |
| `errors.go` | Exported sentinel errors (`ErrNoAvailableProvider`, `ErrQuotaExceeded`, `ErrRetriesExhausted`, `ErrProviderPoolExhausted`, `ErrAlreadyRunning`) |
| `ethclient.go` | `EthClient` interface (subset of `*ethclient.Client`) + `DialFunc`, so RPC calls are mockable in tests |
| `provider.go` | `Provider` (rate limit → circuit breaker → retry → quota per RPC call) + unexported `QuotaManager` |
| `pool.go` | `Pool` — ranks/selects `Provider`s by health score |
| `ratelimit.go`, `breaker.go`, `retry.go` | Token-bucket limiter, circuit breaker, retry/backoff — each independently testable |
| `dispatcher.go` | `EventDispatcher` — fans a log out to listeners, stops at first error |
| `exhaustion.go` | `ExhaustionTracker` — counts consecutive "no provider available" attempts |
| `nudge.go` | `NudgeSignal` — coalescing wake-up channel |
| `indexer.go` | `Indexer` — the sync loop; depends on `ProviderPool`/`ChainStorage`/`BlockchainEventDispatcher` interfaces (plus `ScoreStorage`/`QuotaStorage`, detected optionally via type assertion), never on `Pool`/`Memory` concretely |
| `memory.go` | `Memory` — composes the three pieces below (embedding) into the full `Storage` |
| `memorychain.go` | `MemoryChainStorage` — thread-safe in-memory `ChainStorage`, standalone-usable |
| `memoryscore.go` | `MemoryScoreStorage` — thread-safe in-memory `ScoreStorage`, standalone-usable |
| `memoryquota.go` | `MemoryQuotaStorage` — thread-safe in-memory `QuotaStorage`, standalone-usable |
| `tests/` | The entire test suite (see below) |

**Do not introduce subpackages** (`internal/`, `core/`, `service/`, ...)
without being asked — the flat layout and single `idx` package are a
deliberate choice, not an oversight.

## Tests live in `tests/`, as a black-box suite

Every `_test.go` file is under `tests/`, declares `package idx_test`, and
dot-imports the module (`. "github.com/andrewdevelop/go-block-walk"`) so
tests read without an `idx.` prefix on every identifier.

**This is a hard Go constraint, not a style choice**: a `_test.go` file that
needs access to an unexported identifier (a private func, method, field, or
type) *must* live in the same directory as the package it tests — Go's
tooling has no mechanism for an out-of-directory "internal" test. Since
tests were moved to their own directory, **every test in this repo can only
use `idx`'s exported API**. Concretely:

- Never add a test importing `package idx` (non-`_test` suffix) back at the
  repo root. If you need to test something, expose it (deliberately, as
  real API) or test its externally-observable effect instead.
- When a behavior lives behind an unexported function/method (e.g. a
  private classifier like the old `isUnsupportedTxType`, or an unexported
  method like `Pool.recalculateScores`), test its effect through the public
  surface: drive it via `Start()`/`Nudge()`/`GetLastBlock()` polling
  (`tests/indexer_test.go`'s `waitFor` helper), or via a short
  `PoolConfig.UpdateInterval` and a poll loop (see
  `TestPool_RecalculateScoresAppliesQuotaPenalty`), rather than reaching for
  the unexported symbol directly.
- Fakes (`fakeEthClient` implementing `EthClient`, `fakePool` implementing
  `ProviderPool`, `fakeProviderHandle` implementing `RPCProvider`,
  `chainOnlyStorage`/`quotaOnlyStore` in `tests/storage_segregation_test.go`
  proving the `ChainStorage`/`QuotaStorage` split is real) already exist —
  reuse them instead of writing new ones per file.
- `Memory`'s three pieces each get their own test file —
  `tests/memorychain_test.go`, `tests/memoryscore_test.go`,
  `tests/memoryquota_test.go` — constructing `NewMemoryChainStorage()` /
  `NewMemoryScoreStorage()` / `NewMemoryQuotaStorage()` directly rather than
  going through `NewMemory()`, so each piece is verified independently of
  the other two. `tests/memory_test.go` itself only covers the composition
  (that embedding wires up the full `Storage` correctly, and that using all
  three together concurrently through one `Memory` value is safe) — put new
  behavioral tests in the piece-specific file they belong to, not there.
- If a test genuinely needs a new hook into internal behavior, consider
  whether that hook is legitimate public API (like `DialFunc` on
  `ProviderConfig`, added specifically so `Provider` is testable without a
  real RPC endpoint) before assuming the test can't be written.

Run tests:

```sh
go test ./...                              # everything
go test ./... -race                        # with the race detector (always do this before considering work done)
go test ./tests/... -coverpkg=./... -cover # coverage of the idx package (plain `go test ./...` shows 0% for
                                            # the root package itself, since its tests live in a different
                                            # directory/package — coverage still needs -coverpkg to attribute
                                            # correctly)
```

## Design invariants — do not casually change these

These were deliberate fixes/decisions made while turning the original draft
into this package. If you touch the surrounding code, preserve the
invariant unless the user explicitly asks to change it:

- **`Provider.IsAvailable()` has no side effects.** It must never consume
  rate-limit tokens or quota — only `Provider.withRetry` (i.e. an actual
  RPC attempt) does, via `QuotaManager.Consume()`. `QuotaManager.Remaining()`
  is the read-only peek used by `IsAvailable`. (The original draft this
  package was built from had `IsAvailable` silently burn one quota unit per
  check — a real bug; don't reintroduce it.)
- **`Pool.recalculateScores` penalizes on quota *usage*, not remaining
  quota.** Use `Provider.GetQuotaUsage()` (returns `limit, used, ...`), not
  `GetQuotaRemaining()` (returns `remaining, used, ...`) — mixing them up
  makes the `used >= limit` check always false, silently disabling the
  penalty.
- **`IndexerConfig.StartBlock` must apply on a fresh (empty) `ChainStorage`.**
  `syncBlocks` treats a persisted last-block of `0` as "nothing indexed
  yet" and substitutes `StartBlock - 1` so the first sync begins exactly at
  `StartBlock`, not block 1. Don't remove this without confirming the
  fresh-start behavior still makes sense.
- **`Indexer` never calls `os.Exit`.** Provider-pool exhaustion is surfaced
  via the `WithOnExhausted(func(error))` callback (default: no-op), passing
  an error wrapping `ErrProviderPoolExhausted`. A library must not
  unilaterally kill the host process; that decision belongs to the caller.
- **`ProviderPool`, `ChainStorage`, `BlockchainEventDispatcher` are the only
  things `Indexer` depends on** — never let it import or construct a
  concrete `Pool`/`Memory` internally. This is what keeps `Indexer` testable
  with fakes and reusable with any backend.
- **Score/quota persistence is optional, detected via type assertion —
  never make it a required part of `ChainStorage`.** `NewIndexer` and
  `Pool.PersistQuotaUsage` deliberately take the narrowest interface they
  need (`ChainStorage`, `QuotaStorage`), not the composite `Storage`. If
  you add a new kind of persisted state, follow the same pattern: a new
  small interface, checked with `storage.(NewInterface)` once at
  construction (see `Indexer.scores`/`Indexer.quotas` in `indexer.go`), not
  a new required method on `ChainStorage`. This is exactly what was fixed
  here — `ChainStorage`/`ScoreStorage`/`QuotaStorage` used to be one
  monolithic `Storage` interface forcing every implementer to stub out
  score/quota methods it might not care about.
- **No concrete SQL/Postgres storage in this repo.** `ChainStorage`/
  `ScoreStorage`/`QuotaStorage` stay interfaces; `Memory` is the only
  implementation, deliberately non-durable. If asked to add a real backend,
  it belongs in a separate package/repo, not merged into this flat layout.
- **`Memory` is a composition, not a monolith — keep it that way.**
  `MemoryChainStorage`/`MemoryScoreStorage`/`MemoryQuotaStorage` are
  separate types with separate locks, embedded into `Memory`. If you add a
  new piece of persisted state, give it its own `Memory*Storage` type (own
  file, own constructor, own lock) and embed it into `Memory` — don't grow
  one of the existing three types to cover an unrelated concern.
- **`PoolConfig.MaxLogBlockRange` is global, not per-provider.** RPC
  providers used to each carry their own `MaxLogBlockRange`, and `Pool`
  fell back to "the smallest configured value across providers" when picking
  a chunk size — that complexity was deliberately removed. One value now
  applies to every provider in the pool (default `DefaultMaxLogBlockRange`
  = 1, i.e. no real batching until configured). Don't reintroduce a
  per-provider override without being asked; if providers genuinely need
  different limits, the caller should run separate `Pool`s.
- **Sequential vs. batched sync is derived, not separately configured.**
  `Indexer.syncBlocks` picks the path purely from
  `idx.pool.MaxLogBlockRange()`: `== 1` → sequential (`syncBlocksSequential`,
  one `HeaderByNumber`+`LogsByBlockNumber` per block), `> 1` → batched
  (`syncBlocksBatched`, chunked `LogsByBlockRange`). There used to be a
  separate `IndexerConfig.BatchLagThreshold` (switch to batching once the
  chain lag exceeded some N) — removed as redundant: `PoolConfig` already
  says how big a chunk the providers can take, so that's what decides
  whether chunking is worth doing, not how far behind the indexer happens
  to be on any given tick. Don't add a lag-based heuristic back; if you
  need behavior to differ by how large the backlog is, that's a
  `syncBlocksBatched`-internal concern (chunk sizing), not a path-selection
  one. Note this makes the choice static for a `Pool`'s whole lifetime —
  tests that want to exercise the sequential path need a `Pool`/fake with
  `MaxLogBlockRange == 1` (the default), and a separate one with `> 1` for
  the batched path; one `Pool` can't demonstrate both.

## Before finishing any change

1. `go build ./...`
2. `go vet ./...`
3. `gofmt -l .` (should print nothing)
4. `go test ./... -race`
5. If you touched anything storage- or pool-related, also run
   `go test ./tests/... -coverpkg=./... -cover` and sanity-check coverage
   didn't regress.
