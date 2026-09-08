// Package idx is a reusable, chain-agnostic-storage EVM blockchain indexer:
// a pool of RPC providers with health tracking, rate limiting, circuit
// breaking and retries, feeding a block-by-block sync loop that dispatches
// decoded logs to listeners and persists progress through a small
// ChainStorage interface. No concrete storage backend is bundled —
// implement ChainStorage (plus, optionally, ScoreStorage and/or
// QuotaStorage) against your own database, or use the in-memory Memory
// store included here for tests, demos, and local development.
//
// The exported surface is built entirely around interfaces (RPCProvider,
// ProviderPool, ChainStorage, ScoreStorage, QuotaStorage,
// BlockchainListener, BlockchainEventDispatcher) so every piece can be
// swapped or faked in tests independently of the others.
package idx

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BlockchainListener handles raw blockchain log events.
type BlockchainListener interface {
	HandleLog(log types.Log, blockTimestamp uint64) error
}

// BlockchainEventDispatcher dispatches blockchain events to registered listeners.
type BlockchainEventDispatcher interface {
	Dispatch(log types.Log, blockTimestamp uint64) error
	Close()
}

// RPCProvider is a single upstream RPC endpoint, wrapped with health
// tracking (score, availability) so a ProviderPool can pick between several.
type RPCProvider interface {
	Name() string
	Priority() int
	Score() float64
	SetScore(score float64)
	IsAvailable() bool
	RecordSuccess()
	RecordFailure(err error)
	Close()

	// MaxLogBlockRange returns this provider's own eth_getLogs range limit,
	// or 0 to defer to the pool-wide PoolConfig.MaxLogBlockRange. See
	// ProviderConfig.MaxLogBlockRange and Pool.MaxLogBlockRange.
	MaxLogBlockRange() int

	BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error)
	BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	// SubscribeNewHead is a long-lived streaming subscription, not a
	// request/response call — implementations (see Provider) deliberately
	// don't run it through retry/circuit-breaker/quota/RequestTimeout, and
	// don't record success/failure against the provider's health score.
	SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockNumber(ctx context.Context) (uint64, error)
	BalanceAt(ctx context.Context, address common.Address) (*big.Int, error)
	Call(ctx context.Context, msg ethereum.CallMsg) ([]byte, error)
	CodeAt(ctx context.Context, address common.Address) ([]byte, error)
}

// ProviderPool selects a healthy RPCProvider for each call and tracks
// success/failure across the underlying providers.
type ProviderPool interface {
	GetProvider() RPCProvider
	GetAllProviders() []RPCProvider
	RecordSuccess(provider RPCProvider)
	RecordFailure(provider RPCProvider, err error)
	GetScores() map[string]float64
	PersistQuotaUsage(ctx context.Context, storage QuotaStorage) error
	Close()

	BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error)
	BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	LogsByBlockNumber(ctx context.Context, blockNum uint64) ([]types.Log, error)
	LogsByBlockRange(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error)
	SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockNumber(ctx context.Context) (uint64, error)
	BalanceAt(ctx context.Context, address common.Address) (*big.Int, error)
	CodeAt(ctx context.Context, address common.Address) ([]byte, error)

	// MaxLogBlockRange is the pool-wide max eth_getLogs block range (see
	// PoolConfig.MaxLogBlockRange), used when the indexer batches a backfill.
	MaxLogBlockRange() int
}

// ParallelBackfiller is an optional capability of a ProviderPool: fanning a
// single backfill range out across every currently available provider at
// once, instead of serving it from just the top-ranked one. The Indexer
// type-asserts its ProviderPool against this interface and uses it
// opportunistically — a pool that doesn't implement it (e.g. a test fake)
// simply never gets the parallel fast path, falling back to the ordinary
// single-provider chunked batching.
type ParallelBackfiller interface {
	// ParallelLogPlan reports how many providers are currently available to
	// fan a backfill out across, and the per-provider chunk size to use
	// (the smallest MaxLogBlockRange among them — see ProviderConfig.
	// MaxLogBlockRange and PoolConfig.MaxLogBlockRange for how an individual
	// provider's limit is resolved). Either value can be 0 (no providers
	// available); callers should not attempt a parallel round in that case.
	ParallelLogPlan() (providers int, chunkSize int)

	// LogsByBlockRangeParallel fetches logs for [fromBlock, toBlock] by
	// splitting it into ParallelLogPlan's chunkSize-sized sub-ranges and
	// dispatching them concurrently, each to a distinct available provider.
	// If a provider fails or becomes unavailable (circuit-broken, quota
	// exhausted) partway through, its outstanding sub-range is transparently
	// reassigned to another available provider; the call only returns once
	// every sub-range has succeeded, or once no available provider remains
	// to serve one — the caller never observes a partial, out-of-order, or
	// incomplete result. Returned logs are ordered the same way a single
	// FilterLogs call over the whole range would order them (ascending by
	// sub-range).
	LogsByBlockRangeParallel(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error)
}

// RangeTooLargeClassifier is an optional ProviderPool capability for
// recognizing provider-specific "range too large" error wording beyond the
// package's built-in heuristic (see PoolConfig.RangeTooLargeFilters and
// isRangeTooLargeError). The Indexer type-asserts its ProviderPool against
// this and prefers it when present, falling back to the package's default,
// non-configurable classifier if the pool doesn't implement it (e.g. a test
// fake) — the same optional-capability pattern as ScoreStorage/QuotaStorage.
type RangeTooLargeClassifier interface {
	IsRangeTooLargeError(err error) bool
}

// ChainStorage persists the indexer's core, chain-related state: sync
// progress and the events it has already indexed. It is the only storage
// capability the Indexer strictly requires — implement just this to back
// the indexer with your own database.
type ChainStorage interface {
	GetLastBlock(ctx context.Context, chain string) (uint64, error)
	SetLastBlock(ctx context.Context, chain string, blockNum uint64) error

	SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error
	GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error)
	IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error)

	Close() error
}

// ScoreStorage persists RPC provider health scores across restarts. It is
// optional: the Indexer type-asserts its ChainStorage against this
// interface and simply skips persisting scores if it isn't implemented.
type ScoreStorage interface {
	GetProviderScore(ctx context.Context, provider string) (*ProviderScore, error)
	SetProviderScore(ctx context.Context, provider string, score, penalty float64) error
	GetAllProviderScores(ctx context.Context) ([]ProviderScore, error)
}

// QuotaStorage persists RPC provider quota usage across restarts. Optional,
// same as ScoreStorage — a ProviderPool that tracks no quotas, or a Storage
// that doesn't care to persist them, need not implement it.
type QuotaStorage interface {
	GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error)
	SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error
}

// QuotaRestorer is implemented by an RPCProvider that can have previously
// persisted quota usage (see QuotaStorage) applied back to it. It's checked
// via type assertion, the same optional-capability pattern as
// ScoreStorage/QuotaStorage themselves — RPCProvider fakes in tests, or any
// implementation that doesn't track quota, simply don't implement it and
// are skipped. *Provider implements it.
type QuotaRestorer interface {
	RestoreQuotaUsage(used int64, resetAt time.Time)
}

// Storage is the full persistence surface: ChainStorage plus the optional
// ScoreStorage and QuotaStorage capabilities. This package ships no
// concrete backend (e.g. Postgres) — implement whichever of the three
// interfaces fit the host application, or use Memory (which implements all
// three) for tests/local development. NewIndexer only requires
// ChainStorage; Storage exists as a convenient "implements everything"
// shorthand for callers that want it.
type Storage interface {
	ChainStorage
	ScoreStorage
	QuotaStorage
}

type ProviderScore struct {
	ID        int
	Provider  string
	Score     float64
	Penalty   float64
	UpdatedAt time.Time
}

type QuotaUsage struct {
	ID        int
	Provider  string
	QuotaType string
	Used      int
	ResetAt   time.Time
	UpdatedAt time.Time
}

type IndexedEvent struct {
	ID          int
	Chain       string
	BlockNumber uint64
	TxHash      string
	Address     string
	Topics      string
	Data        string
	LogIndex    uint
	IndexedAt   time.Time
}
