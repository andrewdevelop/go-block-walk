package idx

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const defaultPoolUpdateInterval = 30 * time.Second

// DefaultMaxLogBlockRange is used when PoolConfig doesn't set
// MaxLogBlockRange, keeping batched eth_getLogs calls conservative (one
// block per call) by default — real batching is an opt-in.
const DefaultMaxLogBlockRange = 1

// Pool selects a healthy Provider for each call, biased towards whichever
// provider currently has the best health score, and tracks success/failure
// against whichever provider actually served the request. Safe for
// concurrent use.
type Pool struct {
	mu          sync.RWMutex
	providers   map[string]*Provider
	orderedList []*Provider
	penalties   map[string]float64

	updateInterval   time.Duration
	maxLogBlockRange int
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	closeOnce        sync.Once
}

// NewPool dials every configured provider and starts a background loop that
// periodically re-ranks them by health score. Call Close when done.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if len(cfg.Providers) == 0 {
		return nil, fmt.Errorf("idx: pool requires at least one provider")
	}

	seenNames := make(map[string]bool, len(cfg.Providers))
	for _, pc := range cfg.Providers {
		if pc.Name == "" {
			return nil, fmt.Errorf("idx: provider name is required")
		}
		if seenNames[pc.Name] {
			return nil, fmt.Errorf("idx: duplicate provider name %q", pc.Name)
		}
		seenNames[pc.Name] = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	updateInterval := cfg.UpdateInterval
	if updateInterval <= 0 {
		updateInterval = defaultPoolUpdateInterval
	}

	maxLogBlockRange := cfg.MaxLogBlockRange
	if maxLogBlockRange <= 0 {
		maxLogBlockRange = DefaultMaxLogBlockRange
	}

	pool := &Pool{
		providers:        make(map[string]*Provider, len(cfg.Providers)),
		orderedList:      make([]*Provider, 0, len(cfg.Providers)),
		penalties:        make(map[string]float64),
		updateInterval:   updateInterval,
		maxLogBlockRange: maxLogBlockRange,
		ctx:              ctx,
		cancel:           cancel,
	}

	for _, pc := range cfg.Providers {
		p := NewProvider(ctx, pc)
		pool.providers[pc.Name] = p
		pool.orderedList = append(pool.orderedList, p)
	}

	pool.sortOrderedListLocked()

	pool.wg.Add(1)
	go pool.updateLoop()

	return pool, nil
}

func (pool *Pool) updateLoop() {
	defer pool.wg.Done()

	ticker := time.NewTicker(pool.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pool.ctx.Done():
			return
		case <-ticker.C:
			pool.recalculateScores()
		}
	}
}

func (pool *Pool) recalculateScores() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, p := range pool.orderedList {
		limit, used, creditsLimit, creditsUsed := p.GetQuotaUsage()

		penalty := 0.0
		if limit > 0 && used >= limit {
			penalty += 10
		}
		if creditsLimit > 0 && creditsUsed >= creditsLimit {
			penalty += 5
		}
		pool.penalties[p.Name()] = penalty
	}

	pool.sortOrderedListLocked()
}

// sortOrderedListLocked requires pool.mu to be held. Priority (ascending —
// lower number preferred) is the primary key, exactly matching NewPool's
// initial ordering; adjustedScore (descending — higher is healthier) only
// breaks ties between same-priority providers. Sorting by adjustedScore
// alone would let it override Priority entirely, since baseScore is seeded
// from the raw Priority number and dominates the small +0.1/-0.2 deltas
// RecordSuccess/RecordFailure apply — that was the bug: a low-priority
// (large-number) provider's raw score outweighs a high-priority one's.
func (pool *Pool) sortOrderedListLocked() {
	sort.Slice(pool.orderedList, func(i, j int) bool {
		a, b := pool.orderedList[i], pool.orderedList[j]
		if a.Priority() != b.Priority() {
			return a.Priority() < b.Priority()
		}
		return pool.adjustedScoreLocked(a) > pool.adjustedScoreLocked(b)
	})
}

// adjustedScoreLocked requires pool.mu to be held.
func (pool *Pool) adjustedScoreLocked(p *Provider) float64 {
	return p.Score() - pool.penalties[p.Name()]
}

func (pool *Pool) GetProvider() RPCProvider {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	for _, p := range pool.orderedList {
		if p.IsAvailable() {
			return p
		}
	}
	return nil
}

func (pool *Pool) GetAllProviders() []RPCProvider {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	result := make([]RPCProvider, len(pool.orderedList))
	for i, p := range pool.orderedList {
		result[i] = p
	}
	return result
}

func (pool *Pool) RecordSuccess(provider RPCProvider) {
	pool.mu.RLock()
	p, ok := pool.providers[provider.Name()]
	pool.mu.RUnlock()
	if ok {
		p.RecordSuccess()
	}
}

// RecordFailure lowers provider's health score for err — except when err is
// ErrQuotaExceeded: recalculateScores already applies its own penalty for
// quota exhaustion (the `used >= limit` check), so applying RecordFailure's
// score penalty here too would double-penalize the exact same condition on
// every quota-denied call.
func (pool *Pool) RecordFailure(provider RPCProvider, err error) {
	if errors.Is(err, ErrQuotaExceeded) {
		return
	}

	pool.mu.RLock()
	p, ok := pool.providers[provider.Name()]
	pool.mu.RUnlock()
	if ok {
		p.RecordFailure(err)
	}
}

func (pool *Pool) BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.BlockByNumber(ctx, blockNum)
	if err != nil {
		if !isLocalDecodeError(err) {
			pool.RecordFailure(p, err)
		}
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.BlockByHash(ctx, hash)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, false, ErrNoAvailableProvider
	}

	result, isPending, err := p.TransactionByHash(ctx, hash)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, false, err
	}

	pool.RecordSuccess(p)
	return result, isPending, nil
}

func (pool *Pool) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.TransactionReceipt(ctx, hash)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.FilterLogs(ctx, q)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) LogsByBlockNumber(ctx context.Context, blockNum uint64) ([]types.Log, error) {
	return pool.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(blockNum),
		ToBlock:   new(big.Int).SetUint64(blockNum),
	})
}

// LogsByBlockRange fetches logs for [fromBlock, toBlock] in a single
// eth_getLogs call. Callers doing a batched backfill are expected to size
// the range using MaxLogBlockRange first — this does not split oversized
// ranges itself.
func (pool *Pool) LogsByBlockRange(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error) {
	return pool.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})
}

// MaxLogBlockRange returns the eth_getLogs chunk size to use next: the
// currently active provider's own limit (see ProviderConfig.MaxLogBlockRange)
// if it has one, falling back to the pool-wide PoolConfig.MaxLogBlockRange
// otherwise. Re-resolved on every call (Indexer.syncBlocksBatched calls it
// once per chunk) so a higher-priority provider with a larger limit isn't
// dragged down to whatever the smallest fallback provider in the pool
// supports.
//
// Because GetProvider is also re-resolved independently on the actual
// LogsByBlockRange call, the provider that ends up serving a chunk can
// still differ from the one active when the chunk was sized (e.g. a
// failover happening in between) — see isRangeTooLargeError and
// Indexer.syncBlocksBatched for how that's recovered from.
func (pool *Pool) MaxLogBlockRange() int {
	if p := pool.GetProvider(); p != nil {
		if r := p.MaxLogBlockRange(); r > 0 {
			return r
		}
	}
	return pool.maxLogBlockRange
}

func (pool *Pool) SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}
	return p.SubscribeNewHead(ctx, ch)
}

func (pool *Pool) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.HeaderByNumber(ctx, number)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) BlockNumber(ctx context.Context) (uint64, error) {
	p := pool.GetProvider()
	if p == nil {
		return 0, ErrNoAvailableProvider
	}

	result, err := p.BlockNumber(ctx)
	if err != nil {
		pool.RecordFailure(p, err)
		return 0, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

func (pool *Pool) BalanceAt(ctx context.Context, address common.Address) (*big.Int, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}

	result, err := p.BalanceAt(ctx, address)
	if err != nil {
		pool.RecordFailure(p, err)
		return nil, err
	}

	pool.RecordSuccess(p)
	return result, nil
}

// CodeAt does not record provider success/failure — it isn't on the
// critical indexing path.
func (pool *Pool) CodeAt(ctx context.Context, address common.Address) ([]byte, error) {
	p := pool.GetProvider()
	if p == nil {
		return nil, ErrNoAvailableProvider
	}
	return p.CodeAt(ctx, address)
}

// Close is safe to call more than once; only the first call has any effect.
func (pool *Pool) Close() {
	pool.closeOnce.Do(func() {
		pool.cancel()
		pool.wg.Wait()

		pool.mu.RLock()
		defer pool.mu.RUnlock()
		for _, p := range pool.providers {
			p.Close()
		}
	})
}

func (pool *Pool) GetScores() map[string]float64 {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	scores := make(map[string]float64, len(pool.providers))
	for name, p := range pool.providers {
		scores[name] = pool.adjustedScoreLocked(p)
	}
	return scores
}

func (pool *Pool) PersistQuotaUsage(ctx context.Context, storage QuotaStorage) error {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	for _, p := range pool.providers {
		_, used, _, _ := p.GetQuotaUsage()
		reqReset, _ := p.GetQuotaReset()
		if err := storage.SetQuotaUsage(ctx, p.Name(), "requests", int(used), reqReset); err != nil {
			return fmt.Errorf("persist quota for %s: %w", p.Name(), err)
		}
	}
	return nil
}

var _ ProviderPool = (*Pool)(nil)
