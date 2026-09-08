package idx

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
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

	updateInterval         time.Duration
	maxLogBlockRange       int
	maxParallelJobAttempts int
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	closeOnce              sync.Once
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
		providers:              make(map[string]*Provider, len(cfg.Providers)),
		orderedList:            make([]*Provider, 0, len(cfg.Providers)),
		penalties:              make(map[string]float64),
		updateInterval:         updateInterval,
		maxLogBlockRange:       maxLogBlockRange,
		maxParallelJobAttempts: cfg.MaxParallelJobAttempts,
		ctx:                    ctx,
		cancel:                 cancel,
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

// parallelJobAttemptLimit bounds how many times a single sub-range job may
// be retried (against any provider, not just the one that first failed it)
// during a parallel backfill round before LogsByBlockRangeParallel gives up
// on it and fails the whole round. Without a cap, a sub-range that every
// provider rejects for a reason that doesn't trip the circuit breaker or
// exhaust quota (so IsAvailable keeps reporting the provider as fine) would
// bounce between providers forever.
//
// Defaults to twice the number of providers taking part in this round (so
// every provider gets, on average, two independent chances at any given
// job before it's abandoned) unless PoolConfig.MaxParallelJobAttempts
// overrides it. available is guaranteed >= 2 by LogsByBlockRangeParallel's
// only caller of this, so the default is always at least 4.
func (pool *Pool) parallelJobAttemptLimit(available int) int {
	if pool.maxParallelJobAttempts > 0 {
		return pool.maxParallelJobAttempts
	}
	return available * 2
}

// availableProvidersSnapshot returns the currently available providers, in
// the pool's existing priority/score order, as concrete *Provider so
// LogsByBlockRangeParallel can call FilterLogs on a specific one directly
// (Pool's other methods always go through GetProvider, i.e. only ever the
// single top-ranked provider).
func (pool *Pool) availableProvidersSnapshot() []*Provider {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	out := make([]*Provider, 0, len(pool.orderedList))
	for _, p := range pool.orderedList {
		if p.IsAvailable() {
			out = append(out, p)
		}
	}
	return out
}

// effectiveMaxLogBlockRangeLocked returns p's own eth_getLogs range limit,
// falling back to the pool-wide default — the same resolution
// Pool.MaxLogBlockRange applies to the single active provider, generalised
// to any provider. Does not require pool.mu (maxLogBlockRange is immutable
// after NewPool); named "Locked" only for symmetry with its one caller.
func (pool *Pool) effectiveMaxLogBlockRange(p *Provider) int {
	if r := p.MaxLogBlockRange(); r > 0 {
		return r
	}
	return pool.maxLogBlockRange
}

// ParallelLogPlan implements ParallelBackfiller.
func (pool *Pool) ParallelLogPlan() (providers int, chunkSize int) {
	available := pool.availableProvidersSnapshot()
	if len(available) == 0 {
		return 0, 0
	}

	smallest := 0
	for _, p := range available {
		limit := pool.effectiveMaxLogBlockRange(p)
		if limit <= 0 {
			limit = 1
		}
		if smallest == 0 || limit < smallest {
			smallest = limit
		}
	}
	return len(available), smallest
}

type parallelLogJob struct {
	index    int
	from, to uint64
	attempts int
}

// LogsByBlockRangeParallel implements ParallelBackfiller. See that
// interface's doc for the external contract; the approach here is a
// worker-per-available-provider pool pulling sub-range jobs off a shared
// queue: a job that fails gets pushed back onto the queue for a different
// (or, if it later recovers, the same) provider to pick up, and a worker
// exits once its own provider stops being available, shrinking the pool of
// active workers. The round only completes once every job has succeeded
// (results assembled back in ascending sub-range order) or fails outright
// once no worker remains to make progress.
//
// The set of workers is fixed to the availableProvidersSnapshot taken at
// the top of this call: a provider that was down at that point and comes
// back up before the round finishes does NOT get a worker spun up for it
// mid-round — it only rejoins on the *next* call, once
// Indexer.syncBlocksBatched re-queries ParallelLogPlan for the next chunk.
// This is deliberate, not an oversight: a round only ever spans
// chunkSize*len(available) blocks, so the window where a recovered
// provider sits idle is short-lived and self-corrects on the very next
// round: polling provider health mid-round and growing the worker pool on
// the fly isn't worth the added complexity for that small a window.
func (pool *Pool) LogsByBlockRangeParallel(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error) {
	if toBlock < fromBlock {
		return nil, nil
	}

	available := pool.availableProvidersSnapshot()
	if len(available) == 0 {
		return nil, ErrNoAvailableProvider
	}

	chunkSize := uint64(1)
	for _, p := range available {
		if limit := uint64(pool.effectiveMaxLogBlockRange(p)); limit > 0 && (chunkSize == 1 || limit < chunkSize) {
			chunkSize = limit
		}
	}

	totalBlocks := toBlock - fromBlock + 1
	if len(available) < 2 || totalBlocks <= chunkSize {
		// Not enough spare providers, or not enough backlog to give more than
		// one of them meaningful work — a plain single-provider fetch (which
		// already knows how to recover from a too-large range) covers this
		// just as well with none of the fan-out bookkeeping.
		return pool.LogsByBlockRange(ctx, fromBlock, toBlock)
	}

	var jobs []parallelLogJob
	for start, i := fromBlock, 0; start <= toBlock; i++ {
		end := start + chunkSize - 1
		if end > toBlock {
			end = toBlock
		}
		jobs = append(jobs, parallelLogJob{index: i, from: start, to: end})
		start = end + 1
	}

	results := make([][]types.Log, len(jobs))
	jobCh := make(chan parallelLogJob, len(jobs))
	for _, j := range jobs {
		jobCh <- j
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	attemptLimit := pool.parallelJobAttemptLimit(len(available))

	var (
		mu       sync.Mutex
		firstErr error
	)
	remaining := int32(len(jobs))
	active := int32(len(available))

	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}

	var wg sync.WaitGroup
	for _, p := range available {
		wg.Add(1)
		go func(p *Provider) {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return
					}

					logs, err := p.FilterLogs(runCtx, ethereum.FilterQuery{
						FromBlock: new(big.Int).SetUint64(job.from),
						ToBlock:   new(big.Int).SetUint64(job.to),
					})
					if err != nil {
						pool.RecordFailure(p, err)

						if isRangeTooLargeError(err) {
							// Sizing already used the smallest available
							// provider's limit, so this means the provider
							// that actually served it (resolved independently
							// of the sizing snapshot) has an even smaller one
							// — bail out to the caller's own chunk-splitting
							// recovery rather than spinning sub-ranges that
							// keep coming back too large.
							fail(fmt.Errorf("idx: parallel backfill chunk %d..%d rejected as too large: %w", job.from, job.to, err))
							return
						}

						job.attempts++
						if job.attempts >= attemptLimit {
							fail(fmt.Errorf("idx: parallel backfill chunk %d..%d failed after %d attempts: %w", job.from, job.to, job.attempts, err))
							return
						}

						select {
						case jobCh <- job:
						case <-runCtx.Done():
							return
						}

						if !p.IsAvailable() {
							if atomic.AddInt32(&active, -1) == 0 {
								fail(fmt.Errorf("idx: parallel backfill exhausted every provider before finishing: %w", err))
							}
							return
						}
						continue
					}

					pool.RecordSuccess(p)
					mu.Lock()
					results[job.index] = logs
					mu.Unlock()

					if atomic.AddInt32(&remaining, -1) == 0 {
						cancel()
						return
					}
				}
			}
		}(p)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if remaining != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("idx: parallel backfill of %d..%d aborted before completion", fromBlock, toBlock)
	}

	out := make([]types.Log, 0, totalBlocks)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
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
var _ ParallelBackfiller = (*Pool)(nil)
