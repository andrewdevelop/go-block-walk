package idx

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	defaultMaxConsecutiveProviderExhaustion = 5
	defaultProviderExhaustionRestartDelay   = 15 * time.Second
)

// Indexer polls a ProviderPool for new blocks on a fixed interval (or on
// demand via Nudge), dispatches their logs to a BlockchainEventDispatcher,
// and persists sync progress to a ChainStorage. It depends only on the
// ProviderPool, ChainStorage and BlockchainEventDispatcher interfaces, so
// any implementation of each can be plugged in — including fakes in tests.
//
// Persisting provider health scores and quota usage (see ScoreStorage,
// QuotaStorage) is optional: if the ChainStorage passed to NewIndexer also
// implements one or both, the Indexer persists that data as a side effect
// of normal syncing; if not, it's silently skipped. A minimal ChainStorage
// implementation with no score/quota persistence is a complete, valid
// Indexer backend.
type Indexer struct {
	config     IndexerConfig
	pool       ProviderPool
	dispatcher BlockchainEventDispatcher
	storage    ChainStorage
	scores     ScoreStorage // nil if storage doesn't implement ScoreStorage
	quotas     QuotaStorage // nil if storage doesn't implement QuotaStorage
	logger     *slog.Logger

	mu        sync.RWMutex
	isRunning bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopOnce  sync.Once

	exhaustion  *ExhaustionTracker
	onExhausted func(error)

	nudge *NudgeSignal

	// reorgDepth and the hash bookkeeping below are only ever touched from
	// syncLoop's single goroutine, so they need no lock of their own.
	reorgDepth   int
	recentHashes map[uint64]common.Hash
	hashOrder    []uint64
}

// Option configures optional Indexer behaviour.
type Option func(*Indexer)

// WithLogger sets the logger used for diagnostic output. Defaults to
// slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(i *Indexer) {
		if logger != nil {
			i.logger = logger
		}
	}
}

// WithOnExhausted sets the callback invoked once the provider pool has had
// no available provider for IndexerConfig.MaxConsecutiveProviderExhaustion
// consecutive sync attempts — every provider down, circuit-broken, or with
// its retries/quota exhausted. The callback receives an error wrapping
// ErrProviderPoolExhausted (check it with errors.Is if you want to branch
// on the specific condition, or just log it). Defaults to a no-op (the loop
// simply keeps retrying on the next tick after logging). A typical
// production choice is to treat it as the "please restart me" signal and
// exit the process, letting an external supervisor (e.g. a container
// orchestrator) restart it with fresh circuit breakers:
//
//	idx.WithOnExhausted(func(err error) {
//		log.Println(err)
//		os.Exit(1)
//	})
func WithOnExhausted(fn func(err error)) Option {
	return func(i *Indexer) {
		if fn != nil {
			i.onExhausted = fn
		}
	}
}

// NewIndexer wires up an Indexer. pool, storage and dispatcher must be
// non-nil. If storage also implements ScoreStorage and/or QuotaStorage,
// that data is persisted automatically; otherwise it's skipped.
func NewIndexer(cfg IndexerConfig, pool ProviderPool, storage ChainStorage, dispatcher BlockchainEventDispatcher, opts ...Option) (*Indexer, error) {
	if pool == nil {
		return nil, fmt.Errorf("idx: pool is required")
	}
	if storage == nil {
		return nil, fmt.Errorf("idx: storage is required")
	}
	if dispatcher == nil {
		return nil, fmt.Errorf("idx: dispatcher is required")
	}
	if cfg.Chain == "" {
		return nil, fmt.Errorf("idx: chain is required")
	}
	if cfg.BlockInterval <= 0 {
		return nil, fmt.Errorf("idx: BlockInterval must be positive")
	}

	if cfg.MaxConsecutiveProviderExhaustion <= 0 {
		cfg.MaxConsecutiveProviderExhaustion = defaultMaxConsecutiveProviderExhaustion
	}
	if cfg.ProviderExhaustionRestartDelay <= 0 {
		cfg.ProviderExhaustionRestartDelay = defaultProviderExhaustionRestartDelay
	}

	ctx, cancel := context.WithCancel(context.Background())

	scores, _ := storage.(ScoreStorage)
	quotas, _ := storage.(QuotaStorage)

	idx := &Indexer{
		config:      cfg,
		pool:        pool,
		dispatcher:  dispatcher,
		storage:     storage,
		scores:      scores,
		quotas:      quotas,
		logger:      slog.Default(),
		ctx:         ctx,
		cancel:      cancel,
		exhaustion:  NewExhaustionTracker(cfg.MaxConsecutiveProviderExhaustion),
		onExhausted: func(error) {},
		nudge:       NewNudgeSignal(),

		reorgDepth:   cfg.ReorgDepth,
		recentHashes: make(map[uint64]common.Hash),
	}

	for _, opt := range opts {
		opt(idx)
	}

	idx.restoreProviderState()

	return idx, nil
}

// restoreProviderState loads previously persisted provider health scores
// and quota usage (if storage implements ScoreStorage/QuotaStorage) back
// onto the pool's providers, so a restart doesn't make a broken provider
// look healthy again or forget quota already spent this period. Missing
// entries (nothing persisted yet for a provider) are left at their
// construction-time defaults.
func (idx *Indexer) restoreProviderState() {
	if idx.scores == nil && idx.quotas == nil {
		return
	}

	for _, p := range idx.pool.GetAllProviders() {
		if p == nil {
			// A ProviderPool implementation is free to return a nil entry
			// for "no provider available right now" (e.g. this package's
			// own test fakes do) — nothing to restore onto.
			continue
		}

		if idx.scores != nil {
			if s, err := idx.scores.GetProviderScore(idx.ctx, p.Name()); err != nil {
				idx.logger.Warn("failed to restore provider score", "provider", p.Name(), "error", err)
			} else if s != nil {
				p.SetScore(s.Score)
			}
		}

		if idx.quotas != nil {
			if restorer, ok := p.(QuotaRestorer); ok {
				if u, err := idx.quotas.GetQuotaUsage(idx.ctx, p.Name(), "requests"); err != nil {
					idx.logger.Warn("failed to restore provider quota usage", "provider", p.Name(), "error", err)
				} else if u != nil {
					restorer.RestoreQuotaUsage(int64(u.Used), u.ResetAt)
				}
			}
		}
	}
}

// Start launches the sync loop. An Indexer is one-shot: once Stop has been
// called, Start returns ErrIndexerStopped instead of silently launching a
// sync loop whose context is already cancelled (Stop tears down the pool
// and dispatcher for good — there's nothing left to resume).
func (idx *Indexer) Start() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.stopped {
		return ErrIndexerStopped
	}
	if idx.isRunning {
		return ErrAlreadyRunning
	}
	idx.isRunning = true

	idx.wg.Add(1)
	go idx.syncLoop()

	return nil
}

// Stop cancels the sync loop, waits for it to exit, and closes the
// dispatcher and pool (but not the storage — callers that share it with
// other components close it themselves). Safe to call more than once or
// without a prior Start; only the first call has any effect.
func (idx *Indexer) Stop() {
	idx.stopOnce.Do(func() {
		idx.cancel()
		idx.wg.Wait()

		idx.mu.Lock()
		idx.isRunning = false
		idx.stopped = true
		idx.mu.Unlock()

		idx.dispatcher.Close()
		idx.pool.Close()
	})
}

// Nudge requests an immediate sync instead of waiting for the next
// BlockInterval tick. It only changes the timing of the next sync call; the
// call itself is the same block-by-block pipeline as always, so it doesn't
// bypass ordering or idempotency. Safe to call from any goroutine; multiple
// pending nudges collapse into a single early sync.
func (idx *Indexer) Nudge() {
	idx.nudge.Fire()
}

func (idx *Indexer) syncLoop() {
	defer idx.wg.Done()

	ticker := time.NewTicker(idx.config.BlockInterval)
	defer ticker.Stop()

	runSync := func(reason string) {
		if err := idx.syncBlocks(); err != nil {
			idx.logger.Error("sync error", "reason", reason, "error", err)
		}
	}

	for {
		select {
		case <-idx.ctx.Done():
			return
		case <-ticker.C:
			runSync("tick")
		case <-idx.nudge.C():
			runSync("nudge")
			ticker.Reset(idx.config.BlockInterval)
		}
	}
}

func (idx *Indexer) syncBlocks() error {
	if idx.exhaustion.Observe(idx.pool.GetProvider() != nil) {
		err := fmt.Errorf("%w: no provider available for %d consecutive sync attempts",
			ErrProviderPoolExhausted, idx.exhaustion.Consecutive())
		idx.logger.Error("provider pool exhausted, invoking OnExhausted",
			"consecutive", idx.exhaustion.Consecutive(),
			"delay", idx.config.ProviderExhaustionRestartDelay,
			"error", err)
		select {
		case <-idx.ctx.Done():
			return nil
		case <-time.After(idx.config.ProviderExhaustionRestartDelay):
		}
		idx.onExhausted(err)
		return nil
	}

	lastBlock, err := idx.getLastBlock()
	if err != nil {
		return fmt.Errorf("failed to get last block: %w", err)
	}
	if lastBlock == 0 && idx.config.StartBlock > 0 {
		// Nothing persisted yet: treat StartBlock-1 as "already processed" so
		// the first sync begins exactly at StartBlock instead of block 1.
		lastBlock = idx.config.StartBlock - 1
	}

	lastBlock, err = idx.detectReorgRewind(lastBlock)
	if err != nil {
		return fmt.Errorf("reorg check failed: %w", err)
	}

	currentBlock, err := idx.pool.BlockNumber(idx.ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	idx.logger.Debug("syncBlocks", "lastBlock", lastBlock, "currentBlock", currentBlock)

	if currentBlock < lastBlock {
		// Chain was reset (e.g. local devnet restart) — re-index from StartBlock.
		idx.logger.Warn("chain reset detected, re-indexing from StartBlock",
			"currentBlock", currentBlock, "lastBlock", lastBlock, "startBlock", idx.config.StartBlock)
		lastBlock = idx.config.StartBlock
		if err := idx.storeLastBlock(lastBlock); err != nil {
			return fmt.Errorf("failed to reset last block after chain reset: %w", err)
		}
		idx.pruneHashesAbove(lastBlock)
	}

	if currentBlock <= lastBlock {
		return nil
	}

	startBlock := lastBlock + 1
	if startBlock == 0 {
		startBlock = 1
	}

	if idx.pool.MaxLogBlockRange() > 1 {
		idx.logger.Debug("processing blocks in batches",
			"maxLogBlockRange", idx.pool.MaxLogBlockRange(), "from", startBlock, "to", currentBlock)
		return idx.syncBlocksBatched(startBlock, currentBlock)
	}

	idx.logger.Debug("processing blocks sequentially", "from", startBlock, "to", currentBlock)
	return idx.syncBlocksSequential(startBlock, currentBlock)
}

// syncBlocksSequential processes blocks one at a time: a header fetch plus
// a single-block eth_getLogs call per block, with progress persisted after
// every block. This is the low-latency path used once the indexer is
// caught up.
func (idx *Indexer) syncBlocksSequential(startBlock, currentBlock uint64) error {
	for blockNum := startBlock; blockNum <= currentBlock; blockNum++ {
		if err := idx.processBlock(blockNum); err != nil {
			if isLocalDecodeError(err) {
				idx.logger.Warn("skipping block permanently: unsupported transaction type (chain upgrade ahead of go-ethereum); its logs will not be indexed", "block", blockNum)
			} else {
				return fmt.Errorf("failed to process block %d: %w", blockNum, err)
			}
		}

		if err := idx.storeLastBlock(blockNum); err != nil {
			return fmt.Errorf("failed to save last block: %w", err)
		}

		idx.persistProviderState()
	}

	return nil
}

// syncBlocksBatched backfills [startBlock, currentBlock] using ranged
// eth_getLogs calls instead of one call per block, chunked to the currently
// selected provider's MaxLogBlockRange. Block headers (needed only for
// their timestamp) are fetched once per distinct block that actually
// produced logs, instead of once per block in the range. Progress is
// persisted once per chunk rather than once per block.
//
// chunkSize starts at pool.MaxLogBlockRange() and is refreshed from the
// pool at the start of each new chunk — so a failover to a smaller- or
// larger-limit provider between chunks is picked up automatically. It is
// NOT refreshed from the pool while retrying the same chunkStart after a
// range-too-large split (the retrying flag below): the pool-wide/active-
// provider value that produced the oversized request in the first place
// would just undo the split and spin forever.
func (idx *Indexer) syncBlocksBatched(startBlock, currentBlock uint64) error {
	chunkSize := uint64(idx.pool.MaxLogBlockRange())
	if chunkSize == 0 {
		chunkSize = 1
	}

	retrying := false
	for chunkStart := startBlock; chunkStart <= currentBlock; {
		if !retrying {
			if poolLimit := uint64(idx.pool.MaxLogBlockRange()); poolLimit > 0 {
				chunkSize = poolLimit
			}
		}
		retrying = false

		chunkEnd := chunkStart + chunkSize - 1
		if chunkEnd > currentBlock {
			chunkEnd = currentBlock
		}

		idx.logger.Info("fetching logs for chunk", "from", chunkStart, "to", chunkEnd)

		logs, err := idx.pool.LogsByBlockRange(idx.ctx, chunkStart, chunkEnd)
		if err != nil {
			if isRangeTooLargeError(err) && chunkEnd > chunkStart {
				// The provider that ended up serving this particular
				// request (GetProvider is re-resolved independently by the
				// pool on every call) has a smaller limit than whatever
				// chunkSize was computed from. Split the range instead of
				// failing the whole sync, and retry the same chunkStart.
				chunkSize = (chunkEnd - chunkStart + 1) / 2
				if chunkSize == 0 {
					chunkSize = 1
				}
				retrying = true
				idx.logger.Warn("log range rejected as too large by serving provider, splitting chunk and retrying",
					"from", chunkStart, "to", chunkEnd, "newChunkSize", chunkSize, "error", err)
				continue
			}
			return fmt.Errorf("failed to get logs for blocks %d..%d: %w", chunkStart, chunkEnd, err)
		}

		blockTimes := make(map[uint64]uint64, len(logs))
		for _, l := range logs {
			blockTime, ok := blockTimes[l.BlockNumber]
			if !ok {
				header, err := idx.pool.HeaderByNumber(idx.ctx, new(big.Int).SetUint64(l.BlockNumber))
				if err != nil {
					if isLocalDecodeError(err) {
						idx.logger.Warn("skipping block permanently: unsupported transaction type (chain upgrade ahead of go-ethereum); its logs will not be indexed", "block", l.BlockNumber)
					} else {
						return fmt.Errorf("failed to get header for block %d: %w", l.BlockNumber, err)
					}
					blockTimes[l.BlockNumber] = 0
					continue
				}
				blockTime = header.Time
				blockTimes[l.BlockNumber] = blockTime
			}

			if blockTime == 0 {
				// Header fetch for this block failed (unsupported tx type) — already warned above.
				continue
			}

			if err := idx.dispatcher.Dispatch(l, blockTime); err != nil {
				return fmt.Errorf("failed to dispatch log for block %d: %w", l.BlockNumber, err)
			}
		}

		if idx.reorgDepth > 0 {
			if header, err := idx.pool.HeaderByNumber(idx.ctx, new(big.Int).SetUint64(chunkEnd)); err == nil {
				idx.recordBlockHash(chunkEnd, header.Hash())
			}
		}

		if err := idx.storeLastBlock(chunkEnd); err != nil {
			return fmt.Errorf("failed to save last block: %w", err)
		}

		idx.persistProviderState()

		chunkStart = chunkEnd + 1
	}

	return nil
}

// persistProviderState saves provider scores and quota usage to storage,
// if it supports either (see ScoreStorage, QuotaStorage). Called after each
// block in the sequential path and after each chunk in the batched path.
func (idx *Indexer) persistProviderState() {
	if idx.scores != nil {
		for provider, score := range idx.pool.GetScores() {
			if err := idx.scores.SetProviderScore(idx.ctx, provider, score, 0); err != nil {
				idx.logger.Warn("failed to save provider score", "provider", provider, "error", err)
			}
		}
	}

	if idx.quotas != nil {
		if err := idx.pool.PersistQuotaUsage(idx.ctx, idx.quotas); err != nil {
			idx.logger.Warn("failed to persist quota usage", "error", err)
		}
	}
}

func (idx *Indexer) getLastBlock() (uint64, error) {
	if idx.config.DebugMode {
		return 0, nil
	}
	return idx.storage.GetLastBlock(idx.ctx, idx.config.Chain)
}

func (idx *Indexer) storeLastBlock(blockNum uint64) error {
	if idx.config.DebugMode {
		return nil
	}
	return idx.storage.SetLastBlock(idx.ctx, idx.config.Chain, blockNum)
}

func (idx *Indexer) processBlock(blockNum uint64) error {
	idx.logger.Debug("processing block", "block", blockNum)

	// Use HeaderByNumber instead of BlockByNumber: we only need the
	// timestamp, and BlockByNumber fails on blocks that contain unsupported
	// transaction types (e.g. EIP-4844 blob txs on newer chain forks).
	header, err := idx.pool.HeaderByNumber(idx.ctx, new(big.Int).SetUint64(blockNum))
	if err != nil {
		return fmt.Errorf("failed to get header for block %d: %w", blockNum, err)
	}
	blockTime := header.Time
	idx.recordBlockHash(blockNum, header.Hash())

	logs, err := idx.pool.LogsByBlockNumber(idx.ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to get logs for block %d: %w", blockNum, err)
	}

	for _, l := range logs {
		if err := idx.dispatcher.Dispatch(l, blockTime); err != nil {
			return fmt.Errorf("failed to dispatch log for block %d: %w", blockNum, err)
		}
	}

	return nil
}

// recordBlockHash remembers blockNum's hash for later reorg comparison,
// trimming to the last ReorgDepth entries. No-op if reorg detection is
// disabled (ReorgDepth <= 0).
func (idx *Indexer) recordBlockHash(blockNum uint64, hash common.Hash) {
	if idx.reorgDepth <= 0 {
		return
	}

	if _, exists := idx.recentHashes[blockNum]; !exists {
		idx.hashOrder = append(idx.hashOrder, blockNum)
	}
	idx.recentHashes[blockNum] = hash

	for len(idx.hashOrder) > idx.reorgDepth {
		delete(idx.recentHashes, idx.hashOrder[0])
		idx.hashOrder = idx.hashOrder[1:]
	}
}

// pruneHashesAbove discards any recorded hash above blockNum, e.g. after a
// rewind — those blocks are about to be reprocessed and will re-record
// fresh hashes.
func (idx *Indexer) pruneHashesAbove(blockNum uint64) {
	kept := idx.hashOrder[:0]
	for _, b := range idx.hashOrder {
		if b > blockNum {
			delete(idx.recentHashes, b)
			continue
		}
		kept = append(kept, b)
	}
	idx.hashOrder = kept
}

// detectReorgRewind compares the chain's current hash at lastBlock against
// the hash recorded when that block was processed. A mismatch means a
// reorg happened at or below lastBlock — same height or a few blocks deep,
// not necessarily a full reset (see the currentBlock < lastBlock check in
// syncBlocks, which only catches the latter). When detected, it rewinds up
// to ReorgDepth blocks (bounded by StartBlock) and persists the rewound
// position so the caller reprocesses from there, relying on idempotent
// listeners to make the re-delivery safe.
//
// No-op (returns lastBlock unchanged) if reorg detection is disabled, on a
// fresh start (lastBlock == 0), or if lastBlock's hash was never recorded
// (e.g. right after enabling ReorgDepth, or the block predates what's kept
// in memory).
func (idx *Indexer) detectReorgRewind(lastBlock uint64) (uint64, error) {
	if idx.reorgDepth <= 0 || lastBlock == 0 {
		return lastBlock, nil
	}

	wantHash, tracked := idx.recentHashes[lastBlock]
	if !tracked {
		return lastBlock, nil
	}

	header, err := idx.pool.HeaderByNumber(idx.ctx, new(big.Int).SetUint64(lastBlock))
	if err != nil {
		return lastBlock, fmt.Errorf("failed to verify chain tip for reorg check: %w", err)
	}
	if header.Hash() == wantHash {
		return lastBlock, nil
	}

	rewindTo := idx.config.StartBlock
	if uint64(idx.reorgDepth) < lastBlock {
		if candidate := lastBlock - uint64(idx.reorgDepth); candidate > rewindTo {
			rewindTo = candidate
		}
	}

	idx.logger.Warn("reorg detected: chain hash at last processed block no longer matches what was indexed, rewinding",
		"lastBlock", lastBlock, "rewindTo", rewindTo)

	if err := idx.storeLastBlock(rewindTo); err != nil {
		return lastBlock, fmt.Errorf("failed to persist rewound last block after reorg: %w", err)
	}
	idx.pruneHashesAbove(rewindTo)

	return rewindTo, nil
}

func (idx *Indexer) GetLastBlock() (uint64, error) {
	return idx.storage.GetLastBlock(idx.ctx, idx.config.Chain)
}

func (idx *Indexer) GetProviderScores() map[string]float64 {
	return idx.pool.GetScores()
}

func (idx *Indexer) GetEvents(fromBlock, toBlock uint64) ([]IndexedEvent, error) {
	return idx.storage.GetIndexedEvents(idx.ctx, idx.config.Chain, fromBlock, toBlock)
}

func (idx *Indexer) IsRunning() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.isRunning
}

func (idx *Indexer) Pool() ProviderPool                    { return idx.pool }
func (idx *Indexer) Storage() ChainStorage                 { return idx.storage }
func (idx *Indexer) Dispatcher() BlockchainEventDispatcher { return idx.dispatcher }
