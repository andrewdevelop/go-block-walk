package idx

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"
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
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	exhaustion  *ExhaustionTracker
	onExhausted func(error)

	nudge *NudgeSignal
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
	}

	for _, opt := range opts {
		opt(idx)
	}

	return idx, nil
}

func (idx *Indexer) Start() error {
	idx.mu.Lock()
	if idx.isRunning {
		idx.mu.Unlock()
		return ErrAlreadyRunning
	}
	idx.isRunning = true
	idx.mu.Unlock()

	idx.wg.Add(1)
	go idx.syncLoop()

	return nil
}

// Stop cancels the sync loop, waits for it to exit, and closes the
// dispatcher and pool (but not the storage — callers that share it with
// other components close it themselves).
func (idx *Indexer) Stop() {
	idx.cancel()
	idx.wg.Wait()

	idx.mu.Lock()
	idx.isRunning = false
	idx.mu.Unlock()

	idx.dispatcher.Close()
	idx.pool.Close()
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
			if isUnsupportedTxType(err) {
				idx.logger.Warn("skipping block: unsupported transaction type (chain upgrade ahead of go-ethereum)", "block", blockNum)
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
func (idx *Indexer) syncBlocksBatched(startBlock, currentBlock uint64) error {
	for chunkStart := startBlock; chunkStart <= currentBlock; {
		chunkSize := uint64(idx.pool.MaxLogBlockRange())
		if chunkSize == 0 {
			chunkSize = 1
		}

		chunkEnd := chunkStart + chunkSize - 1
		if chunkEnd > currentBlock {
			chunkEnd = currentBlock
		}

		idx.logger.Info("fetching logs for chunk", "from", chunkStart, "to", chunkEnd)

		logs, err := idx.pool.LogsByBlockRange(idx.ctx, chunkStart, chunkEnd)
		if err != nil {
			return fmt.Errorf("failed to get logs for blocks %d..%d: %w", chunkStart, chunkEnd, err)
		}

		blockTimes := make(map[uint64]uint64, len(logs))
		for _, l := range logs {
			blockTime, ok := blockTimes[l.BlockNumber]
			if !ok {
				header, err := idx.pool.HeaderByNumber(idx.ctx, new(big.Int).SetUint64(l.BlockNumber))
				if err != nil {
					if isUnsupportedTxType(err) {
						idx.logger.Warn("skipping block: unsupported transaction type (chain upgrade ahead of go-ethereum)", "block", l.BlockNumber)
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
				idx.logger.Error("error dispatching log", "error", err)
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

	logs, err := idx.pool.LogsByBlockNumber(idx.ctx, blockNum)
	if err != nil {
		idx.logger.Error("LogsByBlockNumber error", "block", blockNum, "error", err)
		return nil
	}

	for _, l := range logs {
		if err := idx.dispatcher.Dispatch(l, blockTime); err != nil {
			idx.logger.Error("error dispatching log", "error", err)
		}
	}

	return nil
}

func isUnsupportedTxType(err error) bool {
	return err != nil && strings.Contains(err.Error(), "transaction type not supported")
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
