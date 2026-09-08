package idx_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// fakePool is a minimal, concurrency-safe ProviderPool double letting tests
// drive the Indexer's sync loop deterministically without a real Provider,
// RPC endpoint, or network I/O.
type fakePool struct {
	mu sync.Mutex

	available      bool
	blockNumber    uint64
	blockNumberErr error
	headers        map[uint64]*types.Header
	headerErr      error
	logsByBlock    map[uint64][]types.Log
	logsErr        error
	maxRange       int

	// maxAcceptedRange, if non-zero, makes FilterLogs reject any query
	// spanning more blocks than this with a "range too large" error —
	// simulating a request that ends up served by a provider with a
	// smaller limit than the one syncBlocksBatched sized the chunk for.
	maxAcceptedRange int

	// oversizedRangeErrMsg overrides the error text FilterLogs returns once
	// maxAcceptedRange is exceeded. Empty (the default) keeps the original
	// "range too large: max %d blocks" wording, which the package's
	// built-in isRangeTooLargeError keyword set already recognizes; tests
	// exercising a pool's own custom RangeTooLargeFilters (via
	// rangeTooLargeFilters below) set this to wording the built-in set
	// would miss, e.g. dRPC's "ranges over N blocks are not supported on
	// free plan".
	oversizedRangeErrMsg string

	// rangeTooLargeFilters, if non-empty, makes fakePool implement
	// RangeTooLargeClassifier using these substrings (matched
	// case-insensitively) in addition to a same-defaults fallback mirroring
	// the package's own built-in keyword set — see IsRangeTooLargeError.
	// Left nil (the default), classification still falls back to that same
	// built-in-equivalent set, so existing tests that never touch this
	// field keep seeing identical behaviour to before this field existed.
	rangeTooLargeFilters []string

	blockNumberCalls int
	logsCalls        int
}

// IsRangeTooLargeError implements RangeTooLargeClassifier so tests can
// exercise Indexer's use of a pool-specific classifier (see
// PoolConfig.RangeTooLargeFilters) without needing a real *Pool. Mirrors
// the package's own isRangeTooLargeError default keyword set as a fallback,
// re-implemented here since that helper is unexported and this test package
// only has access to idx's public surface.
func (p *fakePool) IsRangeTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "range") {
		return false
	}
	for _, kw := range append([]string{"large", "limit", "exceed", "too many"}, p.rangeTooLargeFilters...) {
		if strings.Contains(s, strings.ToLower(kw)) {
			return true
		}
	}
	return false
}

// newFakePool defaults maxRange to 1, mirroring PoolConfig.MaxLogBlockRange's
// real default — so tests get the sequential sync path unless they
// explicitly opt into batching by setting p.maxRange > 1 (see
// TestIndexer_BatchesAcrossMultipleChunks).
func newFakePool() *fakePool {
	return &fakePool{
		available:   true,
		headers:     make(map[uint64]*types.Header),
		logsByBlock: make(map[uint64][]types.Log),
		maxRange:    1,
	}
}

func (p *fakePool) GetProvider() RPCProvider {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.available {
		return nil
	}
	return &fakeProviderHandle{name: "fake"}
}

func (p *fakePool) GetAllProviders() []RPCProvider                { return []RPCProvider{p.GetProvider()} }
func (p *fakePool) RecordSuccess(provider RPCProvider)            {}
func (p *fakePool) RecordFailure(provider RPCProvider, err error) {}
func (p *fakePool) GetScores() map[string]float64                 { return map[string]float64{"fake": 1} }
func (p *fakePool) PersistQuotaUsage(ctx context.Context, storage QuotaStorage) error {
	return storage.SetQuotaUsage(ctx, "fake", "requests", 0, time.Now())
}
func (p *fakePool) Close() {}

func (p *fakePool) BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error) {
	return nil, errNotImplemented
}
func (p *fakePool) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return nil, errNotImplemented
}
func (p *fakePool) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errNotImplemented
}
func (p *fakePool) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return nil, errNotImplemented
}

func (p *fakePool) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logsCalls++
	if p.logsErr != nil {
		return nil, p.logsErr
	}

	from := q.FromBlock.Uint64()
	to := q.ToBlock.Uint64()
	if p.maxAcceptedRange > 0 && to-from+1 > uint64(p.maxAcceptedRange) {
		if p.oversizedRangeErrMsg != "" {
			return nil, errors.New(p.oversizedRangeErrMsg)
		}
		return nil, fmt.Errorf("range too large: max %d blocks", p.maxAcceptedRange)
	}
	var out []types.Log
	for b := from; b <= to; b++ {
		out = append(out, p.logsByBlock[b]...)
	}
	return out, nil
}

func (p *fakePool) LogsByBlockNumber(ctx context.Context, blockNum uint64) ([]types.Log, error) {
	return p.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(blockNum),
		ToBlock:   new(big.Int).SetUint64(blockNum),
	})
}

func (p *fakePool) LogsByBlockRange(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error) {
	return p.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	})
}

func (p *fakePool) SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error) {
	return nil, errNotImplemented
}

func (p *fakePool) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.headerErr != nil {
		return nil, p.headerErr
	}
	if h, ok := p.headers[number.Uint64()]; ok {
		return h, nil
	}
	return &types.Header{Number: number, Time: number.Uint64() * 1000}, nil
}

func (p *fakePool) BlockNumber(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockNumberCalls++
	if p.blockNumberErr != nil {
		return 0, p.blockNumberErr
	}
	return p.blockNumber, nil
}

func (p *fakePool) BalanceAt(ctx context.Context, address common.Address) (*big.Int, error) {
	return big.NewInt(0), nil
}
func (p *fakePool) CodeAt(ctx context.Context, address common.Address) ([]byte, error) {
	return nil, nil
}
func (p *fakePool) MaxLogBlockRange() int { return p.maxRange }

func (p *fakePool) setBlockNumber(n uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockNumber = n
}

func (p *fakePool) setAvailable(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.available = v
}

func (p *fakePool) setHeaderErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.headerErr = err
}

// setHeader pins the header returned for a given block number — used to
// simulate a reorg by changing what HeaderByNumber returns for an
// already-processed height (a different header, thus a different hash,
// while the block number itself stays the same).
func (p *fakePool) setHeader(number uint64, h *types.Header) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.headers[number] = h
}

func (p *fakePool) addLog(blockNum uint64, l types.Log) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l.BlockNumber = blockNum
	p.logsByBlock[blockNum] = append(p.logsByBlock[blockNum], l)
}

func (p *fakePool) callCounts() (blockNumberCalls, logsCalls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blockNumberCalls, p.logsCalls
}

// fakeProviderHandle is a stand-in RPCProvider returned by fakePool.GetProvider.
type fakeProviderHandle struct{ name string }

func (h *fakeProviderHandle) Name() string          { return h.name }
func (h *fakeProviderHandle) Priority() int         { return 0 }
func (h *fakeProviderHandle) Score() float64        { return 1 }
func (h *fakeProviderHandle) SetScore(float64)      {}
func (h *fakeProviderHandle) IsAvailable() bool     { return true }
func (h *fakeProviderHandle) MaxLogBlockRange() int { return 0 }
func (h *fakeProviderHandle) RecordSuccess()        {}
func (h *fakeProviderHandle) RecordFailure(error)   {}
func (h *fakeProviderHandle) Close()                {}
func (h *fakeProviderHandle) BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errNotImplemented
}
func (h *fakeProviderHandle) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) BlockNumber(ctx context.Context) (uint64, error) { return 0, nil }
func (h *fakeProviderHandle) BalanceAt(ctx context.Context, address common.Address) (*big.Int, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) Call(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	return nil, errNotImplemented
}
func (h *fakeProviderHandle) CodeAt(ctx context.Context, address common.Address) ([]byte, error) {
	return nil, errNotImplemented
}

var _ ProviderPool = (*fakePool)(nil)
var _ RPCProvider = (*fakeProviderHandle)(nil)

// countingListener records every HandleLog call it receives.
type countingListener struct {
	mu   sync.Mutex
	logs []types.Log
}

func (l *countingListener) HandleLog(log types.Log, blockTimestamp uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, log)
	return nil
}

func (l *countingListener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.logs)
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newTestIndexer(t *testing.T, cfg IndexerConfig, pool ProviderPool, store ChainStorage, dispatcher BlockchainEventDispatcher, opts ...Option) *Indexer {
	t.Helper()
	if cfg.Chain == "" {
		cfg.Chain = "eth"
	}
	if cfg.BlockInterval == 0 {
		cfg.BlockInterval = 10 * time.Millisecond
	}
	opts = append([]Option{WithLogger(silentLogger())}, opts...)
	idx, err := NewIndexer(cfg, pool, store, dispatcher, opts...)
	if err != nil {
		t.Fatalf("NewIndexer failed: %v", err)
	}
	return idx
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestNewIndexer_ValidatesRequiredArguments(t *testing.T) {
	pool := newFakePool()
	store := NewMemory()
	dispatcher := NewEventDispatcher(nil)
	cfg := IndexerConfig{Chain: "eth", BlockInterval: time.Second}

	if _, err := NewIndexer(cfg, nil, store, dispatcher); err == nil {
		t.Fatal("expected an error for a nil pool")
	}
	if _, err := NewIndexer(cfg, pool, nil, dispatcher); err == nil {
		t.Fatal("expected an error for a nil storage")
	}
	if _, err := NewIndexer(cfg, pool, store, nil); err == nil {
		t.Fatal("expected an error for a nil dispatcher")
	}
	if _, err := NewIndexer(IndexerConfig{BlockInterval: time.Second}, pool, store, dispatcher); err == nil {
		t.Fatal("expected an error for a missing chain")
	}
	if _, err := NewIndexer(IndexerConfig{Chain: "eth"}, pool, store, dispatcher); err == nil {
		t.Fatal("expected an error for a zero BlockInterval")
	}
}

func TestIndexer_StartTwiceReturnsErrAlreadyRunning(t *testing.T) {
	pool := newFakePool()
	idx := newTestIndexer(t, IndexerConfig{}, pool, NewMemory(), NewEventDispatcher(nil))
	defer idx.Stop()

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error on first Start: %v", err)
	}
	if err := idx.Start(); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("expected ErrAlreadyRunning, got %v", err)
	}
	if !idx.IsRunning() {
		t.Fatal("expected IsRunning() == true")
	}
}

func TestIndexer_SyncsSequentiallyAndPersistsProgress(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(3)
	pool.addLog(2, types.Log{Address: common.HexToAddress("0x1")})
	pool.addLog(3, types.Log{Address: common.HexToAddress("0x2")})

	listener := &countingListener{}
	dispatcher := NewEventDispatcher([]BlockchainListener{listener})
	store := NewMemory()

	idx := newTestIndexer(t, IndexerConfig{StartBlock: 1}, pool, store, dispatcher)

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 3
	})

	if got := listener.count(); got != 2 {
		t.Fatalf("expected 2 dispatched logs, got %d", got)
	}
}

func TestIndexer_NudgeTriggersImmediateSync(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(1)

	store := NewMemory()
	idx := newTestIndexer(t, IndexerConfig{
		StartBlock:    0,
		BlockInterval: time.Hour, // effectively "never" without a nudge
	}, pool, store, NewEventDispatcher(nil))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 1
	})
}

func TestIndexer_ChainResetRewindsToStartBlock(t *testing.T) {
	pool := newFakePool()
	store := NewMemory()
	_ = store.SetLastBlock(context.Background(), "eth", 100)
	pool.setBlockNumber(10) // simulates a chain reset: head (10) is behind the persisted last block (100)

	idx := newTestIndexer(t, IndexerConfig{StartBlock: 5}, pool, store, NewEventDispatcher(nil))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 10
	})
}

func TestIndexer_FreshStartUsesConfiguredStartBlock(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(1000)
	store := NewMemory()
	idx := newTestIndexer(t, IndexerConfig{StartBlock: 990}, pool, store, NewEventDispatcher(nil))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 1000
	})

	// Blocks 990..1000 inclusive is 11 blocks. If StartBlock were ignored,
	// indexing would have started at block 1 and made far more calls.
	// Give the loop a brief moment to settle (currentBlock<=lastBlock makes
	// further ticks no-ops) before reading the final call count.
	time.Sleep(20 * time.Millisecond)
	if _, logsCalls := pool.callCounts(); logsCalls != 11 {
		t.Fatalf("expected exactly 11 blocks processed starting at StartBlock, got %d FilterLogs calls", logsCalls)
	}
}

func TestIndexer_BatchesAcrossMultipleChunks(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(20)
	pool.maxRange = 5 // > 1 selects the batched path; 20 blocks / chunk size 5 = 4 chunks
	for b := uint64(1); b <= 20; b++ {
		pool.addLog(b, types.Log{Address: common.HexToAddress("0x1")})
	}

	listener := &countingListener{}
	store := NewMemory()
	idx := newTestIndexer(t, IndexerConfig{
		StartBlock: 0,
	}, pool, store, NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 20
	})

	if got := listener.count(); got != 20 {
		t.Fatalf("expected 20 dispatched logs, got %d", got)
	}
	if _, logsCalls := pool.callCounts(); logsCalls == 0 {
		t.Fatal("expected the batched path to call FilterLogs")
	}
}

func TestIndexer_BatchedSyncSplitsChunkOnRangeTooLargeError(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(20)
	pool.maxRange = 20        // chunk sized for a high-limit provider...
	pool.maxAcceptedRange = 5 // ...but the request actually lands on one with a smaller limit
	for b := uint64(1); b <= 20; b++ {
		pool.addLog(b, types.Log{Address: common.HexToAddress("0x1")})
	}

	listener := &countingListener{}
	store := NewMemory()
	idx := newTestIndexer(t, IndexerConfig{
		StartBlock: 0,
	}, pool, store, NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 20
	})

	if got := listener.count(); got != 20 {
		t.Fatalf("expected all 20 logs to still be dispatched despite the oversized first attempt, got %d", got)
	}
	if _, logsCalls := pool.callCounts(); logsCalls <= 4 {
		t.Fatalf("expected more than the 4 chunks a clean 20/5 split would need, since the initial 20-block chunk had to be rejected and split first, got %d calls", logsCalls)
	}
}

// TestIndexer_BatchedSyncSplitsChunkOnPoolCustomRangeTooLargeWording proves
// the fix for the dRPC bug report: its free-plan rejection reads "ranges
// over 10000 blocks are not supported on free plan" — "range" is there, but
// none of the package's built-in isRangeTooLargeError keywords
// ("large"/"limit"/"exceed"/"too many") are, so without a pool-specific
// classifier this would be treated as an ordinary chunk failure instead of
// being split and retried. Configuring the pool's RangeTooLargeFilters
// (here simulated via fakePool.rangeTooLargeFilters, since fakePool isn't a
// real *Pool) closes that gap: Indexer.isRangeTooLargeError prefers the
// pool's own RangeTooLargeClassifier over the package default.
func TestIndexer_BatchedSyncSplitsChunkOnPoolCustomRangeTooLargeWording(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(20)
	pool.maxRange = 20
	pool.maxAcceptedRange = 5
	pool.oversizedRangeErrMsg = "ranges over 5 blocks are not supported on free plan"
	pool.rangeTooLargeFilters = []string{"not supported on", "free plan"}
	for b := uint64(1); b <= 20; b++ {
		pool.addLog(b, types.Log{Address: common.HexToAddress("0x1")})
	}

	listener := &countingListener{}
	store := NewMemory()
	idx := newTestIndexer(t, IndexerConfig{
		StartBlock: 0,
	}, pool, store, NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 20
	})

	if got := listener.count(); got != 20 {
		t.Fatalf("expected all 20 logs dispatched via split-and-retry recognizing the pool's custom wording, got %d", got)
	}
}

func TestIndexer_ProviderExhaustionInvokesOnExhausted(t *testing.T) {
	pool := newFakePool()
	pool.setAvailable(false)

	invoked := make(chan error, 1)
	idx := newTestIndexer(t, IndexerConfig{
		BlockInterval:                    2 * time.Millisecond,
		MaxConsecutiveProviderExhaustion: 2,
		ProviderExhaustionRestartDelay:   time.Millisecond,
	}, pool, NewMemory(), NewEventDispatcher(nil), WithOnExhausted(func(err error) {
		select {
		case invoked <- err:
		default:
		}
	}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	select {
	case err := <-invoked:
		if !errors.Is(err, ErrProviderPoolExhausted) {
			t.Fatalf("expected the OnExhausted error to wrap ErrProviderPoolExhausted, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected OnExhausted to be invoked once the threshold was crossed")
	}
}

func TestIndexer_DebugModeReprocessesFromGenesisEveryCycle(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(2)
	pool.addLog(1, types.Log{Address: common.HexToAddress("0x1")})

	store := NewMemory()
	_ = store.SetLastBlock(context.Background(), "eth", 500)

	listener := &countingListener{}
	idx := newTestIndexer(t, IndexerConfig{
		DebugMode:     true,
		BlockInterval: 5 * time.Millisecond,
	}, pool, store, NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	// If DebugMode were not re-processing from genesis every cycle, the
	// listener count would plateau at 1 once currentBlock<=lastBlock. It
	// should instead keep climbing as each tick re-dispatches block 1.
	waitFor(t, time.Second, func() bool { return listener.count() >= 3 })

	last, err := idx.GetLastBlock()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if last != 500 {
		t.Fatalf("expected DebugMode to leave persisted last block untouched at 500, got %d", last)
	}
}

func TestIndexer_AccessorsExposeCollaborators(t *testing.T) {
	pool := newFakePool()
	store := NewMemory()
	dispatcher := NewEventDispatcher(nil)
	idx := newTestIndexer(t, IndexerConfig{}, pool, store, dispatcher)

	if idx.Pool() != pool {
		t.Fatal("expected Pool() to return the injected pool")
	}
	if idx.Storage() != store {
		t.Fatal("expected Storage() to return the injected storage")
	}
	if idx.Dispatcher() != dispatcher {
		t.Fatal("expected Dispatcher() to return the injected dispatcher")
	}
}

func TestIndexer_StopIsIdempotentSafe(t *testing.T) {
	pool := newFakePool()
	idx := newTestIndexer(t, IndexerConfig{}, pool, NewMemory(), NewEventDispatcher(nil))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	idx.Stop()
	if idx.IsRunning() {
		t.Fatal("expected IsRunning() == false after Stop")
	}

	// Calling Stop again must not panic or re-run teardown (double
	// dispatcher.Close()/pool.Close()).
	idx.Stop()
}

// TestIndexer_StartAfterStopReturnsErrIndexerStopped proves the fix for a
// previously silent lifecycle bug: Stop cancels the Indexer's internal
// context for good, so a subsequent Start used to return nil and launch a
// sync loop whose very first select on ctx.Done() would fire immediately,
// silently doing nothing forever. Start must now reject it explicitly.
func TestIndexer_StartAfterStopReturnsErrIndexerStopped(t *testing.T) {
	pool := newFakePool()
	idx := newTestIndexer(t, IndexerConfig{}, pool, NewMemory(), NewEventDispatcher(nil))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	idx.Stop()

	if err := idx.Start(); !errors.Is(err, ErrIndexerStopped) {
		t.Fatalf("expected ErrIndexerStopped, got %v", err)
	}
	if idx.IsRunning() {
		t.Fatal("expected IsRunning() == false: Start after Stop must not launch the sync loop")
	}
}
