package idx_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// flakyEthClient is a richer EthClient double than fakeEthClient: it can
// fail its first few HeaderByNumber calls (to exercise retry/backoff
// recovery), then behave normally, then be flipped into "permanently
// broken" mode (to exercise circuit-breaker tripping and, eventually,
// provider-pool exhaustion) — all without ever dialing a real node.
type flakyEthClient struct {
	mu sync.Mutex

	blockNumber             uint64
	headers                 map[uint64]*types.Header
	logsByBlock             map[uint64][]types.Log
	headerFailuresRemaining int
	forceFailure            bool
	forceErr                error
}

func newFlakyEthClient() *flakyEthClient {
	return &flakyEthClient{
		headers:     make(map[uint64]*types.Header),
		logsByBlock: make(map[uint64][]types.Log),
	}
}

func (f *flakyEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return nil, errNotImplemented
}
func (f *flakyEthClient) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return nil, errNotImplemented
}
func (f *flakyEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errNotImplemented
}
func (f *flakyEthClient) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return nil, errNotImplemented
}
func (f *flakyEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	return nil, errNotImplemented
}
func (f *flakyEthClient) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	return big.NewInt(0), nil
}
func (f *flakyEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, errNotImplemented
}
func (f *flakyEthClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}
func (f *flakyEthClient) Close() {}

func (f *flakyEthClient) BlockNumber(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceFailure {
		return 0, f.forceErr
	}
	return f.blockNumber, nil
}

func (f *flakyEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceFailure {
		return nil, f.forceErr
	}
	if f.headerFailuresRemaining > 0 {
		f.headerFailuresRemaining--
		return nil, errors.New("timeout: simulated transient upstream failure")
	}
	if h, ok := f.headers[number.Uint64()]; ok {
		return h, nil
	}
	return &types.Header{Number: number, Time: number.Uint64() * 1000}, nil
}

func (f *flakyEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceFailure {
		return nil, f.forceErr
	}
	from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
	var out []types.Log
	for b := from; b <= to; b++ {
		out = append(out, f.logsByBlock[b]...)
	}
	return out, nil
}

func (f *flakyEthClient) setBlockNumber(n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockNumber = n
}

func (f *flakyEthClient) addLog(blockNum uint64, logIndex uint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logsByBlock[blockNum] = append(f.logsByBlock[blockNum], types.Log{
		BlockNumber: blockNum,
		Index:       logIndex,
		Address:     common.BigToAddress(big.NewInt(int64(blockNum))),
		TxHash:      common.BigToHash(big.NewInt(int64(blockNum*1000 + uint64(logIndex)))),
	})
}

func (f *flakyEthClient) setForceFailure(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forceFailure = true
	f.forceErr = err
}

var _ EthClient = (*flakyEthClient)(nil)

// persistingListener writes every log it receives into a ChainStorage as an
// IndexedEvent — standing in for a real consumer that projects raw chain
// logs into its own domain/event store. It only needs ChainStorage, not
// the full Storage — proof that a listener doesn't have to care whether
// the backing store also does score/quota bookkeeping.
type persistingListener struct {
	storage ChainStorage
	chain   string

	mu    sync.Mutex
	count int
}

func (l *persistingListener) HandleLog(log types.Log, blockTimestamp uint64) error {
	l.mu.Lock()
	l.count++
	l.mu.Unlock()

	return l.storage.SaveIndexedEvent(context.Background(), &IndexedEvent{
		Chain:       l.chain,
		BlockNumber: log.BlockNumber,
		TxHash:      log.TxHash.Hex(),
		Address:     log.Address.Hex(),
		LogIndex:    log.Index,
	})
}

func (l *persistingListener) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// TestIntegration_FullPipeline wires up real Pool + Provider + Memory +
// Indexer + EventDispatcher (only the JSON-RPC transport is faked) and
// drives them through a realistic lifecycle in five phases:
//
//  1. Bootstrap: one provider is permanently down at dial time (higher
//     priority, so failover must actually happen), the other is flaky —
//     its first couple of calls fail transiently and must be recovered by
//     the retry/backoff layer without surfacing an error.
//  2. Sequential catch-up from a configured StartBlock on a small lag.
//  3. A burst of new blocks pushes the lag over BatchLagThreshold,
//     switching to the chunked eth_getLogs batch path (chunk size forced
//     small so multiple chunks are exercised).
//  4. Assert that provider health scores and quota usage were persisted to
//     Storage as a side effect of normal syncing, and that every dispatched
//     log was durably recorded exactly once.
//  5. The healthy provider is switched to permanent failure; enough
//     consecutive failures must trip its circuit breaker, at which point
//     the pool has no available provider at all — the Indexer must detect
//     that and invoke WithOnExhausted with an error wrapping
//     ErrProviderPoolExhausted, the library's "please restart me" signal.
func TestIntegration_FullPipeline(t *testing.T) {
	const chain = "integration-chain"

	offlineDial := testDial(nil, errors.New("connection refused: endpoint offline"))
	flaky := newFlakyEthClient()
	flaky.headerFailuresRemaining = 2 // recovered within RetryConfig.MaxAttempts below

	pool := NewPool(PoolConfig{
		UpdateInterval:   5 * time.Millisecond,
		MaxLogBlockRange: 4, // global, applies to every provider: forces multiple chunks in the batch phase
		Providers: []ProviderConfig{
			{
				// Tried first (lower Priority number) but always down —
				// proves the pool fails over past a dead top-priority
				// provider instead of getting stuck on it.
				Name:     "offline",
				URL:      "http://offline.invalid",
				Priority: 1,
				Dial:     offlineDial,
				Retry:    RetryConfig{MaxAttempts: 1},
			},
			{
				Name:      "primary",
				URL:       "http://primary.invalid",
				Priority:  2,
				Dial:      testDial(flaky, nil),
				RateLimit: RateLimitConfig{Enabled: true, RPS: 1000, Burst: 1000},
				CircuitBreaker: CircuitBreakerConfig{
					Enabled: true, Threshold: 3, Timeout: 10 * time.Second, HalfOpenMaxCalls: 1,
				},
				Retry: RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
				Quota: QuotaConfig{Limit: 10_000, Period: time.Hour},
			},
		},
	})
	defer pool.Close()

	// The "offline" provider must never be usable, from the very first check.
	for _, p := range pool.GetAllProviders() {
		if p.Name() == "offline" && p.IsAvailable() {
			t.Fatal("expected the dial-failed 'offline' provider to never be available")
		}
	}

	store := NewMemory()
	counter := &countingListener{}
	persister := &persistingListener{storage: store, chain: chain}
	dispatcher := NewEventDispatcher([]BlockchainListener{counter, persister})

	exhausted := make(chan error, 1)
	indexer, err := NewIndexer(
		IndexerConfig{
			Chain:                            chain,
			StartBlock:                       3,
			BlockInterval:                    5 * time.Millisecond,
			BatchLagThreshold:                8,
			MaxConsecutiveProviderExhaustion: 2,
			ProviderExhaustionRestartDelay:   time.Millisecond,
		},
		pool, store, dispatcher,
		WithLogger(silentLogger()),
		WithOnExhausted(func(err error) {
			select {
			case exhausted <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewIndexer failed: %v", err)
	}

	if err := indexer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer indexer.Stop()

	// --- Phase 2: sequential catch-up, blocks 3..5, with 2 transient
	// header failures along the way that retry must absorb silently. ---
	flaky.setBlockNumber(5)
	flaky.addLog(3, 0)
	flaky.addLog(5, 0)
	flaky.addLog(5, 1)
	indexer.Nudge()

	waitFor(t, 2*time.Second, func() bool {
		last, _ := indexer.GetLastBlock()
		return last == 5
	})
	if got := counter.count(); got != 3 {
		t.Fatalf("phase 2: expected 3 dispatched logs (blocks 3 and 5), got %d", got)
	}

	// --- Phase 3: a burst of 20 new blocks pushes lag (20) past
	// BatchLagThreshold (8), forcing the chunked eth_getLogs path with
	// MaxLogBlockRange=4 (5 chunks for blocks 6..25). ---
	flaky.setBlockNumber(25)
	for b := uint64(6); b <= 25; b++ {
		flaky.addLog(b, 0)
	}
	indexer.Nudge()

	waitFor(t, 2*time.Second, func() bool {
		last, _ := indexer.GetLastBlock()
		return last == 25
	})

	const totalLogs = 3 + 20
	if got := counter.count(); got != totalLogs {
		t.Fatalf("phase 3: expected %d total dispatched logs, got %d", totalLogs, got)
	}
	if got := persister.Count(); got != totalLogs {
		t.Fatalf("phase 3: expected %d logs handled by the persisting listener, got %d", totalLogs, got)
	}

	// --- Phase 4: side effects landed in Storage, not just the listener. ---
	events, err := store.GetIndexedEvents(context.Background(), chain, 0, 1000)
	if err != nil {
		t.Fatalf("GetIndexedEvents failed: %v", err)
	}
	if len(events) != totalLogs {
		t.Fatalf("expected %d persisted IndexedEvents, got %d", totalLogs, len(events))
	}
	if events[0].BlockNumber != 3 || events[len(events)-1].BlockNumber != 25 {
		t.Fatalf("expected events sorted from block 3 to block 25, got first=%d last=%d",
			events[0].BlockNumber, events[len(events)-1].BlockNumber)
	}

	score, err := store.GetProviderScore(context.Background(), "primary")
	if err != nil {
		t.Fatalf("GetProviderScore failed: %v", err)
	}
	if score == nil {
		t.Fatal("expected the primary provider's health score to have been persisted during normal syncing")
	}

	quota, err := store.GetQuotaUsage(context.Background(), "primary", "requests")
	if err != nil {
		t.Fatalf("GetQuotaUsage failed: %v", err)
	}
	if quota == nil || quota.Used == 0 {
		t.Fatalf("expected non-zero persisted quota usage for the primary provider, got %+v", quota)
	}

	// --- Phase 5: the only working provider goes permanently bad. Enough
	// consecutive circuit-breaker-tripping failures (Threshold=3) must open
	// its breaker; with "offline" already unusable, the pool then has zero
	// available providers, and after MaxConsecutiveProviderExhaustion (2)
	// consecutive sync attempts the Indexer must fire WithOnExhausted with
	// the ErrProviderPoolExhausted domain signal. ---
	flaky.setForceFailure(errors.New("connection refused: primary went dark"))

	select {
	case err := <-exhausted:
		if !errors.Is(err, ErrProviderPoolExhausted) {
			t.Fatalf("expected the exhaustion error to wrap ErrProviderPoolExhausted, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected WithOnExhausted to fire once every provider became unavailable")
	}

	// The chain must not have silently advanced past block 25 once the
	// only working provider went dark.
	if last, _ := indexer.GetLastBlock(); last != 25 {
		t.Fatalf("expected no further progress once the pool was exhausted, last block = %d", last)
	}

	indexer.Stop()
	if indexer.IsRunning() {
		t.Fatal("expected IsRunning() == false after Stop")
	}
}
