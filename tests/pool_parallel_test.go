package idx_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// rangeAwareEthClient is a fakeEthClient variant whose FilterLogs actually
// respects the requested [FromBlock, ToBlock] — needed to verify that a
// parallel backfill round covers the right sub-ranges, in the right order,
// rather than just checking a call count against a fixed canned response.
type rangeAwareEthClient struct {
	mu sync.Mutex

	failAlways bool

	// sharedFailBudget, if set, is decremented (down to -1) on every
	// FilterLogs call across every client that shares the same pointer —
	// modelling a handful of transient failures tied to whichever provider
	// happens to be asking, not to a specific client instance. Tests use
	// this instead of a per-client failCount because with multiple workers
	// pulling jobs off a shared queue, which client sees which failure is
	// scheduler-dependent; a shared budget makes the total failure count
	// (and therefore whether a per-job attempt limit is exceeded) the only
	// thing that matters, keeping the test deterministic regardless of
	// goroutine ordering.
	sharedFailBudget *int32

	calls int32
}

func (f *rangeAwareEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	atomic.AddInt32(&f.calls, 1)

	f.mu.Lock()
	failAlways := f.failAlways
	f.mu.Unlock()
	if failAlways {
		return nil, errors.New("boom: provider down")
	}
	if f.sharedFailBudget != nil {
		if atomic.AddInt32(f.sharedFailBudget, -1) >= 0 {
			return nil, errors.New("boom: transient failure")
		}
	}

	from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
	logs := make([]types.Log, 0, to-from+1)
	for b := from; b <= to; b++ {
		logs = append(logs, types.Log{BlockNumber: b})
	}
	return logs, nil
}

func (f *rangeAwareEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return nil, errNotImplemented
}
func (f *rangeAwareEthClient) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return nil, errNotImplemented
}
func (f *rangeAwareEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	return nil, false, errNotImplemented
}
func (f *rangeAwareEthClient) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return nil, errNotImplemented
}
func (f *rangeAwareEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	return nil, errNotImplemented
}
func (f *rangeAwareEthClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return &types.Header{Number: number}, nil
}
func (f *rangeAwareEthClient) BlockNumber(ctx context.Context) (uint64, error) { return 0, nil }
func (f *rangeAwareEthClient) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*big.Int, error) {
	return big.NewInt(0), nil
}
func (f *rangeAwareEthClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, errNotImplemented
}
func (f *rangeAwareEthClient) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}
func (f *rangeAwareEthClient) Close() {}

var _ EthClient = (*rangeAwareEthClient)(nil)

// parallelProviderConfig mirrors poolProviderConfig but for
// rangeAwareEthClient, and lets the test set a per-provider MaxLogBlockRange
// — the scenario in the feature request: infura/alchemy capped at 10 blocks,
// zan capped at 10000.
func parallelProviderConfig(name string, priority, maxLogBlockRange int, client *rangeAwareEthClient) ProviderConfig {
	return ProviderConfig{
		Name:             name,
		Priority:         priority,
		Dial:             testDial(client, nil),
		Retry:            RetryConfig{MaxAttempts: 1},
		MaxLogBlockRange: maxLogBlockRange,
	}
}

func TestPool_ParallelLogPlan_UsesSmallestAvailableProviderLimit(t *testing.T) {
	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("infura", 1, 10, &rangeAwareEthClient{}),
		parallelProviderConfig("alchemy", 2, 10, &rangeAwareEthClient{}),
		parallelProviderConfig("zan", 3, 10000, &rangeAwareEthClient{}),
	}})
	defer pool.Close()

	providers, chunkSize := pool.ParallelLogPlan()
	if providers != 3 {
		t.Fatalf("expected 3 available providers, got %d", providers)
	}
	if chunkSize != 10 {
		t.Fatalf("expected the smallest provider limit (10) to win, got %d", chunkSize)
	}
}

func TestPool_LogsByBlockRangeParallel_SplitsAcrossAllProvidersInOrder(t *testing.T) {
	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("infura", 1, 10, &rangeAwareEthClient{}),
		parallelProviderConfig("alchemy", 2, 10, &rangeAwareEthClient{}),
		parallelProviderConfig("zan", 3, 10000, &rangeAwareEthClient{}),
	}})
	defer pool.Close()

	// 30 blocks, 3 providers, chunk size 10: exactly one sub-range each.
	logs, err := pool.LogsByBlockRangeParallel(context.Background(), 1000, 1029)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 30 {
		t.Fatalf("expected 30 logs (one per block), got %d", len(logs))
	}
	for i, l := range logs {
		want := uint64(1000 + i)
		if l.BlockNumber != want {
			t.Fatalf("expected logs in ascending block order (sequence preserved), log %d: want block %d, got %d", i, want, l.BlockNumber)
		}
	}
}

func TestPool_LogsByBlockRangeParallel_ReassignsJobFromFailingProvider(t *testing.T) {
	failing := &rangeAwareEthClient{failAlways: true}
	good1 := &rangeAwareEthClient{}
	good2 := &rangeAwareEthClient{}

	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("infura", 1, 10, failing),
		parallelProviderConfig("alchemy", 2, 10, good1),
		parallelProviderConfig("zan", 3, 10, good2),
	}})
	defer pool.Close()

	// "infura" fails every call but its circuit breaker is disabled by
	// default (CircuitBreakerConfig.Enabled defaults to false), so it stays
	// "available" and keeps grabbing jobs and failing them — proving jobs
	// bounce to a working provider instead of the whole round failing, as
	// long as the per-job attempt limit isn't exhausted first.
	logs, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 30)
	if err != nil {
		t.Fatalf("expected the round to complete via the working providers, got error: %v", err)
	}
	if len(logs) != 30 {
		t.Fatalf("expected all 30 blocks' worth of logs despite one dead provider, got %d", len(logs))
	}
}

func TestPool_LogsByBlockRangeParallel_AllProvidersDownReturnsError(t *testing.T) {
	c1 := &rangeAwareEthClient{failAlways: true}
	c2 := &rangeAwareEthClient{failAlways: true}

	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("a", 1, 10, c1),
		parallelProviderConfig("b", 2, 10, c2),
	}})
	defer pool.Close()

	// Circuit breakers are disabled, so IsAvailable() never flips false and
	// "active" workers never drop to 0 — but the per-job attempt limit bounds a
	// single job's retries regardless, so the round still terminates with an
	// error instead of hanging forever.
	_, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 30)
	if err == nil {
		t.Fatal("expected an error once every provider consistently fails a job")
	}
}

func TestPool_LogsByBlockRangeParallel_FallsBackWhenBacklogTooSmall(t *testing.T) {
	c1 := &rangeAwareEthClient{}
	c2 := &rangeAwareEthClient{}

	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("infura", 1, 10, c1),
		parallelProviderConfig("alchemy", 2, 10, c2),
	}})
	defer pool.Close()

	// Only 5 blocks outstanding — at or below one provider's own chunk size,
	// so LogsByBlockRangeParallel should behave exactly like a plain
	// LogsByBlockRange (no fan-out needed), leaving the second provider idle.
	logs, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 5 {
		t.Fatalf("expected 5 logs, got %d", len(logs))
	}
	if atomic.LoadInt32(&c1.calls)+atomic.LoadInt32(&c2.calls) != 1 {
		t.Fatalf("expected exactly one FilterLogs call across both providers for a sub-threshold backlog, got infura=%d alchemy=%d",
			atomic.LoadInt32(&c1.calls), atomic.LoadInt32(&c2.calls))
	}
}

func TestPool_LogsByBlockRangeParallel_AttemptLimitDefaultsToTwiceProviderCount(t *testing.T) {
	// Two providers, two jobs (20 blocks / chunk size 10), and a failure
	// budget of 3 shared across both clients. By pigeonhole, at least one
	// job will see 2 of those 3 failures — but the default attempt limit for
	// a 2-provider round is 2*2=4, comfortably above that, so the round must
	// still complete successfully.
	budget := int32(3)
	c1 := &rangeAwareEthClient{sharedFailBudget: &budget}
	c2 := &rangeAwareEthClient{sharedFailBudget: &budget}

	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("a", 1, 10, c1),
		parallelProviderConfig("b", 2, 10, c2),
	}})
	defer pool.Close()

	logs, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 20)
	if err != nil {
		t.Fatalf("expected the default attempt limit (4) to absorb the shared failure budget, got error: %v", err)
	}
	if len(logs) != 20 {
		t.Fatalf("expected 20 logs, got %d", len(logs))
	}
}

func TestPool_LogsByBlockRangeParallel_MaxParallelJobAttemptsOverridesDefault(t *testing.T) {
	// Same setup, but MaxParallelJobAttempts is explicitly capped at 2. By
	// pigeonhole, the 3-failure shared budget spread across only 2 jobs
	// guarantees at least one job accumulates 2 failures — hitting the
	// override exactly, regardless of which worker happens to pick it up —
	// so the round must fail instead of eventually succeeding.
	budget := int32(3)
	c1 := &rangeAwareEthClient{sharedFailBudget: &budget}
	c2 := &rangeAwareEthClient{sharedFailBudget: &budget}

	pool := mustNewPool(t, PoolConfig{
		MaxParallelJobAttempts: 2,
		Providers: []ProviderConfig{
			parallelProviderConfig("a", 1, 10, c1),
			parallelProviderConfig("b", 2, 10, c2),
		},
	})
	defer pool.Close()

	_, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 20)
	if err == nil {
		t.Fatal("expected MaxParallelJobAttempts: 2 to cut the round off before the shared failure budget is exhausted")
	}
}

func TestPool_LogsByBlockRangeParallel_SingleAvailableProviderFallsBack(t *testing.T) {
	client := &rangeAwareEthClient{}
	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{
		parallelProviderConfig("only", 1, 10, client),
	}})
	defer pool.Close()

	providers, _ := pool.ParallelLogPlan()
	if providers != 1 {
		t.Fatalf("expected 1 available provider, got %d", providers)
	}

	logs, err := pool.LogsByBlockRangeParallel(context.Background(), 1, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 100 {
		t.Fatalf("expected 100 logs served sequentially by the lone provider, got %d", len(logs))
	}
}
