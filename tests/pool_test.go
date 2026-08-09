package idx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

func poolProviderConfig(name string, priority int, client *fakeEthClient) ProviderConfig {
	return ProviderConfig{
		Name:     name,
		Priority: priority,
		Dial:     testDial(client, nil),
		Retry:    RetryConfig{MaxAttempts: 1},
	}
}

func TestPool_GetProviderPicksHighestPriorityFirst(t *testing.T) {
	c1, c2 := newFakeEthClient(), newFakeEthClient()
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		poolProviderConfig("low", 10, c1),
		poolProviderConfig("high", 1, c2),
	}})
	defer pool.Close()

	p := pool.GetProvider()
	if p == nil {
		t.Fatal("expected a provider")
	}
	if p.Name() != "high" {
		t.Fatalf("expected the lowest-priority-number provider ('high') to be picked first, got %q", p.Name())
	}
}

func TestPool_SkipsUnavailableProviders(t *testing.T) {
	goodClient := newFakeEthClient()
	goodClient.setBlockNumber(99)

	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		{Name: "bad", Priority: 1, Dial: testDial(nil, errors.New("dial error")), Retry: RetryConfig{MaxAttempts: 1}},
		poolProviderConfig("good", 2, goodClient),
	}})
	defer pool.Close()

	n, err := pool.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 99 {
		t.Fatalf("expected the healthy provider's block number 99, got %d", n)
	}
}

func TestPool_NoAvailableProviderReturnsError(t *testing.T) {
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		{Name: "bad", Priority: 1, Dial: testDial(nil, errors.New("dial error")), Retry: RetryConfig{MaxAttempts: 1}},
	}})
	defer pool.Close()

	if _, err := pool.BlockNumber(context.Background()); !errors.Is(err, ErrNoAvailableProvider) {
		t.Fatalf("expected ErrNoAvailableProvider, got %v", err)
	}
	if pool.GetProvider() != nil {
		t.Fatal("expected GetProvider() to return nil")
	}
}

func TestPool_FailureFallsBackToNextProvider(t *testing.T) {
	failing := newFakeEthClient()
	failing.setBlockNumberErr(errors.New("execution reverted")) // non-retryable-ish, single attempt then fail
	working := newFakeEthClient()
	working.setBlockNumber(7)

	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		poolProviderConfig("failing", 1, failing),
		poolProviderConfig("working", 2, working),
	}})
	defer pool.Close()

	// First call: "failing" has priority 1 so it's picked and fails; Pool
	// itself does not automatically fail over within a single call, it
	// records the failure and returns the error. Repeated calls, however,
	// eventually favour "working" once "failing"'s score drops.
	for i := 0; i < 20; i++ {
		_, _ = pool.BlockNumber(context.Background())
	}

	n, err := pool.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("expected the pool to have failed over to the working provider, got error: %v", err)
	}
	if n != 7 {
		t.Fatalf("expected block number 7 from the working provider, got %d", n)
	}
}

func TestPool_RecordSuccessAndFailureRouteToNamedProvider(t *testing.T) {
	client := newFakeEthClient()
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("p1", 1, client)}})
	defer pool.Close()

	p := pool.GetProvider()
	before := p.Score()

	pool.RecordSuccess(p)
	if p.Score() <= before {
		t.Fatal("expected RecordSuccess to raise the provider's score")
	}

	afterSuccess := p.Score()
	pool.RecordFailure(p, errors.New("boom"))
	if p.Score() >= afterSuccess {
		t.Fatal("expected RecordFailure to lower the provider's score")
	}
}

func TestPool_MaxLogBlockRangeDefaultsToOne(t *testing.T) {
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("a", 1, newFakeEthClient())}})
	defer pool.Close()

	if got := pool.MaxLogBlockRange(); got != DefaultMaxLogBlockRange {
		t.Fatalf("expected the default (%d), got %d", DefaultMaxLogBlockRange, got)
	}
}

func TestPool_MaxLogBlockRangeAppliesGloballyToAllProviders(t *testing.T) {
	pool := NewPool(PoolConfig{
		MaxLogBlockRange: 777,
		Providers: []ProviderConfig{
			poolProviderConfig("a", 1, newFakeEthClient()),
			poolProviderConfig("b", 2, newFakeEthClient()),
		},
	})
	defer pool.Close()

	// The configured value applies uniformly, regardless of which provider
	// (if any) is currently selected — there's no more per-provider value
	// to fall back on.
	if got := pool.MaxLogBlockRange(); got != 777 {
		t.Fatalf("expected 777, got %d", got)
	}
}

func TestPool_LogsByBlockNumberAndRange(t *testing.T) {
	client := newFakeEthClient()
	client.setLogs([]types.Log{{BlockNumber: 5}})
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("a", 1, client)}})
	defer pool.Close()

	logs, err := pool.LogsByBlockNumber(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	logs, err = pool.LogsByBlockRange(context.Background(), 1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}
}

func TestPool_PersistQuotaUsage(t *testing.T) {
	client := newFakeEthClient()
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("p1", 1, client)}})
	defer pool.Close()

	store := NewMemory()
	if err := pool.PersistQuotaUsage(context.Background(), store); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	usage, err := store.GetQuotaUsage(context.Background(), "p1", "requests")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage == nil {
		t.Fatal("expected quota usage to have been persisted")
	}
}

func TestPool_GetScoresReturnsAllProviders(t *testing.T) {
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		poolProviderConfig("a", 1, newFakeEthClient()),
		poolProviderConfig("b", 2, newFakeEthClient()),
	}})
	defer pool.Close()

	scores := pool.GetScores()
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}
	if _, ok := scores["a"]; !ok {
		t.Fatal("expected score for provider 'a'")
	}
	if _, ok := scores["b"]; !ok {
		t.Fatal("expected score for provider 'b'")
	}
}

func TestPool_RecalculateScoresAppliesQuotaPenalty(t *testing.T) {
	client := newFakeEthClient()
	pool := NewPool(PoolConfig{
		Providers: []ProviderConfig{{
			Name:     "p1",
			Priority: 1,
			Dial:     testDial(client, nil),
			Retry:    RetryConfig{MaxAttempts: 1},
			Quota:    QuotaConfig{Limit: 1, Period: time.Hour},
		}},
		// Short UpdateInterval so the background recalculation loop applies
		// the quota penalty within the test's polling window, instead of
		// reaching into the unexported recalculateScores method directly.
		UpdateInterval: 5 * time.Millisecond,
	})
	defer pool.Close()

	// Exhaust the provider's quota.
	if _, err := pool.BlockNumber(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if pool.GetScores()["p1"] < 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected quota exhaustion to eventually apply a penalty lowering the score below the base priority, got %v", pool.GetScores()["p1"])
}

func TestPool_CloseClosesAllProviders(t *testing.T) {
	c1, c2 := newFakeEthClient(), newFakeEthClient()
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		poolProviderConfig("a", 1, c1),
		poolProviderConfig("b", 2, c2),
	}})

	pool.Close()

	if got1, got2 := atomic.LoadInt32(&c1.closed), atomic.LoadInt32(&c2.closed); got1 != 1 || got2 != 1 {
		t.Fatalf("expected both clients closed, got c1=%d c2=%d", got1, got2)
	}
}

func TestPool_ConcurrentUseNoRace(t *testing.T) {
	client := newFakeEthClient()
	client.setBlockNumber(1)
	client.setLogs([]types.Log{{BlockNumber: 1}})
	pool := NewPool(PoolConfig{
		Providers:      []ProviderConfig{poolProviderConfig("a", 1, client)},
		UpdateInterval: time.Millisecond,
	})
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pool.BlockNumber(context.Background())
			_, _ = pool.LogsByBlockNumber(context.Background(), 1)
			_ = pool.GetScores()
			_ = pool.GetAllProviders()
		}()
	}
	wg.Wait()
}
