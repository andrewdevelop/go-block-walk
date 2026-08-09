package idx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

func testDial(client EthClient, err error) DialFunc {
	return func(ctx context.Context, rawurl string) (EthClient, error) {
		return client, err
	}
}

func newTestProvider(t *testing.T, client *fakeEthClient, cfg ProviderConfig) *Provider {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test-provider"
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 1
	}
	cfg.Dial = testDial(client, nil)
	return NewProvider(context.Background(), cfg)
}

func TestProvider_DialFailureMarksUnavailable(t *testing.T) {
	p := NewProvider(context.Background(), ProviderConfig{
		Name: "broken",
		Dial: testDial(nil, errors.New("connection refused")),
	})

	if p.IsAvailable() {
		t.Fatal("expected a provider whose dial failed to be unavailable")
	}
}

func TestProvider_DefaultDialFailsFastOnMalformedURL(t *testing.T) {
	// No Dial override: exercises the real default (go-ethereum's ethclient)
	// dial path against a URL it cannot even parse.
	p := NewProvider(context.Background(), ProviderConfig{
		Name: "malformed",
		URL:  "://not-a-valid-url",
	})

	if p.IsAvailable() {
		t.Fatal("expected a provider dialed with a malformed URL to be unavailable")
	}
}

func TestProvider_BlockNumberSuccessUpdatesScoreOnPoolLevel(t *testing.T) {
	client := newFakeEthClient()
	client.setBlockNumber(42)
	p := newTestProvider(t, client, ProviderConfig{Priority: 1})

	n, err := p.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 42 {
		t.Fatalf("expected block number 42, got %d", n)
	}
}

func TestProvider_RecordSuccessAndFailureAdjustScore(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{Priority: 5})

	if got := p.Score(); got != 5 {
		t.Fatalf("expected initial score == priority (5), got %v", got)
	}

	p.RecordSuccess()
	if got := p.Score(); got != 5.1 {
		t.Fatalf("expected score 5.1 after one success, got %v", got)
	}

	// Score should be capped at 2x base score.
	for i := 0; i < 200; i++ {
		p.RecordSuccess()
	}
	if got := p.Score(); got != 10 {
		t.Fatalf("expected score capped at 2x base score (10), got %v", got)
	}

	for i := 0; i < 100; i++ {
		p.RecordFailure(errors.New("boom"))
	}
	if p.IsAvailable() {
		t.Fatal("expected provider to become unavailable after enough failures")
	}

	p.RecordSuccess()
	if !p.IsAvailable() {
		t.Fatal("expected a success to restore availability")
	}
}

func TestProvider_IsAvailableHasNoSideEffectsOnQuota(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{
		Quota: QuotaConfig{Limit: 1, Period: time.Minute},
	})

	// Calling IsAvailable many times must not itself consume the quota.
	for i := 0; i < 50; i++ {
		if !p.IsAvailable() {
			t.Fatalf("iteration %d: expected provider to remain available; IsAvailable must not consume quota", i)
		}
	}

	limit, used, _, _ := p.GetQuotaUsage()
	if used != 0 {
		t.Fatalf("expected 0 quota used after only calling IsAvailable, got %d (limit=%d)", used, limit)
	}
}

func TestProvider_QuotaExhaustionBlocksRequests(t *testing.T) {
	client := newFakeEthClient()
	p := newTestProvider(t, client, ProviderConfig{
		Quota: QuotaConfig{Limit: 2, Period: time.Hour},
	})

	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("call 1: unexpected error: %v", err)
	}
	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("call 2: unexpected error: %v", err)
	}

	if p.IsAvailable() {
		t.Fatal("expected provider to be unavailable once quota is exhausted")
	}

	_, err := p.BlockNumber(context.Background())
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
}

func TestProvider_QuotaCreditsLimitBlocksRequests(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{
		Quota: QuotaConfig{
			Credits: &CreditsConfig{Enabled: true, PerRequest: 3, Limit: 5, Period: time.Hour},
		},
	})

	// First call consumes 3 of 5 credits — allowed.
	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Second call needs 3 more credits but only 2 remain — denied.
	if _, err := p.BlockNumber(context.Background()); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded once credits are exhausted, got %v", err)
	}
}

func TestProvider_QuotaPeriodRollover(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{
		Quota: QuotaConfig{Limit: 1, Period: 20 * time.Millisecond},
	})

	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.IsAvailable() {
		t.Fatal("expected quota to be exhausted immediately after the single allowed call")
	}

	time.Sleep(40 * time.Millisecond)

	if !p.IsAvailable() {
		t.Fatal("expected quota to roll over and become available again after the period elapses")
	}
	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("unexpected error after rollover: %v", err)
	}
}

func TestProvider_QuotaCreditsRollover(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{
		Quota: QuotaConfig{
			Credits: &CreditsConfig{Enabled: true, PerRequest: 5, Limit: 5, Period: 20 * time.Millisecond},
		},
	})

	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := p.BlockNumber(context.Background()); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected credits exhausted, got %v", err)
	}

	time.Sleep(40 * time.Millisecond)

	if _, err := p.BlockNumber(context.Background()); err != nil {
		t.Fatalf("expected credits to roll over and allow the call, got error: %v", err)
	}
}

func TestProvider_FilterLogsPropagatesResults(t *testing.T) {
	client := newFakeEthClient()
	client.setLogs([]types.Log{{BlockNumber: 1}, {BlockNumber: 2}})
	p := newTestProvider(t, client, ProviderConfig{})

	logs, err := p.FilterLogs(context.Background(), ethereum.FilterQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(logs))
	}
}

func TestProvider_RetriesTransientFailures(t *testing.T) {
	client := newFakeEthClient()
	client.setBlockNumberErr(errors.New("timeout"))
	p := newTestProvider(t, client, ProviderConfig{
		Retry: RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond},
	})

	_, err := p.BlockNumber(context.Background())
	if err == nil {
		t.Fatal("expected an error since the fake client always fails")
	}
	if calls := atomic.LoadInt32(&client.blockNumberCalls); calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestProvider_CloseClosesUnderlyingClient(t *testing.T) {
	client := newFakeEthClient()
	p := newTestProvider(t, client, ProviderConfig{})
	p.Close()
	if closed := atomic.LoadInt32(&client.closed); closed != 1 {
		t.Fatalf("expected underlying client to be closed exactly once, got %d", closed)
	}
}

func TestProvider_ConcurrentAccessNoRace(t *testing.T) {
	client := newFakeEthClient()
	client.setBlockNumber(1)
	p := newTestProvider(t, client, ProviderConfig{
		RateLimit: RateLimitConfig{Enabled: true, RPS: 1000, Burst: 1000},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.BlockNumber(context.Background())
			p.RecordSuccess()
			p.RecordFailure(errors.New("x"))
			_ = p.IsAvailable()
			_ = p.Score()
		}()
	}
	wg.Wait()
}
