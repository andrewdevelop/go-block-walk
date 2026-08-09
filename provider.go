package idx

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Provider is a single upstream RPC endpoint wrapped with rate limiting,
// circuit breaking, retries, quota tracking and a health score. Safe for
// concurrent use.
type Provider struct {
	name           string
	priority       int
	client         EthClient
	dialErr        error
	limiter        *RateLimiter
	circuitBreaker *CircuitBreaker
	quota          *QuotaManager
	retryCfg       RetryConfig

	mu           sync.RWMutex
	healthy      bool
	lastUsed     time.Time
	baseScore    float64
	currentScore float64
}

// NewProvider dials cfg.URL (via cfg.Dial, or go-ethereum's ethclient by
// default) and wraps it. A dial failure does not fail construction — the
// provider is simply marked unavailable, mirroring how a circuit-broken or
// quota-exhausted provider behaves; the pool just skips it.
func NewProvider(ctx context.Context, cfg ProviderConfig) *Provider {
	dial := cfg.Dial
	if dial == nil {
		dial = dialEthClient
	}

	url := cfg.URL
	if cfg.APIKey != "" {
		url = fmt.Sprintf("%s/%s", cfg.URL, cfg.APIKey)
	}

	client, err := dial(ctx, url)

	return &Provider{
		name:           cfg.Name,
		priority:       cfg.Priority,
		client:         client,
		dialErr:        err,
		limiter:        NewRateLimiter(cfg.RateLimit),
		circuitBreaker: NewCircuitBreaker(cfg.CircuitBreaker),
		quota:          newQuotaManager(cfg.Quota),
		retryCfg:       cfg.Retry,
		healthy:        err == nil && client != nil,
		baseScore:      float64(cfg.Priority),
		currentScore:   float64(cfg.Priority),
	}
}

func (p *Provider) Name() string           { return p.name }
func (p *Provider) Priority() int          { return p.priority }
func (p *Provider) SetScore(score float64) { p.mu.Lock(); defer p.mu.Unlock(); p.currentScore = score }

func (p *Provider) Score() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentScore
}

func (p *Provider) BaseScore() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.baseScore
}

func (p *Provider) LastUsed() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastUsed
}

// IsAvailable reports whether the provider can currently serve a request. It
// never has side effects — checking availability does not itself consume
// rate-limit tokens or quota.
func (p *Provider) IsAvailable() bool {
	if p.dialErr != nil || p.client == nil {
		return false
	}

	p.mu.RLock()
	healthy := p.healthy
	p.mu.RUnlock()
	if !healthy {
		return false
	}

	if !p.circuitBreaker.IsAvailable() {
		return false
	}

	return p.quota.Remaining()
}

func (p *Provider) RecordSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.healthy = true
	p.currentScore += 0.1
	if max := p.baseScore * 2; p.currentScore > max {
		p.currentScore = max
	}
	p.lastUsed = time.Now()
}

// RecordFailure lowers the provider's score; once it drops below -1.0 the
// provider is marked unhealthy until a subsequent success recovers it.
func (p *Provider) RecordFailure(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.currentScore -= 0.2
	if p.currentScore < -1.0 {
		p.healthy = false
	}
}

func (p *Provider) BlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error) {
	var result *types.Block
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.BlockByNumber(ctx, new(big.Int).SetUint64(blockNum))
		return err
	})
	return result, err
}

func (p *Provider) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	var result *types.Block
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.BlockByHash(ctx, hash)
		return err
	})
	return result, err
}

func (p *Provider) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	var result *types.Transaction
	var isPending bool
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, isPending, err = p.client.TransactionByHash(ctx, hash)
		return err
	})
	return result, isPending, err
}

func (p *Provider) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	var result *types.Receipt
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.TransactionReceipt(ctx, hash)
		return err
	})
	return result, err
}

func (p *Provider) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	var result []types.Log
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.FilterLogs(ctx, q)
		return err
	})
	return result, err
}

func (p *Provider) SubscribeNewHead(ctx context.Context, ch chan *types.Header) (ethereum.Subscription, error) {
	if p.client == nil {
		return nil, fmt.Errorf("provider %s: not connected", p.name)
	}
	return p.client.SubscribeNewHead(ctx, ch)
}

func (p *Provider) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	var result *types.Header
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.HeaderByNumber(ctx, number)
		return err
	})
	return result, err
}

func (p *Provider) BlockNumber(ctx context.Context) (uint64, error) {
	var result uint64
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.BlockNumber(ctx)
		return err
	})
	return result, err
}

func (p *Provider) BalanceAt(ctx context.Context, address common.Address) (*big.Int, error) {
	var result *big.Int
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.BalanceAt(ctx, address, nil)
		return err
	})
	return result, err
}

func (p *Provider) Call(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	var result []byte
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.CallContract(ctx, msg, nil)
		return err
	})
	return result, err
}

func (p *Provider) CodeAt(ctx context.Context, address common.Address) ([]byte, error) {
	var result []byte
	err := p.withRetry(ctx, func(ctx context.Context) error {
		var err error
		result, err = p.client.CodeAt(ctx, address, nil)
		return err
	})
	return result, err
}

// withRetry consumes one unit of quota up front (a request denied by quota
// is not sent at all, so it isn't retried or rate-limited) and then runs fn
// through the rate limiter, circuit breaker and retry/backoff loop.
func (p *Provider) withRetry(ctx context.Context, fn func(context.Context) error) error {
	if !p.quota.Consume() {
		return fmt.Errorf("provider %s: %w", p.name, ErrQuotaExceeded)
	}
	return WithRetry(ctx, p.retryCfg, fn, p.limiter, p.circuitBreaker)
}

func (p *Provider) Close() {
	if p.client != nil {
		p.client.Close()
	}
}

func (p *Provider) GetQuotaUsage() (limit, used, creditsLimit, creditsUsed int64) {
	return p.quota.Usage()
}

func (p *Provider) GetQuotaRemaining() (limit, used, creditsLimit, creditsUsed int64) {
	return p.quota.RemainingUnits()
}

func (p *Provider) GetQuotaReset() (requestReset, creditReset time.Time) {
	return p.quota.ResetTimes()
}

// QuotaManager tracks a rolling request/credit budget for one provider.
// Safe for concurrent use.
type QuotaManager struct {
	mu          sync.Mutex
	limit       int64
	period      time.Duration
	used        int64
	resetTime   time.Time
	credits     *CreditsConfig
	creditUsed  int64
	creditLimit int64
	creditReset time.Time
}

func newQuotaManager(cfg QuotaConfig) *QuotaManager {
	now := time.Now()
	qm := &QuotaManager{
		limit:     cfg.Limit,
		period:    cfg.Period,
		resetTime: now.Add(cfg.Period),
	}

	if cfg.Credits != nil && cfg.Credits.Enabled {
		qm.credits = cfg.Credits
		qm.creditLimit = cfg.Credits.Limit
		qm.creditReset = now.Add(cfg.Credits.Period)
	}

	return qm
}

func (qm *QuotaManager) rolloverLocked(now time.Time) {
	if qm.period > 0 && now.After(qm.resetTime) {
		qm.used = 0
		qm.resetTime = now.Add(qm.period)
	}
	if qm.credits != nil && now.After(qm.creditReset) {
		qm.creditUsed = 0
		qm.creditReset = now.Add(qm.credits.Period)
	}
}

// Remaining reports whether quota is currently available, without
// consuming it.
func (qm *QuotaManager) Remaining() bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.rolloverLocked(time.Now())

	if qm.limit > 0 && qm.used >= qm.limit {
		return false
	}
	if qm.credits != nil && qm.creditUsed+qm.credits.PerRequest > qm.creditLimit {
		return false
	}
	return true
}

// Consume atomically checks and reserves quota for one request. Returns
// false (without reserving anything) if the request would exceed the limit.
func (qm *QuotaManager) Consume() bool {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.rolloverLocked(time.Now())

	if qm.limit > 0 && qm.used >= qm.limit {
		return false
	}
	if qm.credits != nil {
		if qm.creditUsed+qm.credits.PerRequest > qm.creditLimit {
			return false
		}
		qm.creditUsed += qm.credits.PerRequest
	}

	qm.used++
	return true
}

func (qm *QuotaManager) Usage() (limit, used, creditsLimit, creditsUsed int64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.limit, qm.used, qm.creditLimit, qm.creditUsed
}

func (qm *QuotaManager) RemainingUnits() (limit, used, creditsLimit, creditsUsed int64) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.limit - qm.used, qm.used, qm.creditLimit - qm.creditUsed, qm.creditUsed
}

func (qm *QuotaManager) ResetTimes() (requestReset, creditReset time.Time) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.resetTime, qm.creditReset
}

var _ RPCProvider = (*Provider)(nil)
