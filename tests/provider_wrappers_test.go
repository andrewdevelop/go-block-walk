package idx_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// TestProvider_RPCWrapperMethods exercises every thin pass-through wrapper
// around EthClient once, so a signature/argument-plumbing mistake in any of
// them would fail a test.
func TestProvider_RPCWrapperMethods(t *testing.T) {
	client := newFakeEthClient()
	p := newTestProvider(t, client, ProviderConfig{})
	ctx := context.Background()

	if _, err := p.BlockByNumber(ctx, 1); err != errNotImplemented {
		t.Errorf("BlockByNumber: unexpected error %v", err)
	}
	if _, err := p.BlockByHash(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("BlockByHash: unexpected error %v", err)
	}
	if _, _, err := p.TransactionByHash(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("TransactionByHash: unexpected error %v", err)
	}
	if _, err := p.TransactionReceipt(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("TransactionReceipt: unexpected error %v", err)
	}
	if _, err := p.HeaderByNumber(ctx, big.NewInt(1)); err != nil {
		t.Errorf("HeaderByNumber: unexpected error %v", err)
	}
	if _, err := p.BalanceAt(ctx, common.Address{}); err != nil {
		t.Errorf("BalanceAt: unexpected error %v", err)
	}
	if _, err := p.Call(ctx, ethereum.CallMsg{}); err != errNotImplemented {
		t.Errorf("Call: unexpected error %v", err)
	}
	if _, err := p.CodeAt(ctx, common.Address{}); err != nil {
		t.Errorf("CodeAt: unexpected error %v", err)
	}
}

func TestProvider_SubscribeNewHead(t *testing.T) {
	client := newFakeEthClient()
	p := newTestProvider(t, client, ProviderConfig{})

	if _, err := p.SubscribeNewHead(context.Background(), make(chan *types.Header)); err == nil {
		t.Fatal("expected an error from the fake client's SubscribeNewHead")
	}
}

func TestProvider_SubscribeNewHeadNoClient(t *testing.T) {
	p := NewProvider(context.Background(), ProviderConfig{
		Name: "broken",
		Dial: testDial(nil, errors.New("dial failed")),
	})

	if _, err := p.SubscribeNewHead(context.Background(), make(chan *types.Header)); err == nil {
		t.Fatal("expected an error when the provider has no client")
	}
}

func TestProvider_AccessorMethods(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{Priority: 3})

	p.SetScore(9.5)
	if p.Score() != 9.5 {
		t.Fatalf("expected SetScore to be reflected by Score(), got %v", p.Score())
	}
	if p.BaseScore() != 3 {
		t.Fatalf("expected BaseScore() == 3, got %v", p.BaseScore())
	}
	if !p.LastUsed().IsZero() {
		t.Fatal("expected LastUsed() to be zero before any RecordSuccess")
	}

	before := time.Now()
	p.RecordSuccess()
	if p.LastUsed().Before(before) {
		t.Fatal("expected LastUsed() to be updated by RecordSuccess")
	}
}

func TestProvider_GetQuotaRemaining(t *testing.T) {
	p := newTestProvider(t, newFakeEthClient(), ProviderConfig{
		Quota: QuotaConfig{Limit: 5, Period: time.Hour},
	})

	limit, used, _, _ := p.GetQuotaRemaining()
	if limit != 5 || used != 0 {
		t.Fatalf("expected remaining=5, used=0 before any calls, got remaining=%d used=%d", limit, used)
	}

	_, _ = p.BlockNumber(context.Background())

	limit, used, _, _ = p.GetQuotaRemaining()
	if limit != 4 || used != 1 {
		t.Fatalf("expected remaining=4, used=1 after one call, got remaining=%d used=%d", limit, used)
	}
}
