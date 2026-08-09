package idx_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// TestPool_RPCWrapperMethods exercises every thin pass-through wrapper
// around the selected Provider once.
func TestPool_RPCWrapperMethods(t *testing.T) {
	client := newFakeEthClient()
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("a", 1, client)}})
	defer pool.Close()
	ctx := context.Background()

	if _, err := pool.BlockByNumber(ctx, 1); err != errNotImplemented {
		t.Errorf("BlockByNumber: unexpected error %v", err)
	}
	if _, err := pool.BlockByHash(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("BlockByHash: unexpected error %v", err)
	}
	if _, _, err := pool.TransactionByHash(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("TransactionByHash: unexpected error %v", err)
	}
	if _, err := pool.TransactionReceipt(ctx, common.Hash{}); err != errNotImplemented {
		t.Errorf("TransactionReceipt: unexpected error %v", err)
	}
	if _, err := pool.SubscribeNewHead(ctx, make(chan *types.Header)); err == nil {
		t.Error("SubscribeNewHead: expected an error from the fake client")
	}
	if _, err := pool.HeaderByNumber(ctx, big.NewInt(1)); err != nil {
		t.Errorf("HeaderByNumber: unexpected error %v", err)
	}
	if _, err := pool.BalanceAt(ctx, common.Address{}); err != nil {
		t.Errorf("BalanceAt: unexpected error %v", err)
	}
	if _, err := pool.CodeAt(ctx, common.Address{}); err != nil {
		t.Errorf("CodeAt: unexpected error %v", err)
	}
}

func TestPool_NoAvailableProviderErrorsOnEveryWrapper(t *testing.T) {
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{
		{Name: "bad", Priority: 1, Dial: testDial(nil, errors.New("down")), Retry: RetryConfig{MaxAttempts: 1}},
	}})
	defer pool.Close()
	ctx := context.Background()

	if _, err := pool.BlockByHash(ctx, common.Hash{}); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("BlockByHash: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, _, err := pool.TransactionByHash(ctx, common.Hash{}); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("TransactionByHash: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, err := pool.TransactionReceipt(ctx, common.Hash{}); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("TransactionReceipt: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, err := pool.SubscribeNewHead(ctx, make(chan *types.Header)); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("SubscribeNewHead: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, err := pool.HeaderByNumber(ctx, big.NewInt(1)); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("HeaderByNumber: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, err := pool.BalanceAt(ctx, common.Address{}); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("BalanceAt: expected ErrNoAvailableProvider, got %v", err)
	}
	if _, err := pool.CodeAt(ctx, common.Address{}); !errors.Is(err, ErrNoAvailableProvider) {
		t.Errorf("CodeAt: expected ErrNoAvailableProvider, got %v", err)
	}
}

// TestPool_BlockByNumberDoesNotPenalizeLocalDecodeError verifies that a
// BlockByNumber failure caused by go-ethereum being unable to decode a
// transaction type locally does not lower the provider's score, while an
// ordinary provider-side failure does. This is the behavioural effect of
// idx's unexported isLocalDecodeError classifier.
func TestPool_BlockByNumberDoesNotPenalizeLocalDecodeError(t *testing.T) {
	client := newFakeEthClient()
	client.setBlockByNumberErr(errors.New("transaction type not supported"))
	pool := NewPool(PoolConfig{Providers: []ProviderConfig{poolProviderConfig("a", 1, client)}})
	defer pool.Close()

	before := pool.GetScores()["a"]
	if _, err := pool.BlockByNumber(context.Background(), 1); err == nil {
		t.Fatal("expected an error to be returned to the caller")
	}
	if after := pool.GetScores()["a"]; after != before {
		t.Fatalf("expected a local decode error to leave the score unchanged: before=%v after=%v", before, after)
	}

	client.setBlockByNumberErr(errors.New("timeout"))
	if _, err := pool.BlockByNumber(context.Background(), 1); err == nil {
		t.Fatal("expected an error to be returned to the caller")
	}
	if after := pool.GetScores()["a"]; after >= before {
		t.Fatalf("expected a genuine provider failure to lower the score: before=%v after=%v", before, after)
	}
}
