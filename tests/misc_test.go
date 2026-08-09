package idx_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// TestIndexer_SkipsUnsupportedTxTypeBlocksButStopsOnOtherErrors is a
// black-box proof of idx's unexported isUnsupportedTxType classifier's
// effect: a header fetch failing with go-ethereum's "transaction type not
// supported" message is treated as skippable (the indexer logs a warning
// and keeps advancing), while any other header error halts progress at the
// failing block instead.
func TestIndexer_SkipsUnsupportedTxTypeBlocksButStopsOnOtherErrors(t *testing.T) {
	t.Run("unsupported tx type is skipped, sync reaches the head", func(t *testing.T) {
		pool := newFakePool()
		pool.setBlockNumber(3)
		pool.setHeaderErr(errors.New("transaction type not supported"))

		idx := newTestIndexer(t, IndexerConfig{StartBlock: 1}, pool, NewMemory(), NewEventDispatcher(nil))
		if err := idx.Start(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer idx.Stop()
		idx.Nudge()

		waitFor(t, time.Second, func() bool {
			last, _ := idx.GetLastBlock()
			return last == 3
		})
	})

	t.Run("other header errors halt progress at the failing block", func(t *testing.T) {
		pool := newFakePool()
		pool.setBlockNumber(3)
		pool.setHeaderErr(errors.New("connection refused"))

		idx := newTestIndexer(t, IndexerConfig{StartBlock: 1, BlockInterval: 5 * time.Millisecond}, pool, NewMemory(), NewEventDispatcher(nil))
		if err := idx.Start(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer idx.Stop()

		// Give it several tick cycles' worth of time to (incorrectly) reach
		// the head if the error were mis-classified as skippable.
		time.Sleep(100 * time.Millisecond)

		last, err := idx.GetLastBlock()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if last != 0 {
			t.Fatalf("expected a non-skippable header error to prevent any progress, got last block %d", last)
		}
	})
}

// TestIndexer_LogsFetchErrorDoesNotAdvanceProgress is a black-box proof for
// the fix to a previously silent bug: a transient eth_getLogs error used to
// be logged and swallowed, letting the sync loop mark the block processed
// (storeLastBlock) anyway — permanently losing whatever events that block
// contained. The fix must instead halt progress at the failing block so the
// next tick retries it.
func TestIndexer_LogsFetchErrorDoesNotAdvanceProgress(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(3)
	pool.logsErr = errors.New("upstream unavailable")

	idx := newTestIndexer(t, IndexerConfig{StartBlock: 1, BlockInterval: 5 * time.Millisecond}, pool, NewMemory(), NewEventDispatcher(nil))
	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	time.Sleep(100 * time.Millisecond)

	last, err := idx.GetLastBlock()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if last != 0 {
		t.Fatalf("expected a logs-fetch error to prevent any progress (events would otherwise be lost silently), got last block %d", last)
	}
}

// TestIndexer_DispatchErrorDoesNotAdvanceProgress proves the fix for the
// other half of the same bug class: a listener/dispatcher error used to be
// logged and ignored while lastBlock still advanced, silently dropping
// events a listener failed to persist. The fix must halt progress instead
// (at-least-once delivery, relying on listener-side idempotency to make a
// retry safe).
func TestIndexer_DispatchErrorDoesNotAdvanceProgress(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(1)
	pool.addLog(1, types.Log{Address: common.HexToAddress("0x1")})

	listener := &recordingListener{err: errors.New("storage down")}
	idx := newTestIndexer(t, IndexerConfig{StartBlock: 1, BlockInterval: 5 * time.Millisecond}, pool, NewMemory(), NewEventDispatcher([]BlockchainListener{listener}))
	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	time.Sleep(100 * time.Millisecond)

	last, err := idx.GetLastBlock()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if last != 0 {
		t.Fatalf("expected a dispatch error to prevent any progress (events would otherwise be lost silently), got last block %d", last)
	}
}

// TestNewIndexer_RestoresPersistedProviderScoreAndQuota proves that a fresh
// Indexer loads previously persisted provider health score and quota usage
// back onto the pool's providers at construction — previously this state
// was write-only (persisted every sync, never read back), so a restart made
// a broken provider look healthy again and forgot how much quota it had
// already spent this period.
func TestNewIndexer_RestoresPersistedProviderScoreAndQuota(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	if err := store.SetProviderScore(ctx, "p1", -0.7, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.SetQuotaUsage(ctx, "p1", "requests", 5, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pool, err := NewPool(PoolConfig{Providers: []ProviderConfig{{
		Name:     "p1",
		Priority: 1,
		Dial:     testDial(newFakeEthClient(), nil),
		Retry:    RetryConfig{MaxAttempts: 1},
		Quota:    QuotaConfig{Limit: 5, Period: time.Hour},
	}}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pool.Close()

	newTestIndexer(t, IndexerConfig{}, pool, store, NewEventDispatcher(nil))

	providers := pool.GetAllProviders()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	p := providers[0]

	if got := p.Score(); got != -0.7 {
		t.Fatalf("expected the persisted score -0.7 to be restored, got %v", got)
	}
	if p.IsAvailable() {
		t.Fatal("expected the provider to already be unavailable: restored quota usage (5) matches the configured limit (5)")
	}
}

// TestIndexer_DetectsReorgAndRewinds proves the fix for a previously
// unguarded gap: the indexer only detected a full chain reset (currentBlock
// < lastBlock), so a same-height reorg — the chain tip stays at the same
// block number but its content changed — went completely unnoticed,
// silently keeping stale events and never re-fetching the now-canonical
// logs. With ReorgDepth configured, a mismatched hash at the last
// processed block must trigger a rewind and reprocessing.
func TestIndexer_DetectsReorgAndRewinds(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(3)
	pool.addLog(1, types.Log{Address: common.HexToAddress("0x1")})
	pool.addLog(2, types.Log{Address: common.HexToAddress("0x1")})
	pool.addLog(3, types.Log{Address: common.HexToAddress("0x1")})

	listener := &countingListener{}
	idx := newTestIndexer(t, IndexerConfig{
		StartBlock: 1,
		ReorgDepth: 3,
	}, pool, NewMemory(), NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 3
	})
	if got := listener.count(); got != 3 {
		t.Fatalf("expected 3 dispatched logs before the reorg, got %d", got)
	}

	// Simulate a same-height reorg at block 3: a different header (so a
	// different hash) plus a log that wasn't present on the old fork.
	pool.setHeader(3, &types.Header{Number: big.NewInt(3), Time: 999999})
	pool.addLog(3, types.Log{Address: common.HexToAddress("0x2")})
	idx.Nudge()

	waitFor(t, time.Second, func() bool {
		return listener.count() >= 4
	})
}

// TestIndexer_ReorgDetectionDisabledByDefault proves ReorgDepth's zero
// value preserves prior behaviour exactly: a same-height content change
// must not be detected or rewound when the feature isn't opted into.
func TestIndexer_ReorgDetectionDisabledByDefault(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(3)
	pool.addLog(3, types.Log{Address: common.HexToAddress("0x1")})

	listener := &countingListener{}
	idx := newTestIndexer(t, IndexerConfig{StartBlock: 1}, pool, NewMemory(), NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()

	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 3
	})

	pool.setHeader(3, &types.Header{Number: big.NewInt(3), Time: 999999})
	pool.addLog(3, types.Log{Address: common.HexToAddress("0x2")})
	idx.Nudge()

	time.Sleep(50 * time.Millisecond)
	if got := listener.count(); got != 1 {
		t.Fatalf("expected no reprocessing with ReorgDepth unset, got %d dispatched logs", got)
	}
}

func TestIndexer_GetProviderScoresAndEvents(t *testing.T) {
	pool := newFakePool()
	store := NewMemory()
	_ = store.SaveIndexedEvent(context.Background(), &IndexedEvent{Chain: "eth", BlockNumber: 1, LogIndex: 0})

	idx := newTestIndexer(t, IndexerConfig{}, pool, store, NewEventDispatcher(nil))

	scores := idx.GetProviderScores()
	if scores["fake"] != 1 {
		t.Fatalf("expected the fake pool's score for 'fake', got %v", scores)
	}

	events, err := idx.GetEvents(0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}
