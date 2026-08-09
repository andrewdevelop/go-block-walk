package idx_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
