package idx_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/andrewdevelop/go-block-walk"
)

// Memory itself is a thin composition of MemoryChainStorage,
// MemoryScoreStorage and MemoryQuotaStorage — see memorychain_test.go,
// memoryscore_test.go and memoryquota_test.go for the behavioral tests of
// each piece in isolation. These tests only cover the composition itself:
// that embedding correctly wires up the full Storage surface and that
// using all three together concurrently through one Memory value is safe.

func TestMemory_ComposesAllThreePieces(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	if err := m.SetLastBlock(ctx, "eth", 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if last, err := m.GetLastBlock(ctx, "eth"); err != nil || last != 42 {
		t.Fatalf("expected last block 42, got %d (err=%v)", last, err)
	}

	if err := m.SetProviderScore(ctx, "p1", 1, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if score, err := m.GetProviderScore(ctx, "p1"); err != nil || score == nil {
		t.Fatalf("expected a provider score, got %v (err=%v)", score, err)
	}

	if err := m.SetQuotaUsage(ctx, "p1", "requests", 5, time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage, err := m.GetQuotaUsage(ctx, "p1", "requests"); err != nil || usage == nil {
		t.Fatalf("expected quota usage, got %v (err=%v)", usage, err)
	}

	if err := m.SaveIndexedEvent(ctx, &IndexedEvent{Chain: "eth", BlockNumber: 42, LogIndex: 0}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events, err := m.GetIndexedEvents(ctx, "eth", 0, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 indexed event, got %d", len(events))
	}

	if err := m.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMemory_ConcurrentAccessAcrossAllThreePieces(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			blockNum := uint64(i % 5)
			_ = m.SetLastBlock(ctx, "eth", blockNum)
			_, _ = m.GetLastBlock(ctx, "eth")
			_ = m.SetProviderScore(ctx, "p1", float64(i), 0)
			_, _ = m.GetProviderScore(ctx, "p1")
			_ = m.SetQuotaUsage(ctx, "p1", "requests", i, time.Now())
			_, _ = m.GetQuotaUsage(ctx, "p1", "requests")
			_ = m.SaveIndexedEvent(ctx, &IndexedEvent{Chain: "eth", BlockNumber: blockNum, LogIndex: uint(i)})
			_, _ = m.GetIndexedEvents(ctx, "eth", 0, 10)
			_, _ = m.IsEventIndexed(ctx, "eth", blockNum, uint(i))
		}()
	}
	wg.Wait()
}

var (
	_ Storage      = (*Memory)(nil)
	_ ChainStorage = (*Memory)(nil)
	_ ScoreStorage = (*Memory)(nil)
	_ QuotaStorage = (*Memory)(nil)
)
