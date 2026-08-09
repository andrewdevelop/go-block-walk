package idx_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestMemory_LastBlockRoundTrip(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	got, err := m.GetLastBlock(ctx, "eth")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 for an unset chain, got %d", got)
	}

	if err := m.SetLastBlock(ctx, "eth", 123); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err = m.GetLastBlock(ctx, "eth")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 123 {
		t.Fatalf("expected 123, got %d", got)
	}

	// A different chain must not be affected.
	got, err = m.GetLastBlock(ctx, "polygon")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 for a different, unset chain, got %d", got)
	}
}

func TestMemory_ProviderScoreUpsert(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	if got, err := m.GetProviderScore(ctx, "p1"); err != nil || got != nil {
		t.Fatalf("expected nil, nil for an unknown provider, got %v, %v", got, err)
	}

	if err := m.SetProviderScore(ctx, "p1", 1.5, 0.5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, err := m.GetProviderScore(ctx, "p1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil || s.Score != 1.5 || s.Penalty != 0.5 {
		t.Fatalf("unexpected score record: %+v", s)
	}
	firstID := s.ID

	// Updating the same provider must reuse its ID, not allocate a new one.
	if err := m.SetProviderScore(ctx, "p1", 2.5, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, err = m.GetProviderScore(ctx, "p1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.ID != firstID {
		t.Fatalf("expected ID to stay stable across updates, got %d then %d", firstID, s.ID)
	}
	if s.Score != 2.5 {
		t.Fatalf("expected updated score 2.5, got %v", s.Score)
	}
}

func TestMemory_GetAllProviderScoresSortedByProvider(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	_ = m.SetProviderScore(ctx, "zeta", 1, 0)
	_ = m.SetProviderScore(ctx, "alpha", 2, 0)

	all, err := m.GetAllProviderScores(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(all) != 2 || all[0].Provider != "alpha" || all[1].Provider != "zeta" {
		t.Fatalf("expected sorted [alpha, zeta], got %+v", all)
	}
}

func TestMemory_QuotaUsageUpsert(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	if got, err := m.GetQuotaUsage(ctx, "p1", "requests"); err != nil || got != nil {
		t.Fatalf("expected nil, nil for unknown quota, got %v, %v", got, err)
	}

	resetAt := time.Now().Add(time.Hour)
	if err := m.SetQuotaUsage(ctx, "p1", "requests", 10, resetAt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	u, err := m.GetQuotaUsage(ctx, "p1", "requests")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u == nil || u.Used != 10 || !u.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected quota usage: %+v", u)
	}

	// A distinct quota type for the same provider must be independent.
	if got, err := m.GetQuotaUsage(ctx, "p1", "credits"); err != nil || got != nil {
		t.Fatalf("expected the 'credits' quota type to remain unset, got %v, %v", got, err)
	}
}

func TestMemory_SaveIndexedEventDeduplicates(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	ev := &IndexedEvent{Chain: "eth", BlockNumber: 1, LogIndex: 0, TxHash: "0xabc"}
	if err := m.SaveIndexedEvent(ctx, ev); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dup := &IndexedEvent{Chain: "eth", BlockNumber: 1, LogIndex: 0, TxHash: "0xDIFFERENT"}
	if err := m.SaveIndexedEvent(ctx, dup); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events, err := m.GetIndexedEvents(ctx, "eth", 1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected the duplicate to be ignored, got %d events", len(events))
	}
	if events[0].TxHash != "0xabc" {
		t.Fatalf("expected the original event to win, got TxHash=%q", events[0].TxHash)
	}
}

func TestMemory_SaveIndexedEventNilGuard(t *testing.T) {
	m := NewMemory()
	if err := m.SaveIndexedEvent(context.Background(), nil); err == nil {
		t.Fatal("expected an error when saving a nil event")
	}
}

func TestMemory_GetIndexedEventsFiltersAndSorts(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	events := []*IndexedEvent{
		{Chain: "eth", BlockNumber: 5, LogIndex: 1},
		{Chain: "eth", BlockNumber: 3, LogIndex: 0},
		{Chain: "eth", BlockNumber: 3, LogIndex: 2},
		{Chain: "eth", BlockNumber: 10, LogIndex: 0},    // out of range
		{Chain: "polygon", BlockNumber: 3, LogIndex: 0}, // wrong chain
	}
	for _, e := range events {
		if err := m.SaveIndexedEvent(ctx, e); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	got, err := m.GetIndexedEvents(ctx, "eth", 3, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 events in range, got %d: %+v", len(got), got)
	}
	if got[0].BlockNumber != 3 || got[0].LogIndex != 0 {
		t.Fatalf("expected first event to be block 3/log 0, got %+v", got[0])
	}
	if got[1].BlockNumber != 3 || got[1].LogIndex != 2 {
		t.Fatalf("expected second event to be block 3/log 2, got %+v", got[1])
	}
	if got[2].BlockNumber != 5 {
		t.Fatalf("expected third event to be block 5, got %+v", got[2])
	}
}

func TestMemory_IsEventIndexed(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	indexed, err := m.IsEventIndexed(ctx, "eth", 1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if indexed {
		t.Fatal("expected false before the event is saved")
	}

	_ = m.SaveIndexedEvent(ctx, &IndexedEvent{Chain: "eth", BlockNumber: 1, LogIndex: 0})

	indexed, err = m.IsEventIndexed(ctx, "eth", 1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !indexed {
		t.Fatal("expected true after the event is saved")
	}
}

func TestMemory_Close(t *testing.T) {
	m := NewMemory()
	if err := m.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMemory_ConcurrentAccessNoRace(t *testing.T) {
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

var _ Storage = (*Memory)(nil)
