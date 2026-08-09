package idx_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestMemoryQuotaStorage_QuotaUsageUpsert(t *testing.T) {
	m := NewMemoryQuotaStorage()
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

	firstID := u.ID
	// Updating the same (provider, quotaType) must reuse its ID.
	if err := m.SetQuotaUsage(ctx, "p1", "requests", 20, resetAt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	u, err = m.GetQuotaUsage(ctx, "p1", "requests")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.ID != firstID {
		t.Fatalf("expected ID to stay stable across updates, got %d then %d", firstID, u.ID)
	}
	if u.Used != 20 {
		t.Fatalf("expected updated Used=20, got %v", u.Used)
	}
}

func TestMemoryQuotaStorage_ConcurrentAccessNoRace(t *testing.T) {
	m := NewMemoryQuotaStorage()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.SetQuotaUsage(ctx, "p1", "requests", i, time.Now())
			_, _ = m.GetQuotaUsage(ctx, "p1", "requests")
		}()
	}
	wg.Wait()
}

var _ QuotaStorage = (*MemoryQuotaStorage)(nil)
