package idx_test

import (
	"context"
	"sync"
	"testing"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestMemoryScoreStorage_ProviderScoreUpsert(t *testing.T) {
	m := NewMemoryScoreStorage()
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

func TestMemoryScoreStorage_GetAllProviderScoresSortedByProvider(t *testing.T) {
	m := NewMemoryScoreStorage()
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

func TestMemoryScoreStorage_ConcurrentAccessNoRace(t *testing.T) {
	m := NewMemoryScoreStorage()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.SetProviderScore(ctx, "p1", float64(i), 0)
			_, _ = m.GetProviderScore(ctx, "p1")
			_, _ = m.GetAllProviderScores(ctx)
		}()
	}
	wg.Wait()
}

var _ ScoreStorage = (*MemoryScoreStorage)(nil)
