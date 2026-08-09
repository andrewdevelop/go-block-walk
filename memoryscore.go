package idx

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryScoreStorage is an in-memory, thread-safe ScoreStorage
// implementation — provider health score bookkeeping, nothing else.
type MemoryScoreStorage struct {
	mu     sync.RWMutex
	scores map[string]ProviderScore

	nextScoreID int
}

func NewMemoryScoreStorage() *MemoryScoreStorage {
	return &MemoryScoreStorage{scores: make(map[string]ProviderScore)}
}

func (m *MemoryScoreStorage) GetProviderScore(ctx context.Context, provider string) (*ProviderScore, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.scores[provider]
	if !ok {
		return nil, nil
	}
	cp := s
	return &cp, nil
}

func (m *MemoryScoreStorage) SetProviderScore(ctx context.Context, provider string, score, penalty float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.scores[provider]
	id := existing.ID
	if !ok {
		m.nextScoreID++
		id = m.nextScoreID
	}
	m.scores[provider] = ProviderScore{
		ID:        id,
		Provider:  provider,
		Score:     score,
		Penalty:   penalty,
		UpdatedAt: time.Now().UTC(),
	}
	return nil
}

func (m *MemoryScoreStorage) GetAllProviderScores(ctx context.Context) ([]ProviderScore, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ProviderScore, 0, len(m.scores))
	for _, s := range m.scores {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out, nil
}

var _ ScoreStorage = (*MemoryScoreStorage)(nil)
