package idx

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Memory is an in-memory, thread-safe implementation of Storage. State is
// lost on process restart, making it a convenient default for tests, local
// development and demos. Production deployments should back Storage with a
// persistent store implementing this package's Storage interface instead.
type Memory struct {
	mu sync.RWMutex

	lastBlocks map[string]uint64
	scores     map[string]ProviderScore
	quota      map[quotaKey]QuotaUsage
	events     map[eventKey]IndexedEvent

	nextScoreID int
	nextQuotaID int
	nextEventID int
}

type quotaKey struct {
	provider  string
	quotaType string
}

type eventKey struct {
	chain       string
	blockNumber uint64
	logIndex    uint
}

func NewMemory() *Memory {
	return &Memory{
		lastBlocks: make(map[string]uint64),
		scores:     make(map[string]ProviderScore),
		quota:      make(map[quotaKey]QuotaUsage),
		events:     make(map[eventKey]IndexedEvent),
	}
}

func (m *Memory) GetLastBlock(ctx context.Context, chain string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastBlocks[chain], nil
}

func (m *Memory) SetLastBlock(ctx context.Context, chain string, blockNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastBlocks[chain] = blockNum
	return nil
}

func (m *Memory) GetProviderScore(ctx context.Context, provider string) (*ProviderScore, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.scores[provider]
	if !ok {
		return nil, nil
	}
	cp := s
	return &cp, nil
}

func (m *Memory) SetProviderScore(ctx context.Context, provider string, score, penalty float64) error {
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

func (m *Memory) GetAllProviderScores(ctx context.Context) ([]ProviderScore, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ProviderScore, 0, len(m.scores))
	for _, s := range m.scores {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out, nil
}

func (m *Memory) GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	u, ok := m.quota[quotaKey{provider, quotaType}]
	if !ok {
		return nil, nil
	}
	cp := u
	return &cp, nil
}

func (m *Memory) SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := quotaKey{provider, quotaType}
	existing, ok := m.quota[key]
	id := existing.ID
	if !ok {
		m.nextQuotaID++
		id = m.nextQuotaID
	}
	m.quota[key] = QuotaUsage{
		ID:        id,
		Provider:  provider,
		QuotaType: quotaType,
		Used:      used,
		ResetAt:   resetAt,
		UpdatedAt: time.Now().UTC(),
	}
	return nil
}

// SaveIndexedEvent inserts event, keyed by (chain, block number, log
// index). A duplicate is silently ignored — mirroring an `ON CONFLICT DO
// NOTHING` upsert — so re-processing a block already indexed stays
// idempotent.
func (m *Memory) SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error {
	if event == nil {
		return fmt.Errorf("idx: event is nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := eventKey{event.Chain, event.BlockNumber, event.LogIndex}
	if _, exists := m.events[key]; exists {
		return nil
	}

	m.nextEventID++
	stored := *event
	stored.ID = m.nextEventID
	stored.IndexedAt = time.Now().UTC()
	m.events[key] = stored
	return nil
}

func (m *Memory) GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]IndexedEvent, 0)
	for _, e := range m.events {
		if e.Chain == chain && e.BlockNumber >= fromBlock && e.BlockNumber <= toBlock {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockNumber != out[j].BlockNumber {
			return out[i].BlockNumber < out[j].BlockNumber
		}
		return out[i].LogIndex < out[j].LogIndex
	})
	return out, nil
}

func (m *Memory) IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.events[eventKey{chain, blockNumber, logIndex}]
	return ok, nil
}

func (m *Memory) Close() error { return nil }

var _ Storage = (*Memory)(nil)
