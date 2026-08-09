package idx

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryChainStorage is an in-memory, thread-safe ChainStorage
// implementation — sync progress and indexed events, nothing else. It is a
// complete, standalone Indexer backend on its own (no provider health/quota
// bookkeeping); Memory embeds it to additionally provide ScoreStorage and
// QuotaStorage.
type MemoryChainStorage struct {
	mu sync.RWMutex

	lastBlocks map[string]uint64
	events     map[eventKey]IndexedEvent

	nextEventID int
}

type eventKey struct {
	chain       string
	blockNumber uint64
	logIndex    uint
}

func NewMemoryChainStorage() *MemoryChainStorage {
	return &MemoryChainStorage{
		lastBlocks: make(map[string]uint64),
		events:     make(map[eventKey]IndexedEvent),
	}
}

func (m *MemoryChainStorage) GetLastBlock(ctx context.Context, chain string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastBlocks[chain], nil
}

func (m *MemoryChainStorage) SetLastBlock(ctx context.Context, chain string, blockNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastBlocks[chain] = blockNum
	return nil
}

// SaveIndexedEvent inserts event, keyed by (chain, block number, log
// index). A duplicate is silently ignored — mirroring an `ON CONFLICT DO
// NOTHING` upsert — so re-processing a block already indexed stays
// idempotent.
func (m *MemoryChainStorage) SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error {
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

func (m *MemoryChainStorage) GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error) {
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

func (m *MemoryChainStorage) IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.events[eventKey{chain, blockNumber, logIndex}]
	return ok, nil
}

func (m *MemoryChainStorage) Close() error { return nil }

var _ ChainStorage = (*MemoryChainStorage)(nil)
