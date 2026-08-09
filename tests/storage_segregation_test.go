package idx_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

// chainOnlyStorage implements ChainStorage and nothing else — no
// ScoreStorage, no QuotaStorage. It exists to prove that's a complete,
// working Indexer backend: score/quota bookkeeping is optional, not a
// tax every Storage implementer has to pay.
type chainOnlyStorage struct {
	mu         sync.Mutex
	lastBlocks map[string]uint64
	events     []IndexedEvent
}

func newChainOnlyStorage() *chainOnlyStorage {
	return &chainOnlyStorage{lastBlocks: make(map[string]uint64)}
}

func (s *chainOnlyStorage) GetLastBlock(ctx context.Context, chain string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastBlocks[chain], nil
}

func (s *chainOnlyStorage) SetLastBlock(ctx context.Context, chain string, blockNum uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBlocks[chain] = blockNum
	return nil
}

func (s *chainOnlyStorage) SaveIndexedEvent(ctx context.Context, event *IndexedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, *event)
	return nil
}

func (s *chainOnlyStorage) GetIndexedEvents(ctx context.Context, chain string, fromBlock, toBlock uint64) ([]IndexedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []IndexedEvent
	for _, e := range s.events {
		if e.Chain == chain && e.BlockNumber >= fromBlock && e.BlockNumber <= toBlock {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *chainOnlyStorage) IsEventIndexed(ctx context.Context, chain string, blockNumber uint64, logIndex uint) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.Chain == chain && e.BlockNumber == blockNumber && e.LogIndex == logIndex {
			return true, nil
		}
	}
	return false, nil
}

func (s *chainOnlyStorage) Close() error { return nil }

// Deliberately does NOT implement ScoreStorage or QuotaStorage — if it
// compiles and satisfies ChainStorage below, the interface segregation
// works as intended.
var _ ChainStorage = (*chainOnlyStorage)(nil)

func TestIndexer_MinimalChainOnlyStorageIsSufficient(t *testing.T) {
	pool := newFakePool()
	pool.setBlockNumber(3)
	pool.addLog(2, types.Log{Address: common.HexToAddress("0x1")})

	store := newChainOnlyStorage()
	listener := &countingListener{}
	idx := newTestIndexer(t, IndexerConfig{StartBlock: 1}, pool, store, NewEventDispatcher([]BlockchainListener{listener}))

	if err := idx.Start(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer idx.Stop()
	idx.Nudge()

	// The Indexer must sync normally — persisting scores/quota is silently
	// skipped rather than erroring — even though store implements neither
	// ScoreStorage nor QuotaStorage.
	waitFor(t, time.Second, func() bool {
		last, _ := idx.GetLastBlock()
		return last == 3
	})
	if got := listener.count(); got != 1 {
		t.Fatalf("expected 1 dispatched log, got %d", got)
	}

	// The Indexer itself never calls SaveIndexedEvent (that's a listener's
	// job) — GetIndexedEvents should simply report none yet.
	events, err := store.GetIndexedEvents(context.Background(), "eth", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no indexed events, got %d", len(events))
	}

	if idx.Storage() != store {
		t.Fatal("expected Storage() to return the chain-only store unchanged")
	}
}

// quotaOnlyStore implements QuotaStorage and nothing else — not
// ChainStorage, not ScoreStorage. It exists to prove Pool.PersistQuotaUsage
// only ever needs QuotaStorage, so a component that only cares about quota
// bookkeeping doesn't have to stub out chain/score methods it'll never use.
type quotaOnlyStore struct {
	mu   sync.Mutex
	used map[string]int
}

func newQuotaOnlyStore() *quotaOnlyStore {
	return &quotaOnlyStore{used: make(map[string]int)}
}

func (s *quotaOnlyStore) GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	used, ok := s.used[provider+"|"+quotaType]
	if !ok {
		return nil, nil
	}
	return &QuotaUsage{Provider: provider, QuotaType: quotaType, Used: used}, nil
}

func (s *quotaOnlyStore) SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.used[provider+"|"+quotaType] = used
	return nil
}

var _ QuotaStorage = (*quotaOnlyStore)(nil)

func TestPool_PersistQuotaUsageAcceptsQuotaOnlyStorage(t *testing.T) {
	client := newFakeEthClient()
	pool := mustNewPool(t, PoolConfig{Providers: []ProviderConfig{poolProviderConfig("p1", 1, client)}})
	defer pool.Close()

	qs := newQuotaOnlyStore()
	if err := pool.PersistQuotaUsage(context.Background(), qs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	usage, err := qs.GetQuotaUsage(context.Background(), "p1", "requests")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage == nil {
		t.Fatal("expected quota usage to have been recorded on the quota-only store")
	}
}
