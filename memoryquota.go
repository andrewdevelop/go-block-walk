package idx

import (
	"context"
	"sync"
	"time"
)

// MemoryQuotaStorage is an in-memory, thread-safe QuotaStorage
// implementation — provider quota usage bookkeeping, nothing else.
type MemoryQuotaStorage struct {
	mu    sync.RWMutex
	quota map[quotaKey]QuotaUsage

	nextQuotaID int
}

type quotaKey struct {
	provider  string
	quotaType string
}

func NewMemoryQuotaStorage() *MemoryQuotaStorage {
	return &MemoryQuotaStorage{quota: make(map[quotaKey]QuotaUsage)}
}

func (m *MemoryQuotaStorage) GetQuotaUsage(ctx context.Context, provider, quotaType string) (*QuotaUsage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	u, ok := m.quota[quotaKey{provider, quotaType}]
	if !ok {
		return nil, nil
	}
	cp := u
	return &cp, nil
}

func (m *MemoryQuotaStorage) SetQuotaUsage(ctx context.Context, provider, quotaType string, used int, resetAt time.Time) error {
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

var _ QuotaStorage = (*MemoryQuotaStorage)(nil)
