package idx

// Memory is an in-memory, thread-safe implementation of the full Storage
// interface, composed from three independent pieces — MemoryChainStorage,
// MemoryScoreStorage and MemoryQuotaStorage — each with its own lock and
// each independently usable/testable on its own. State is lost on process
// restart, making Memory a convenient default for tests, local development
// and demos. Production deployments should back Storage — or just the
// subset of it they actually need — with a persistent store instead.
type Memory struct {
	*MemoryChainStorage
	*MemoryScoreStorage
	*MemoryQuotaStorage
}

func NewMemory() *Memory {
	return &Memory{
		MemoryChainStorage: NewMemoryChainStorage(),
		MemoryScoreStorage: NewMemoryScoreStorage(),
		MemoryQuotaStorage: NewMemoryQuotaStorage(),
	}
}

var (
	_ Storage      = (*Memory)(nil)
	_ ChainStorage = (*Memory)(nil)
	_ ScoreStorage = (*Memory)(nil)
	_ QuotaStorage = (*Memory)(nil)
)
