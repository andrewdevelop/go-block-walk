package idx

import (
	"github.com/ethereum/go-ethereum/core/types"
)

// EventDispatcher fans a decoded log out to every registered listener, in
// order, stopping at the first error. Safe for concurrent use; listeners
// are fixed at construction time.
type EventDispatcher struct {
	listeners []BlockchainListener
}

func NewEventDispatcher(listeners []BlockchainListener) *EventDispatcher {
	if listeners == nil {
		listeners = []BlockchainListener{}
	}
	return &EventDispatcher{listeners: listeners}
}

func (d *EventDispatcher) Dispatch(log types.Log, blockTimestamp uint64) error {
	for _, listener := range d.listeners {
		if err := listener.HandleLog(log, blockTimestamp); err != nil {
			return err
		}
	}
	return nil
}

func (d *EventDispatcher) Close() {}

var _ BlockchainEventDispatcher = (*EventDispatcher)(nil)
