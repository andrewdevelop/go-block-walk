package idx_test

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"

	. "github.com/andrewdevelop/go-block-walk"
)

type recordingListener struct {
	handled []uint64
	err     error
}

func (l *recordingListener) HandleLog(log types.Log, blockTimestamp uint64) error {
	l.handled = append(l.handled, blockTimestamp)
	return l.err
}

func TestEventDispatcher_DispatchesToAllListenersInOrder(t *testing.T) {
	l1 := &recordingListener{}
	l2 := &recordingListener{}
	d := NewEventDispatcher([]BlockchainListener{l1, l2})

	if err := d.Dispatch(types.Log{}, 100); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(l1.handled) != 1 || l1.handled[0] != 100 {
		t.Fatalf("expected listener 1 to receive timestamp 100, got %v", l1.handled)
	}
	if len(l2.handled) != 1 || l2.handled[0] != 100 {
		t.Fatalf("expected listener 2 to receive timestamp 100, got %v", l2.handled)
	}
}

func TestEventDispatcher_StopsAtFirstError(t *testing.T) {
	wantErr := errors.New("listener failed")
	l1 := &recordingListener{err: wantErr}
	l2 := &recordingListener{}
	d := NewEventDispatcher([]BlockchainListener{l1, l2})

	err := d.Dispatch(types.Log{}, 1)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the failing listener's error, got %v", err)
	}
	if len(l2.handled) != 0 {
		t.Fatal("expected dispatch to stop before reaching the second listener")
	}
}

func TestEventDispatcher_NilListenersIsSafe(t *testing.T) {
	d := NewEventDispatcher(nil)
	if err := d.Dispatch(types.Log{}, 1); err != nil {
		t.Fatalf("unexpected error with no listeners: %v", err)
	}
}

func TestEventDispatcher_CloseIsSafe(t *testing.T) {
	d := NewEventDispatcher(nil)
	d.Close()
	d.Close() // must remain safe to call more than once
}
