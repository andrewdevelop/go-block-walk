package idx

// NudgeSignal is a coalescing wake-up signal: any number of Fire() calls
// before the receiver drains C() collapse into a single pending wakeup.
// This lets many concurrent triggers (e.g. several "I just confirmed a tx"
// acks arriving faster than the indexer can react) cost at most one extra
// early sync instead of queuing one per trigger. Safe for concurrent use.
type NudgeSignal struct {
	ch chan struct{}
}

func NewNudgeSignal() *NudgeSignal {
	return &NudgeSignal{ch: make(chan struct{}, 1)}
}

// Fire requests a wakeup. Never blocks.
func (s *NudgeSignal) Fire() {
	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// C receives one value per pending, not-yet-delivered Fire() (never more
// than one, no matter how many times Fire() was called since the last
// receive).
func (s *NudgeSignal) C() <-chan struct{} {
	return s.ch
}
