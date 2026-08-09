package idx_test

import (
	"sync"
	"testing"
	"time"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestNudgeSignal_FireDeliversOnce(t *testing.T) {
	s := NewNudgeSignal()
	s.Fire()

	select {
	case <-s.C():
	default:
		t.Fatal("expected a pending signal after Fire()")
	}

	select {
	case <-s.C():
		t.Fatal("expected no second signal without another Fire()")
	default:
	}
}

func TestNudgeSignal_MultipleFiresCoalesce(t *testing.T) {
	s := NewNudgeSignal()
	for i := 0; i < 10; i++ {
		s.Fire()
	}

	received := 0
	for {
		select {
		case <-s.C():
			received++
		default:
			goto done
		}
	}
done:
	if received != 1 {
		t.Fatalf("expected exactly 1 coalesced signal, got %d", received)
	}
}

func TestNudgeSignal_FireNeverBlocks(t *testing.T) {
	s := NewNudgeSignal()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			s.Fire()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Fire() should never block, even under repeated concurrent calls")
	}
}

func TestNudgeSignal_ConcurrentFireAndReceiveNoRace(t *testing.T) {
	s := NewNudgeSignal()
	var wg sync.WaitGroup

	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.Fire()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-s.C():
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}
