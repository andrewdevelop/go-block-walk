package idx_test

import (
	"testing"

	. "github.com/andrewdevelop/go-block-walk"
)

func TestExhaustionTracker_TripsAtThreshold(t *testing.T) {
	tr := NewExhaustionTracker(3)

	if tr.Observe(false) {
		t.Fatal("should not trip on 1st consecutive miss")
	}
	if tr.Observe(false) {
		t.Fatal("should not trip on 2nd consecutive miss")
	}
	if !tr.Observe(false) {
		t.Fatal("should trip on 3rd consecutive miss")
	}
	if tr.Consecutive() != 3 {
		t.Fatalf("expected Consecutive() == 3, got %d", tr.Consecutive())
	}
}

func TestExhaustionTracker_AvailabilityResetsStreak(t *testing.T) {
	tr := NewExhaustionTracker(2)

	tr.Observe(false)
	if tr.Observe(true) {
		t.Fatal("an available observation should never itself trip")
	}
	if tr.Consecutive() != 0 {
		t.Fatalf("expected streak reset to 0, got %d", tr.Consecutive())
	}

	if tr.Observe(false) {
		t.Fatal("should not trip on the 1st miss after a reset")
	}
	if !tr.Observe(false) {
		t.Fatal("should trip on the 2nd miss after a reset")
	}
}

func TestExhaustionTracker_ThresholdBelowOneClampsToOne(t *testing.T) {
	tr := NewExhaustionTracker(0)
	if !tr.Observe(false) {
		t.Fatal("expected a threshold <1 to be clamped to 1, tripping on the first miss")
	}
}

func TestExhaustionTracker_KeepsTrippingPastThreshold(t *testing.T) {
	tr := NewExhaustionTracker(1)
	for i := 0; i < 5; i++ {
		if !tr.Observe(false) {
			t.Fatalf("iteration %d: expected continued tripping past the threshold", i)
		}
	}
}
