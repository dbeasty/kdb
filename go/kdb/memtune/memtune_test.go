package memtune

import "testing"

func TestGCPercentLeavesSmallHeapsAlone(t *testing.T) {
	o := Options{}
	for _, live := range []int64{0, 10 << 20, 255 << 20} {
		if got := o.GCPercent(live); got != 100 {
			t.Errorf("live %d MiB: GOGC %d, want the runtime default 100", live>>20, got)
		}
	}
}

// The heap goal is live*(1+pct/100): the point is that it is live plus a bounded headroom, not
// twice live.
func TestGCPercentSetsHeadroomAboveLiveNotDoubleIt(t *testing.T) {
	o := Options{}
	cases := []struct {
		live     int64
		wantGoal int64 // heap goal in bytes
	}{
		{512 << 20, 640 << 20}, // 25% headroom is 128 MiB: exactly the floor
		{1 << 30, 1<<30 + 256<<20},
		{4 << 30, 4<<30 + 1<<30},
		{16 << 30, 16<<30 + 2<<30}, // capped at 2 GiB of headroom, not 4
	}
	for _, c := range cases {
		pct := o.GCPercent(c.live)
		goal := c.live + c.live*int64(pct)/100
		// Integer percent rounds down, so allow a sliver under.
		if goal > c.wantGoal || goal < c.wantGoal-c.wantGoal/50 {
			t.Errorf("live %d MiB: GOGC %d gives a goal of %d MiB, want about %d MiB",
				c.live>>20, pct, goal>>20, c.wantGoal>>20)
		}
		if goal >= 2*c.live {
			t.Errorf("live %d MiB: goal %d MiB is not below the default's 2x", c.live>>20, goal>>20)
		}
	}
}

func TestMinimumHeadroomApplies(t *testing.T) {
	// 300 MiB live, 25% would be 75 MiB; the 128 MiB floor wins.
	if got := (Options{}).GCPercent(300 << 20); got < 42 || got > 43 {
		t.Errorf("GOGC %d, want ~42 (128 MiB over 300 MiB)", got)
	}
}

func TestDisabledAndExplicitGOGC(t *testing.T) {
	if (Options{HeadroomPercent: -1}).GCPercent(8<<30) != 100 {
		t.Error("a disabled tuner must leave the default")
	}
	if tn := Start(Options{HeadroomPercent: -1}); tn != nil {
		t.Error("a disabled tuner started")
	}
	t.Setenv("GOGC", "50")
	if tn := Start(Options{}); tn != nil {
		t.Error("the tuner overrode an explicit GOGC")
	}
}
