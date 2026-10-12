package memtune

import (
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

var sink [][]byte

// peakHeapUnderChurn holds liveMB of live data, then allocates churnMB of short-lived garbage, and
// returns the highest allocated heap (live data plus garbage not yet collected) meanwhile, and the
// GC percent the tuner had set by the end (the runtime default when there is no tuner).
func peakHeapUnderChurn(liveMB, churnMB int, o *Options) (uint64, int) {
	debug.SetGCPercent(100)
	live := make([][]byte, 0, liveMB)
	for i := 0; i < liveMB; i++ {
		live = append(live, make([]byte, 1<<20))
		live[i][0] = 1
	}
	runtime.GC()
	var tuner *Tuner
	if o != nil {
		o.Interval = 20 * time.Millisecond
		tuner = Start(*o)
		time.Sleep(200 * time.Millisecond) // let it observe the live heap
	}
	var peak uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peak {
				peak = ms.HeapAlloc
			}
		}
	}()
	for i := 0; i < churnMB; i++ {
		sink = append(sink[:0], make([]byte, 1<<20))
	}
	close(stop)
	wg.Wait()
	pct := tuner.GCPercent()
	tuner.Stop()
	runtime.KeepAlive(live)
	return peak, pct
}

// With a large live heap the tuned collector's peak is well below the default's, which lets the
// heap double. Skipped under -short: it allocates a few GiB.
func TestTunedPeakHeapIsBelowTheDefaultUnderChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates several GiB")
	}
	if _, set := lookupGOGC(); set {
		t.Skip("GOGC set in the environment")
	}
	const liveMB, churnMB = 768, 3000
	def, _ := peakHeapUnderChurn(liveMB, churnMB, nil)
	tuned, pct := peakHeapUnderChurn(liveMB, churnMB, &Options{})
	t.Logf("live %d MiB: default peak %d MiB, tuned peak %d MiB at GC percent %d", liveMB, def>>20, tuned>>20, pct)
	// The tuner must have paced the collector to live + 25%: that is its whole job, and it does not
	// depend on the machine. (Live is slightly over 768 MiB, so the percent rounds to 24 or 25.)
	if want := (Options{}).GCPercent(int64(liveMB) << 20); pct < want-1 || pct > want+1 {
		t.Fatalf("tuner set GC percent %d with %d MiB live, want about %d", pct, liveMB, want)
	}
	if tuned >= def {
		t.Fatalf("tuned peak %d MiB is not below the default's %d MiB", tuned>>20, def>>20)
	}
	// The peak itself is not bounded here. The collector marks concurrently with a loop allocating
	// more than a GiB a second, so how far the peak overshoots the goal depends on the hardware:
	// +300 MiB over live held on a fast laptop, and CI's Linux runner has peaked at 1232, 1384 and
	// 1754 MiB against live 768 MiB, so absolute bounds of live+300 and live+75% both failed there.
}
