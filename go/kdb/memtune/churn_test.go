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
// returns the highest allocated heap (live data plus garbage not yet collected) meanwhile.
func peakHeapUnderChurn(liveMB, churnMB int, o *Options) uint64 {
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
	tuner.Stop()
	runtime.KeepAlive(live)
	return peak
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
	def := peakHeapUnderChurn(liveMB, churnMB, nil)
	tuned := peakHeapUnderChurn(liveMB, churnMB, &Options{})
	t.Logf("live %d MiB: default peak %d MiB, tuned peak %d MiB", liveMB, def>>20, tuned>>20)
	if tuned >= def {
		t.Fatalf("tuned peak %d MiB is not below the default's %d MiB", tuned>>20, def>>20)
	}
	// The heap goal is live + 25% headroom, but the tuner re-reads the heap every 20ms while this
	// loop allocates more than a GiB a second, and the collector marks concurrently with the
	// mutator still allocating, so the peak overshoots the goal by what is allocated in between.
	// That is hardware dependent: +300 MiB over live held on a fast laptop and failed on CI's
	// Linux runner (1232 MiB peak against live 768 MiB; the default collector reached 1861 MiB).
	// Allow live + 75%: still far below the default's roughly +100% to +140%.
	if limit := uint64(liveMB+liveMB*3/4) << 20; tuned > limit {
		t.Errorf("tuned peak %d MiB exceeds live + headroom + overshoot (%d MiB)", tuned>>20, limit>>20)
	}
}
