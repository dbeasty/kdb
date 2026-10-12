// Package memtune adapts the Go collector's pacing to the size of the live heap.
//
// The runtime's default (GOGC=100) lets the heap grow to twice its live size before collecting.
// That is the right trade for a small heap and a poor one for a large one: a service holding a
// gigabyte of documents carries another gigabyte of garbage between cycles purely so the
// collector can run rarely, and the machine has to be sized for that peak. memtune keeps GOGC at
// the default while the heap is small, and above that sets it so the heap goal is the live size
// plus a headroom: a fraction of the live heap (room for growth and for the allocation between
// two cycles), never less than a floor so a cycle is not forced every few megabytes.
//
// It only adjusts the collector's pacing. It never sets a memory limit, and it stands down when
// the operator has set GOGC themselves.
package memtune

import (
	"context"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// Options configure Start. The zero value of each field selects its default.
type Options struct {
	// HeadroomPercent is the headroom kept above the live heap, as a percentage of it (default
	// 25). 0 or less disables the tuner; 100 or more is the runtime's own behaviour.
	HeadroomPercent int
	// MinHeadroomBytes is the least headroom allowed, so a heap just over SmallHeapBytes is not
	// collected every few megabytes (default 128 MiB).
	MinHeadroomBytes int64
	// MaxHeadroomBytes caps the headroom, so a very large heap does not carry a quarter of itself
	// as garbage (default 2 GiB).
	MaxHeadroomBytes int64
	// SmallHeapBytes is the live size below which GOGC stays at the runtime default (default
	// 256 MiB). Collecting a small heap often is cheap and not worth tuning.
	SmallHeapBytes int64
	// Interval is how often the live heap is sampled (default 2 s).
	Interval time.Duration
}

const (
	defaultHeadroomPercent        = 25
	defaultMinHeadroomBytes int64 = 128 << 20
	defaultMaxHeadroomBytes int64 = 2 << 30
	defaultSmallHeapBytes   int64 = 256 << 20
	defaultInterval               = 2 * time.Second
	runtimeDefaultGCPercent       = 100
	minGCPercent                  = 5
)

// GCPercent is the GOGC value for a live heap of liveBytes under o: the runtime default for a
// small heap, otherwise whatever puts the heap goal at live plus headroom.
func (o Options) GCPercent(liveBytes int64) int {
	o = o.withDefaults()
	if o.HeadroomPercent <= 0 || o.HeadroomPercent >= runtimeDefaultGCPercent {
		return runtimeDefaultGCPercent
	}
	if liveBytes < o.SmallHeapBytes || liveBytes <= 0 {
		return runtimeDefaultGCPercent
	}
	headroom := liveBytes * int64(o.HeadroomPercent) / 100
	if headroom > o.MaxHeadroomBytes {
		headroom = o.MaxHeadroomBytes
	}
	if headroom < o.MinHeadroomBytes {
		headroom = o.MinHeadroomBytes
	}
	pct := int(headroom * 100 / liveBytes)
	if pct > runtimeDefaultGCPercent {
		pct = runtimeDefaultGCPercent
	}
	if pct < minGCPercent {
		pct = minGCPercent
	}
	return pct
}

func (o Options) withDefaults() Options {
	if o.HeadroomPercent == 0 {
		o.HeadroomPercent = defaultHeadroomPercent
	}
	if o.MinHeadroomBytes <= 0 {
		o.MinHeadroomBytes = defaultMinHeadroomBytes
	}
	if o.MaxHeadroomBytes <= 0 {
		o.MaxHeadroomBytes = defaultMaxHeadroomBytes
	}
	if o.SmallHeapBytes <= 0 {
		o.SmallHeapBytes = defaultSmallHeapBytes
	}
	if o.Interval <= 0 {
		o.Interval = defaultInterval
	}
	return o
}

// Tuner is a running tuner.
type Tuner struct {
	cancel  context.CancelFunc
	done    chan struct{}
	current atomic.Int64
}

// Active reports whether a tuner was actually started (false when disabled or when GOGC is set).
func (t *Tuner) Active() bool { return t != nil }

// GCPercent is the value the tuner last applied.
func (t *Tuner) GCPercent() int {
	if t == nil {
		return runtimeDefaultGCPercent
	}
	return int(t.current.Load())
}

// Stop ends the tuner and restores the runtime default. Safe on a nil Tuner.
func (t *Tuner) Stop() {
	if t == nil {
		return
	}
	t.cancel()
	<-t.done
	debug.SetGCPercent(runtimeDefaultGCPercent)
}

// Start begins tuning and returns the Tuner, or nil when tuning is disabled or the operator set
// GOGC in the environment (an explicit setting is never overridden).
func Start(o Options) *Tuner {
	o = o.withDefaults()
	if o.HeadroomPercent <= 0 {
		return nil
	}
	if _, set := os.LookupEnv("GOGC"); set {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Tuner{cancel: cancel, done: make(chan struct{})}
	t.current.Store(runtimeDefaultGCPercent)
	go t.run(ctx, o)
	return t
}

func (t *Tuner) run(ctx context.Context, o Options) {
	defer close(t.done)
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		metrics.Read(sample)
		if sample[0].Value.Kind() != metrics.KindUint64 {
			continue
		}
		want := o.GCPercent(int64(sample[0].Value.Uint64()))
		cur := int(t.current.Load())
		// Hysteresis: a change of a few points is noise, and each SetGCPercent briefly stops the
		// world.
		if diff := want - cur; diff < 5 && diff > -5 && want != runtimeDefaultGCPercent {
			continue
		}
		if want != cur {
			debug.SetGCPercent(want)
			t.current.Store(int64(want))
		}
	}
}

func lookupGOGC() (string, bool) { return os.LookupEnv("GOGC") }
