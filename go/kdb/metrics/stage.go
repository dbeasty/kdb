// Package metrics provides minimal, dependency-free per-stage latency
// tracking for the write path. It exists to give the Phase 0 baseline
// (see docs/benchmarks/phase0-baseline.md) a way to see where write
// latency actually goes: lock wait, fsync wait, and tree-rebuild time
// are tracked as separate named stages.
package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Stage names used across the write path. Kept as constants so the Go
// and Kotlin implementations report the same stage names in docs.
const (
	StageLockWait    = "lock_wait"
	StageFsyncWait   = "fsync_wait"
	StageTreeRebuild = "tree_rebuild"
)

// Recorder accumulates duration samples per named stage.
type Recorder struct {
	mu     sync.Mutex
	stages map[string]*stageData
}

type stageData struct {
	count   int64
	sumNs   int64
	samples []int64 // capped ring buffer of recent samples, for percentiles
}

const maxSamples = 4096

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{stages: make(map[string]*stageData)}
}

// Default is the process-wide recorder used by production call sites
// (server_engine.go, in_memory.go). Benchmarks and tests may construct
// their own Recorder instead when isolation is needed.
var Default = NewRecorder()

// Record adds one duration sample to the named stage.
func (r *Recorder) Record(stage string, d time.Duration) {
	if r == nil {
		return
	}
	ns := d.Nanoseconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	sd := r.stages[stage]
	if sd == nil {
		sd = &stageData{}
		r.stages[stage] = sd
	}
	sd.count++
	sd.sumNs += ns
	if len(sd.samples) < maxSamples {
		sd.samples = append(sd.samples, ns)
	} else {
		// Reservoir-style overwrite so long runs don't just capture the
		// first maxSamples ops; cheap deterministic slot reuse.
		sd.samples[sd.count%maxSamples] = ns
	}
}

// Track starts timing `stage` and returns a func to call when the stage
// completes. Usage: `defer metrics.Default.Track(metrics.StageLockWait)()`
func (r *Recorder) Track(stage string) func() {
	start := time.Now()
	return func() {
		r.Record(stage, time.Since(start))
	}
}

// StageSnapshot is a point-in-time summary of one stage's samples.
type StageSnapshot struct {
	Stage string
	Count int64
	Mean  time.Duration
	P50   time.Duration
	P99   time.Duration
	Max   time.Duration
}

// Snapshot returns a summary for every stage recorded so far, sorted by
// stage name for stable output.
func (r *Recorder) Snapshot() []StageSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StageSnapshot, 0, len(r.stages))
	for name, sd := range r.stages {
		if sd.count == 0 {
			continue
		}
		samples := append([]int64(nil), sd.samples...)
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		out = append(out, StageSnapshot{
			Stage: name,
			Count: sd.count,
			Mean:  time.Duration(sd.sumNs / sd.count),
			P50:   percentile(samples, 0.50),
			P99:   percentile(samples, 0.99),
			Max:   percentile(samples, 1.0),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stage < out[j].Stage })
	return out
}

// Reset clears all recorded samples. Intended for use between benchmark
// runs so each run reports its own numbers.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stages = make(map[string]*stageData)
}

func percentile(sortedNs []int64, p float64) time.Duration {
	if len(sortedNs) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sortedNs)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedNs) {
		idx = len(sortedNs) - 1
	}
	return time.Duration(sortedNs[idx])
}
