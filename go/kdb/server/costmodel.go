package server

import (
	"sort"
	"sync"
)

// OpClass classifies an operation by the shape of the resources it consumes - kdb-spec-layer13
// Component 48 §5.2. Classes are by operation *type*, never by caller or tenant (§14 non-goal):
// admission asks "what does this kind of work cost", not "who is asking".
type OpClass int

const (
	// ClassPointRead is a single-document lookup (DocumentGet) - bounded, cheap, and the last
	// class to be shed, because a server that cannot answer a point read is indistinguishable
	// from a server that is down.
	ClassPointRead OpClass = iota
	// ClassScan is a SQL SELECT. The hard case: cost tracks rows *examined*, which the request
	// size does not predict at all. Admitted against a conservative fixed grant plus a row
	// budget enforced during execution (§5.2) rather than a cleverer estimate.
	ClassScan
	// ClassWrite is Commit/Upsert/TxCommit - the class whose retained cost accumulates in the
	// DAG and therefore the one the whole grant system exists to bound.
	ClassWrite
	// ClassReplication is an inbound peer CommitPush. Structurally a write, but shed earlier
	// than client writes: a peer can resume from a log offset, a client cannot (§9.1).
	ClassReplication

	numOpClasses = 4
)

func (c OpClass) String() string {
	switch c {
	case ClassPointRead:
		return "point_read"
	case ClassScan:
		return "scan"
	case ClassWrite:
		return "write"
	case ClassReplication:
		return "replication"
	default:
		return "unknown"
	}
}

// Calibrated defaults, measured by BenchmarkCommitBytesPerOp in go/kdb/transaction (§5.2 requires
// these be measured, not guessed). On an Apple M3 Max, retained bytes per commit fit
// base+k*payload closely across a 64B-32KB payload sweep:
//
//	payload      64B ->  7,079 retained B/op
//	payload     512B ->  7,542 retained B/op
//	payload   4,096B -> 11,814 retained B/op
//	payload  32,768B -> 47,890 retained B/op
//
// Least-squares over the upper three points gives k = 1.25, base = 6,902. The constants below
// round that *up* (8 KiB base, k = 1.5), deliberately: §5.2 says under-estimation is the
// dangerous direction, since an under-estimate admits work the node cannot actually afford,
// which is the exact failure the grant system exists to prevent. ~17-20% headroom over the
// measured fit across the sweep is the cost of that bias, and it is worth paying.
var costBasePerClass = [numOpClasses]int64{
	ClassPointRead:   4 << 10, // decoded document + response buffer, released on completion
	ClassScan:        1 << 20, // conservative fixed grant; the row budget does the real bounding
	ClassWrite:       8 << 10, // measured: ~6.9 KiB, rounded up
	ClassReplication: 8 << 10, // structurally identical to ClassWrite
}

var costKPerClass = [numOpClasses]float64{
	ClassPointRead:   0.5, // response holds roughly the document, not a retained copy of it
	ClassScan:        1.0,
	ClassWrite:       1.5, // measured: 1.25, biased high
	ClassReplication: 1.5,
}

// kMin/kMax clamp what feedback may do to k (§5.2: "clamped to a configured range so a
// pathological sample cannot poison the estimator"). A single enormous outlier - a document that
// happens to expand pathologically, or a GC that lands mid-measurement - must not be able to
// drive the estimator to a value that either wedges the server (k far too high, nothing is ever
// admitted) or disarms it (k far too low, everything is admitted and the node dies).
const (
	costKMin = 0.25
	costKMax = 8.0
)

// costFeedbackWindow is how many recent observations per class the p95 is computed over. A plain
// bounded ring with an exact p95 over it, rather than a streaming quantile sketch: at this size
// the sort is trivially cheap, and an exact number over a known window is far easier to reason
// about (and to test) than an approximate one over an unbounded history.
const costFeedbackWindow = 256

// CostModel estimates the bytes an operation will hold live until it completes, and refines that
// estimate from observed actuals - kdb-spec-layer13 Component 48 §5.2. Safe for concurrent use.
//
// Per design principle P2 (feed actuals back), each completed operation reports its real cost;
// k then tracks the p95 of observed bytes-per-payload-byte rather than staying pinned to a
// compile-time constant that was only ever right for the workload it was measured on.
type CostModel struct {
	mu    sync.RWMutex
	base  [numOpClasses]int64
	k     [numOpClasses]float64
	ratio [numOpClasses][]float64 // ring of observed (actual-base)/payload, newest appended
	next  [numOpClasses]int
}

// NewCostModel returns a model seeded with the calibrated constants above.
func NewCostModel() *CostModel {
	m := &CostModel{base: costBasePerClass, k: costKPerClass}
	return m
}

// Estimate returns the bytes an operation of this class and payload size is expected to hold
// live until it completes. Never returns less than the class's base, and never negative.
func (m *CostModel) Estimate(class OpClass, payloadBytes int) int64 {
	if !class.valid() {
		class = ClassWrite // unknown work is charged as the most expensive ordinary class
	}
	if payloadBytes < 0 {
		payloadBytes = 0
	}
	m.mu.RLock()
	base, k := m.base[class], m.k[class]
	m.mu.RUnlock()
	return base + int64(k*float64(payloadBytes))
}

// Observe records an operation's real retained cost so k can track reality (§5.2's P2 feedback
// loop). payloadBytes <= 0 carries no slope information and is ignored - the base term already
// covers a zero-payload operation, and dividing by it would be meaningless.
func (m *CostModel) Observe(class OpClass, payloadBytes int, actualBytes int64) {
	if !class.valid() || payloadBytes <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ratio := float64(actualBytes-m.base[class]) / float64(payloadBytes)
	if ratio < 0 {
		ratio = 0 // an operation cheaper than its base is information about base, not about k
	}
	ring := m.ratio[class]
	if len(ring) < costFeedbackWindow {
		m.ratio[class] = append(ring, ratio)
	} else {
		ring[m.next[class]] = ratio
		m.next[class] = (m.next[class] + 1) % costFeedbackWindow
	}
	m.k[class] = clampFloat(percentile95(m.ratio[class]), costKMin, costKMax)
}

// KFor exposes the current slope for a class - for metrics and tests, not for admission, which
// goes through Estimate.
func (m *CostModel) KFor(class OpClass) float64 {
	if !class.valid() {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.k[class]
}

func (c OpClass) valid() bool { return c >= 0 && int(c) < numOpClasses }

// percentile95 returns the p95 of vals using nearest-rank, on a copy - the caller's ring must not
// be reordered, since its slot ordering is what makes it a ring.
func percentile95(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := int(0.95*float64(len(sorted))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
