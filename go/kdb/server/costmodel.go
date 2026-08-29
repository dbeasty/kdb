package server

import (
	"encoding/json"
	"math/bits"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/sql"
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
	// ClassScan is a SQL SELECT. Cost tracks rows examined and retained, which the request size
	// does not predict at all - so scans are estimated structurally (namespace cardinality x
	// observed document size x plan shape) and refined by a learned per-shape table, rather
	// than from payload bytes.
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
//
// Writes stay on this static calibrated model, on purpose. Their retained cost is stable,
// measured, and benchmark-guarded; the earlier online feedback for writes measured a different
// quantity than the calibration (cumulative process-wide allocated bytes vs. retained bytes) and
// could only push k toward its clamp, so it was removed rather than repaired. Online learning
// applies where the static model is hopeless AND actuals are exactly measurable: scans and point
// reads, whose executor reports precisely what it materialized (sql.ExecStats).
var costBasePerClass = [numOpClasses]int64{
	ClassPointRead:   4 << 10,  // decoded document + response buffer, released on completion
	ClassScan:        64 << 10, // absolute floor; the structural estimate does the real sizing
	ClassWrite:       8 << 10,  // measured: ~6.9 KiB, rounded up
	ClassReplication: 8 << 10,  // structurally identical to ClassWrite
}

var costKPerClass = [numOpClasses]float64{
	ClassPointRead:   0.5, // response holds roughly the document, not a retained copy of it
	ClassScan:        1.0,
	ClassWrite:       1.5, // measured: 1.25, biased high
	ClassReplication: 1.5,
}

const (
	// defaultDocBytes stands in for a namespace's document size before any has been observed.
	defaultDocBytes = 2048

	// scanRowOverheadBytes mirrors sql's retainedRowOverheadBytes: the per-row fixed cost
	// around each held document (pair struct, slice headers, interface values).
	scanRowOverheadBytes = 128

	// minCellObservations is how many actuals a learned cell needs before it may override the
	// structural estimate. Below this, one or two lucky cheap runs must not talk admission into
	// under-reserving.
	minCellObservations = 8

	// cellSpreadLimit is the oscillation guard: when a cell's observed max/min ratio exceeds
	// this, the same shape is producing wildly different costs (the classic
	// parameter-sensitivity failure of grant feedback - one fingerprint covering both "LIMIT 5"
	// point-ish lookups and full-namespace pulls), and the cell is treated as unreliable:
	// estimation falls back to the structural model rather than trusting a p95 that whipsaws.
	cellSpreadLimit = 64.0

	// learnedHeadroom multiplies a learned p95 so the estimate sits above the typical actual
	// rather than at it - under-estimation is the dangerous direction.
	learnedHeadroom = 1.25

	// reservoirSize bounds each per-cell / per-namespace sample ring. Exact percentiles over a
	// small known window, same reasoning as the earlier costFeedbackWindow: trivially cheap,
	// and far easier to reason about and test than a streaming sketch.
	reservoirSize = 64

	// maxNamespaces / maxShapeCells bound the learned state. Beyond the cap, the oldest entry
	// is evicted (plain FIFO by insertion - shapes come from application code and are few; the
	// cap is a safety net against an adversarial workload of unique shapes, which simply
	// degrades to the structural estimate, never to unbounded memory).
	maxNamespaces = 256
	maxShapeCells = 512

	// accuracyWindow documents the per-class ring size behind the safety multiplier and the
	// estimateAccuracy metric (spec §12 test 10); the reservoir's own cap enforces it.
	accuracyWindow = reservoirSize

	// safetyMax caps how far sustained under-estimation may scale estimates up. Mirrors the old
	// costKMax reasoning: a pathological run must not wedge the server by inflating every
	// estimate past capacity.
	safetyMax = 8.0

	// costStateVersion versions the persisted state; a mismatch discards it and relearns.
	costStateVersion = 1
)

// reservoir is a bounded sample ring with exact quantiles. Not safe for concurrent use - the
// CostModel's lock covers it. Fields are exported only for JSON persistence (SnapshotState).
type reservoir struct {
	Samples []float64 `json:"samples"`
	Next    int       `json:"next"`
	N       int64     `json:"n"` // total observations ever, for min-observation gates
}

func (r *reservoir) add(v float64) {
	if len(r.Samples) < reservoirSize {
		r.Samples = append(r.Samples, v)
	} else {
		r.Samples[r.Next] = v
		r.Next = (r.Next + 1) % reservoirSize
	}
	r.N++
}

// quantile returns the q-quantile (nearest-rank) of the current window; 0 when empty.
func (r *reservoir) quantile(q float64) float64 {
	if len(r.Samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(r.Samples))
	copy(sorted, r.Samples)
	sort.Float64s(sorted)
	idx := int(q*float64(len(sorted))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// spread returns max/min over the window; 1 when empty, and past the limit when any sample is
// non-positive (zero-cost samples in the mix make a ratio meaningless).
func (r *reservoir) spread() float64 {
	if len(r.Samples) == 0 {
		return 1
	}
	lo, hi := r.Samples[0], r.Samples[0]
	for _, v := range r.Samples[1:] {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	if lo <= 0 {
		return cellSpreadLimit + 1
	}
	return hi / lo
}

// CostModel estimates the bytes an operation will hold live until it completes, and refines that
// estimate from observed actuals - kdb-spec-layer13 Component 48 §5.2, design principle P2
// ("cost is estimated, then measured"). Safe for concurrent use.
//
// Three tiers, cheapest-to-compute first:
//
//  1. Static linear (writes, replication): base + k*payloadBytes, calibrated offline by
//     BenchmarkCommitBytesPerOp and guarded by costmodel_test against the recorded sweep.
//  2. Structural (scans, point reads): built from what the node already knows for free at
//     admission time - the namespace's exact cardinality (DocumentTree.Size, O(1)), the
//     observed p95 document size, and the query's shape (ORDER BY / aggregates / SELECT * /
//     LIMIT). This replaces the flat 1 MiB scan grant, which was wrong by orders of magnitude
//     in both directions.
//  3. Learned (scans): a bounded table keyed on (namespace, query shape, log2 cardinality
//     bucket) holding a p95 of exactly-measured actuals (sql.ExecStats.RetainedBytes). A cell
//     overrides the structural estimate only once it has enough observations and its samples
//     are not wildly spread (oscillation guard); otherwise the structural estimate stands.
//
// An accuracy loop closes the whole thing: every operation that reports an actual also records
// actual/estimate, and a per-class safety multiplier scales future estimates up while the p95 of
// that ratio shows systematic under-estimation, decaying back as accuracy recovers.
type CostModel struct {
	mu   sync.RWMutex
	base [numOpClasses]int64
	k    [numOpClasses]float64

	docSizes  map[string]*reservoir // namespace -> observed len(doc.JSON)
	docOrder  []string              // FIFO eviction order for docSizes
	cells     map[string]*reservoir // shape cell -> observed actual retained bytes
	cellOrder []string              // FIFO eviction order for cells

	accuracy [numOpClasses]reservoir // actual/estimate ratios, only for reported actuals
	safety   [numOpClasses]float64   // multiplier derived from accuracy; 1.0 = trusted
}

// NewCostModel returns a model seeded with the calibrated constants above.
func NewCostModel() *CostModel {
	m := &CostModel{
		base:     costBasePerClass,
		k:        costKPerClass,
		docSizes: make(map[string]*reservoir),
		cells:    make(map[string]*reservoir),
	}
	for i := range m.safety {
		m.safety[i] = 1.0
	}
	return m
}

// Estimate returns the bytes an operation of this class and payload size is expected to hold
// live until it completes - the static linear tier. Scans and point reads have richer entry
// points (EstimateScan, EstimatePointRead); this remains correct for them as a fallback when the
// caller has nothing but a payload size.
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

// ScanEstimateInput is everything EstimateScan consumes. All of it is available before the scan
// runs, and none of it costs more than O(1) to obtain.
type ScanEstimateInput struct {
	Namespace string
	Shape     sql.QueryShape
	// TreeSize is the namespace's document count at the read head - DocumentTree.Size(), O(1).
	TreeSize int
	// MaxRows is the server's result-row cap for this request (wire_listen's 10_000).
	MaxRows int
	// RowBudget is the current admission row budget (rows-examined bound); 0 = unbounded.
	RowBudget int
}

// EstimateScan sizes a SELECT before it runs: learned cell when reliable, structural otherwise.
func (m *CostModel) EstimateScan(in ScanEstimateInput) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := cellKey(in.Namespace, in.Shape.Fingerprint, in.TreeSize)
	if cell, ok := m.cells[key]; ok && cell.N >= minCellObservations && cell.spread() <= cellSpreadLimit {
		learned := int64(cell.quantile(0.95) * learnedHeadroom * m.safety[ClassScan])
		if learned < m.base[ClassScan] {
			learned = m.base[ClassScan]
		}
		return learned
	}
	return m.structuralScanLocked(in)
}

// structuralScanLocked is the tier-2 estimate. Requires at least a read lock.
//
// The retained peak of a SELECT is (rows the executor holds simultaneously) x (per-row cost),
// plus a fixed floor. Which rows it holds depends on plan shape:
//
//   - blocking plans (ORDER BY, aggregates) materialize every matching row before producing
//     any output, so LIMIT does not bound retention - only the row budget (rows examined) does;
//   - streaming plans retain at most the result bound (LIMIT if present, else MaxRows).
//
// Per-row cost is the observed p95 document size plus fixed overhead, multiplied by how many
// copies the server holds at peak: the materialized documents, the projected cells (~document
// size again under SELECT *), and the wire row strings. Biased high throughout - admission's
// safe direction - and corrected downward by the learned tier, not by shaving margins here.
func (m *CostModel) structuralScanLocked(in ScanEstimateInput) int64 {
	docBytes := int64(defaultDocBytes)
	if r, ok := m.docSizes[in.Namespace]; ok && r.N > 0 {
		docBytes = int64(r.quantile(0.95))
	}

	rows := int64(in.TreeSize)
	if in.Shape.HasOrderBy || in.Shape.HasAggregate {
		if in.RowBudget > 0 && int64(in.RowBudget) < rows {
			rows = int64(in.RowBudget)
		}
	} else {
		bound := int64(in.MaxRows)
		if in.Shape.Limit > 0 && int64(in.Shape.Limit) < bound {
			bound = int64(in.Shape.Limit)
		}
		if bound > 0 && bound < rows {
			rows = bound
		}
	}

	copies := int64(2) // materialized documents + projected/encoded result
	if in.Shape.ProjStar {
		copies = 3 // projection carries the whole document JSON, and the wire copy again
	}
	if in.Shape.HasAggregate {
		copies = 1 // aggregate input is held once; the output is a single row
	}

	return m.base[ClassScan] + rows*(docBytes+scanRowOverheadBytes)*copies
}

// EstimatePointRead sizes a DocumentGet from the namespace's observed document sizes.
func (m *CostModel) EstimatePointRead(namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	docBytes := int64(defaultDocBytes)
	if r, ok := m.docSizes[namespace]; ok && r.N > 0 {
		docBytes = int64(r.quantile(0.95))
	}
	// The document plus its response copy, floored at the calibrated base.
	est := int64(float64(m.base[ClassPointRead]) * m.safety[ClassPointRead])
	if v := int64(float64(docBytes*2) * m.safety[ClassPointRead]); v > est {
		est = v
	}
	return est
}

// ObserveDocSize records one observed document JSON size for a namespace. Fed by the read path;
// cheap enough for every document that crosses it.
func (m *CostModel) ObserveDocSize(namespace string, jsonBytes int) {
	if jsonBytes <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.docSizes[namespace]
	if !ok {
		if len(m.docSizes) >= maxNamespaces {
			oldest := m.docOrder[0]
			m.docOrder = m.docOrder[1:]
			delete(m.docSizes, oldest)
		}
		r = &reservoir{}
		m.docSizes[namespace] = r
		m.docOrder = append(m.docOrder, namespace)
	}
	r.add(float64(jsonBytes))
}

// ObserveScanActual feeds a completed scan's exactly-measured retained bytes back into the
// learned table and the accuracy loop. estimate is what admission reserved for it.
func (m *CostModel) ObserveScanActual(in ScanEstimateInput, estimate, actualBytes int64) {
	if actualBytes < 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := cellKey(in.Namespace, in.Shape.Fingerprint, in.TreeSize)
	cell, ok := m.cells[key]
	if !ok {
		if len(m.cells) >= maxShapeCells {
			oldest := m.cellOrder[0]
			m.cellOrder = m.cellOrder[1:]
			delete(m.cells, oldest)
		}
		cell = &reservoir{}
		m.cells[key] = cell
		m.cellOrder = append(m.cellOrder, key)
	}
	cell.add(float64(actualBytes))
	m.recordAccuracyLocked(ClassScan, estimate, actualBytes)
}

// ObservePointReadActual feeds a completed point read's actual response bytes back.
func (m *CostModel) ObservePointReadActual(namespace string, estimate, actualBytes int64) {
	if actualBytes < 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordAccuracyLocked(ClassPointRead, estimate, actualBytes)
}

// recordAccuracyLocked updates the class's actual/estimate ring and recomputes its safety
// multiplier. Requires the write lock.
//
// The multiplier is the p95 of actual/estimate clamped to [1, safetyMax]: while estimates
// systematically under-shoot, everything in the class is scaled up by how badly; as accurate
// samples refill the window the p95 falls and the multiplier decays back to 1 on its own. This
// is also the estimateAccuracy signal spec §12 test 10 asserts on.
func (m *CostModel) recordAccuracyLocked(class OpClass, estimate, actual int64) {
	if estimate <= 0 {
		return
	}
	m.accuracy[class].add(float64(actual) / float64(estimate))
	p95 := m.accuracy[class].quantile(0.95)
	m.safety[class] = clampFloat(p95, 1.0, safetyMax)
}

// AccuracyP95 reports the current p95 of actual/estimate for a class - above 1 means the class
// is under-estimated at the tail. Zero when nothing has been observed yet.
func (m *CostModel) AccuracyP95(class OpClass) float64 {
	if !class.valid() {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accuracy[class].quantile(0.95)
}

// SafetyMultiplier reports the class's current estimate scale-up. 1.0 = estimates trusted.
func (m *CostModel) SafetyMultiplier(class OpClass) float64 {
	if !class.valid() {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.safety[class]
}

// KFor exposes the static slope for a class - for metrics and tests.
func (m *CostModel) KFor(class OpClass) float64 {
	if !class.valid() {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.k[class]
}

// baseFor returns the class's floor cost - the minimum any grant of this class reserves.
func (m *CostModel) baseFor(class OpClass) int64 {
	if !class.valid() {
		class = ClassWrite
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.base[class]
}

// LearnedCells reports how many shape cells the model currently holds - for metrics.
func (m *CostModel) LearnedCells() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.cells)
}

// cellKey buckets a namespace+shape by log2 of the tree size, so a cell learned at one
// namespace scale is not applied verbatim at another: a shape observed over 1k documents says
// nothing reliable about the same shape over 1M. Growth simply starts a fresh bucket that
// falls back to the structural estimate until it has its own observations.
func cellKey(namespace, fingerprint string, treeSize int) string {
	if treeSize < 0 {
		treeSize = 0
	}
	bucket := bits.Len(uint(treeSize))
	return namespace + "\x00" + fingerprint + "\x00" + string(rune('a'+bucket))
}

func (c OpClass) valid() bool { return c >= 0 && int(c) < numOpClasses }

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// costState is the persisted form of the learned tiers. The static calibrated constants are
// never persisted - they ship with the binary, and a new build's constants must win.
type costState struct {
	Version   int                   `json:"version"`
	DocSizes  map[string]*reservoir `json:"docSizes"`
	Cells     map[string]*reservoir `json:"cells"`
	DocOrder  []string              `json:"docOrder"`
	CellOrder []string              `json:"cellOrder"`
}

// SnapshotState serializes the learned state (per-namespace doc sizes and shape cells) for
// persistence across restarts. The accuracy rings and safety multipliers are deliberately not
// persisted: they describe how well the *running* estimator tracked the *recent* workload, and
// a restart should re-earn that trust rather than inherit it.
func (m *CostModel) SnapshotState() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(costState{
		Version:   costStateVersion,
		DocSizes:  m.docSizes,
		Cells:     m.cells,
		DocOrder:  m.docOrder,
		CellOrder: m.cellOrder,
	})
}

// RestoreState loads a prior SnapshotState as a *discounted prior*: each restored reservoir's
// observation count is capped at minCellObservations, so restored knowledge is immediately
// usable (a rare scan shape keeps its learned cost across a restart - the actual point of
// persisting it) but is outweighed by fresh observations almost immediately if the workload has
// changed. Malformed or version-mismatched state is discarded without error, per P4
// (crash-only): a bad cost file must never stop the server from starting - it just relearns.
func (m *CostModel) RestoreState(data []byte) {
	var st costState
	if err := json.Unmarshal(data, &st); err != nil || st.Version != costStateVersion {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for ns, r := range st.DocSizes {
		if r == nil || len(r.Samples) == 0 || len(m.docSizes) >= maxNamespaces {
			continue
		}
		if r.N > minCellObservations {
			r.N = minCellObservations
		}
		if _, exists := m.docSizes[ns]; !exists {
			m.docSizes[ns] = r
			m.docOrder = append(m.docOrder, ns)
		}
	}
	for key, r := range st.Cells {
		if r == nil || len(r.Samples) == 0 || len(m.cells) >= maxShapeCells {
			continue
		}
		if r.N > minCellObservations {
			r.N = minCellObservations
		}
		if _, exists := m.cells[key]; !exists {
			m.cells[key] = r
			m.cellOrder = append(m.cellOrder, key)
		}
	}
}
