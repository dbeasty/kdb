package server

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

func TestCostModelEstimateIsLinearInPayload(t *testing.T) {
	m := NewCostModel()
	base := m.Estimate(ClassWrite, 0)
	if base != costBasePerClass[ClassWrite] {
		t.Fatalf("zero-payload estimate should be the class base, got %d want %d", base, costBasePerClass[ClassWrite])
	}
	// Doubling the payload must add exactly k more per byte - the property the calibration
	// benchmark measures and the admission arithmetic assumes.
	a := m.Estimate(ClassWrite, 1000)
	b := m.Estimate(ClassWrite, 2000)
	if got, want := b-a, int64(costKPerClass[ClassWrite]*1000); got != want {
		t.Errorf("slope between 1000B and 2000B payloads = %d, want %d", got, want)
	}
}

// The estimate must never fall below what was actually measured, since under-estimating admits
// work the node cannot afford - the one direction §5.2 says must not happen.
func TestCostModelEstimateCoversMeasuredCost(t *testing.T) {
	m := NewCostModel()
	// Retained bytes measured by BenchmarkCommitBytesPerOp on the calibration run.
	for _, tc := range []struct{ payload, measured int }{
		{64, 7079}, {512, 7542}, {4096, 11814}, {32768, 47890},
	} {
		if got := m.Estimate(ClassWrite, tc.payload); got < int64(tc.measured) {
			t.Errorf("payload %d: estimate %d is below the measured retained cost %d - under-estimation admits work the node cannot afford",
				tc.payload, got, tc.measured)
		}
	}
}

func TestCostModelNegativeAndUnknownInputsAreSafe(t *testing.T) {
	m := NewCostModel()
	if got := m.Estimate(ClassWrite, -5); got != costBasePerClass[ClassWrite] {
		t.Errorf("negative payload should be treated as zero, got %d", got)
	}
	// An out-of-range class is charged as ClassWrite rather than as zero: an unknown operation
	// must not be the cheapest thing in the system to admit.
	if got := m.Estimate(OpClass(99), 0); got != costBasePerClass[ClassWrite] {
		t.Errorf("unknown class should be charged as ClassWrite, got %d", got)
	}
}

// The structural estimate must scale with what actually drives a scan's memory: how many rows
// the plan retains and how big the namespace's documents are - not the SQL string's length,
// which the old flat 1 MiB grant was the model conceding it could not use.
func TestStructuralScanEstimateScalesWithCardinality(t *testing.T) {
	m := NewCostModel()
	unbounded := sql.QueryShape{Fingerprint: "select [*] from t", ProjStar: true}
	small := m.EstimateScan(ScanEstimateInput{Namespace: "ns", Shape: unbounded, TreeSize: 100, MaxRows: 10_000})
	large := m.EstimateScan(ScanEstimateInput{Namespace: "ns", Shape: unbounded, TreeSize: 5_000, MaxRows: 10_000})
	if large <= small {
		t.Errorf("estimate must grow with namespace size: %d docs -> %d, %d docs -> %d", 100, small, 5_000, large)
	}
	// 5000 docs at the 2 KiB default is >20 MB retained under SELECT * - the flat 1 MiB grant
	// this replaces was off by more than an order of magnitude.
	if large < 20<<20 {
		t.Errorf("5000-doc SELECT * estimate = %d, want >= 20MiB - this is the under-reservation the structural model exists to fix", large)
	}
}

// LIMIT bounds a streaming plan's retention but not a blocking one's: ORDER BY and aggregates
// hold every matching row regardless of what the client asked to see.
func TestStructuralScanEstimateRespectsPlanShape(t *testing.T) {
	m := NewCostModel()
	in := ScanEstimateInput{Namespace: "ns", TreeSize: 100_000, MaxRows: 10_000, RowBudget: 100_000}

	in.Shape = sql.QueryShape{Fingerprint: "limited", Limit: 10}
	limited := m.EstimateScan(in)
	in.Shape = sql.QueryShape{Fingerprint: "ordered", Limit: 10, HasOrderBy: true}
	ordered := m.EstimateScan(in)
	if ordered <= limited*10 {
		t.Errorf("ORDER BY LIMIT 10 (%d) must cost far more than LIMIT 10 (%d): the sort materializes every matching row first", ordered, limited)
	}

	in.Shape = sql.QueryShape{Fingerprint: "agg", HasAggregate: true}
	agg := m.EstimateScan(in)
	if agg <= limited*10 {
		t.Errorf("aggregate (%d) must cost far more than LIMIT 10 (%d): COUNT(*) holds the whole input", agg, limited)
	}
}

// Observed document sizes must sharpen both scan and point-read estimates: a namespace of 64 KiB
// documents costs 32x the default assumption per row, and the estimator has that information
// after the first read.
func TestObservedDocSizesFeedEstimates(t *testing.T) {
	m := NewCostModel()
	before := m.EstimatePointRead("ns")
	for i := 0; i < 32; i++ {
		m.ObserveDocSize("ns", 64<<10)
	}
	after := m.EstimatePointRead("ns")
	if after <= before {
		t.Errorf("point-read estimate must rise once 64KiB documents are observed: before=%d after=%d", before, after)
	}
	if after < 128<<10 {
		t.Errorf("point read of a 64KiB-document namespace estimated at %d, want >= 2x the document", after)
	}
}

// A learned cell overrides the structural estimate only after enough consistent observations -
// and then actually overrides it, which is what lets a cheap selective query stop paying a
// worst-case reservation.
func TestLearnedCellOverridesStructuralAfterMinObservations(t *testing.T) {
	m := NewCostModel()
	shape := sql.QueryShape{Fingerprint: "select [a] from t where (b = ?)", HasPredicate: true}
	in := ScanEstimateInput{Namespace: "ns", Shape: shape, TreeSize: 50_000, MaxRows: 10_000}
	structural := m.EstimateScan(in)

	// A selective predicate: actual retained cost is tiny compared to the structural worst case.
	for i := 0; i < minCellObservations; i++ {
		if i < minCellObservations-1 {
			if got := m.EstimateScan(in); got != structural {
				t.Fatalf("estimate moved after only %d observations: got %d want structural %d", i, got, structural)
			}
		}
		m.ObserveScanActual(in, structural, 40_000)
	}
	learned := m.EstimateScan(in)
	if learned >= structural {
		t.Errorf("after %d consistent observations of ~40KB actuals the estimate must drop below structural %d, got %d", minCellObservations, structural, learned)
	}
	if learned < 40_000 {
		t.Errorf("learned estimate %d fell below the observed actual - headroom must keep it above, not at, the p95", learned)
	}
}

// The oscillation guard: when one shape produces wildly different costs (the classic
// parameter-sensitivity failure of grant feedback), the cell must NOT be trusted - estimation
// falls back to the structural worst case rather than whipsawing between under- and
// over-reservation.
func TestSpreadCellFallsBackToStructural(t *testing.T) {
	m := NewCostModel()
	shape := sql.QueryShape{Fingerprint: "select [*] from t where (b = ?)", ProjStar: true, HasPredicate: true}
	in := ScanEstimateInput{Namespace: "ns", Shape: shape, TreeSize: 50_000, MaxRows: 10_000}
	structural := m.EstimateScan(in)
	// Alternate 10KB and 10MB actuals - a 1000x spread, far past cellSpreadLimit.
	for i := 0; i < 2*minCellObservations; i++ {
		actual := int64(10_000)
		if i%2 == 1 {
			actual = 10_000_000
		}
		m.ObserveScanActual(in, structural, actual)
	}
	if got := m.EstimateScan(in); got != structural {
		t.Errorf("a cell with 1000x spread must not override the structural estimate: got %d want %d", got, structural)
	}
}

// Cells are bucketed by namespace scale: what was learned over 1k documents must not be applied
// verbatim once the namespace has grown past the next power-of-two bucket.
func TestLearnedCellsAreBucketedByTreeSize(t *testing.T) {
	m := NewCostModel()
	shape := sql.QueryShape{Fingerprint: "fp"}
	small := ScanEstimateInput{Namespace: "ns", Shape: shape, TreeSize: 1_000, MaxRows: 10_000}
	for i := 0; i < minCellObservations; i++ {
		m.ObserveScanActual(small, 1, 50_000)
	}
	grown := small
	grown.TreeSize = 1_000_000
	if got, structural := m.EstimateScan(grown), func() int64 {
		m2 := NewCostModel()
		return m2.EstimateScan(grown)
	}(); got != structural {
		t.Errorf("a cell learned at 1k docs must not price a 1M-doc scan: got %d want structural %d", got, structural)
	}
}

// The accuracy loop: sustained under-estimation must raise the class's safety multiplier (and
// with it future estimates), and accurate observations must decay it back. This is spec §12
// test 10's estimateAccuracy signal.
func TestAccuracyLoopRaisesAndDecaysSafetyMultiplier(t *testing.T) {
	m := NewCostModel()
	if got := m.SafetyMultiplier(ClassPointRead); got != 1.0 {
		t.Fatalf("initial safety multiplier = %v, want 1.0", got)
	}
	// Under-estimate by 3x repeatedly.
	for i := 0; i < 32; i++ {
		m.ObservePointReadActual("ns", 1000, 3000)
	}
	raised := m.SafetyMultiplier(ClassPointRead)
	if raised < 2.5 {
		t.Errorf("sustained 3x under-estimation must raise the multiplier toward 3, got %v", raised)
	}
	if p95 := m.AccuracyP95(ClassPointRead); p95 < 2.5 {
		t.Errorf("accuracy p95 should report the ~3x under-estimation, got %v", p95)
	}
	// Now the estimates (scaled up) become accurate: ratios ~1 refill the window.
	for i := 0; i < reservoirSize; i++ {
		m.ObservePointReadActual("ns", 3000, 3000)
	}
	decayed := m.SafetyMultiplier(ClassPointRead)
	if decayed > 1.5 {
		t.Errorf("safety multiplier must decay once estimates become accurate, still %v", decayed)
	}
}

// Learned state must survive a snapshot/restore round trip - and come back as a discounted
// prior, immediately usable but quickly outweighed by fresh observations.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	m := NewCostModel()
	shape := sql.QueryShape{Fingerprint: "select [a] from t where (b = ?)", HasPredicate: true}
	in := ScanEstimateInput{Namespace: "ns", Shape: shape, TreeSize: 50_000, MaxRows: 10_000}
	structural := m.EstimateScan(in)
	for i := 0; i < 4*minCellObservations; i++ {
		m.ObserveScanActual(in, structural, 40_000)
	}
	for i := 0; i < 32; i++ {
		m.ObserveDocSize("ns", 64<<10)
	}
	learned := m.EstimateScan(in)

	data, err := m.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCostModel()
	restored.RestoreState(data)
	if got := restored.EstimateScan(in); got != learned {
		t.Errorf("restored model estimates %d, want the learned %d - persistence exists precisely for rare shapes that would otherwise relearn from scratch", got, learned)
	}
	if got := restored.EstimatePointRead("ns"); got < 128<<10 {
		t.Errorf("restored doc sizes should survive: point read estimate %d, want >= 128KiB", got)
	}
}

// Restore must be crash-only-safe: garbage, an empty file, and a version mismatch are all
// silently ignored, never fatal, never partially applied.
func TestRestoreStateRejectsGarbageAndOldVersions(t *testing.T) {
	for _, data := range [][]byte{
		nil,
		{},
		[]byte("not json at all"),
		[]byte(`{"version":999,"cells":{"x":{"samples":[1],"n":1}}}`),
	} {
		m := NewCostModel()
		m.RestoreState(data)
		if got := m.LearnedCells(); got != 0 {
			t.Errorf("restore of %q loaded %d cells, want 0", data, got)
		}
	}
}

// The learned table must stay bounded however many distinct shapes a workload produces - an
// adversarial stream of unique queries degrades to the structural estimate, never to unbounded
// estimator memory.
func TestLearnedTableIsBounded(t *testing.T) {
	m := NewCostModel()
	in := ScanEstimateInput{Namespace: "ns", TreeSize: 1000, MaxRows: 10_000}
	for i := 0; i < 3*maxShapeCells; i++ {
		in.Shape = sql.QueryShape{Fingerprint: fmt.Sprintf("shape-%d", i)}
		m.ObserveScanActual(in, 1, 1000)
	}
	if got := m.LearnedCells(); got > maxShapeCells {
		t.Errorf("learned table grew to %d cells, cap is %d", got, maxShapeCells)
	}
}
