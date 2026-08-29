package server

import (
	"math"
	"testing"
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

// Feedback must move k toward observed reality (P2), which is the whole reason the model is not
// just a pair of constants.
func TestCostModelFeedbackTracksObservedCost(t *testing.T) {
	m := NewCostModel()
	before := m.KFor(ClassWrite)
	// Observe operations costing ~4 bytes per payload byte, well above the seeded 1.5.
	for i := 0; i < 100; i++ {
		m.Observe(ClassWrite, 1000, costBasePerClass[ClassWrite]+4000)
	}
	after := m.KFor(ClassWrite)
	if after <= before {
		t.Fatalf("k should rise toward observed cost: before=%v after=%v", before, after)
	}
	if math.Abs(after-4.0) > 0.5 {
		t.Errorf("k should converge near the observed 4.0, got %v", after)
	}
}

// A single pathological sample must not be able to wedge the server (k so high nothing is ever
// admitted) or disarm it (k so low everything is).
func TestCostModelFeedbackIsClamped(t *testing.T) {
	m := NewCostModel()
	for i := 0; i < 500; i++ {
		m.Observe(ClassWrite, 1, math.MaxInt32)
	}
	if got := m.KFor(ClassWrite); got > costKMax {
		t.Errorf("k must be clamped at %v, got %v", costKMax, got)
	}

	m2 := NewCostModel()
	for i := 0; i < 500; i++ {
		m2.Observe(ClassWrite, 1_000_000, 0) // absurdly cheap
	}
	if got := m2.KFor(ClassWrite); got < costKMin {
		t.Errorf("k must be clamped at %v, got %v", costKMin, got)
	}
}

// p95, not the mean: the estimator must track a high percentile so a workload that is usually
// cheap but occasionally expensive is still admitted against its expensive case.
func TestCostModelUsesHighPercentileNotMean(t *testing.T) {
	m := NewCostModel()
	// 90 cheap observations, 10 expensive ones. The mean ratio is ~1.3; the p95 is 6.
	for i := 0; i < 90; i++ {
		m.Observe(ClassWrite, 1000, costBasePerClass[ClassWrite]+1000) // ratio 1
	}
	for i := 0; i < 10; i++ {
		m.Observe(ClassWrite, 1000, costBasePerClass[ClassWrite]+6000) // ratio 6
	}
	if got := m.KFor(ClassWrite); got < 5 {
		t.Errorf("k should reflect the p95 (6), not the mean (~1.5), got %v", got)
	}
}

func TestCostModelFeedbackWindowIsBounded(t *testing.T) {
	m := NewCostModel()
	for i := 0; i < costFeedbackWindow*4; i++ {
		m.Observe(ClassWrite, 100, costBasePerClass[ClassWrite]+100)
	}
	if got := len(m.ratio[ClassWrite]); got != costFeedbackWindow {
		t.Errorf("feedback ring must stay bounded at %d, got %d", costFeedbackWindow, got)
	}
}

func TestPercentile95(t *testing.T) {
	if got := percentile95(nil); got != 0 {
		t.Errorf("empty input should be 0, got %v", got)
	}
	if got := percentile95([]float64{5}); got != 5 {
		t.Errorf("single element should be itself, got %v", got)
	}
	vals := make([]float64, 100)
	for i := range vals {
		vals[i] = float64(i + 1) // 1..100
	}
	if got := percentile95(vals); got != 95 {
		t.Errorf("p95 of 1..100 should be 95, got %v", got)
	}
}
