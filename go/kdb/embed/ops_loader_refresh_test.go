package embed_test

import (
	"testing"
)

// TestEvictedOperationsLoggedAfterFirstColdLoadStayReadable: the operations loader indexed the
// delta log on its first cold load and never again. Every commit logged after that whose
// operations the budget then evicted was reported as having "no frame in the delta log" - the
// same error a commit genuinely missing from the log produces - although its frame was right
// there. Found while chasing the multi-node e2e drain failure, which reports that error too.
func TestEvictedOperationsLoggedAfterFirstColdLoadStayReadable(t *testing.T) {
	rt := smallBudgetRuntime(t, t.TempDir())
	defer rt.Close()
	d, ok := underlyingDag(rt)
	if !ok {
		t.Skip("no in-memory DAG underneath")
	}
	first, _ := writeVersions(t, rt, 60)
	if !d.OperationsEvicted(first[0]) {
		t.Fatal("precondition: the oldest commit's operations are expected to be evicted")
	}
	if _, err := d.CommitOperations(first[0]); err != nil {
		t.Fatalf("first cold load: %v", err)
	}
	later, _ := writeVersions(t, rt, 60)
	evicted := 0
	for i, h := range later {
		if !d.OperationsEvicted(h) {
			continue
		}
		evicted++
		if _, err := d.CommitOperations(h); err != nil {
			t.Fatalf("commit %d, logged after the first cold load: %v", i, err)
		}
	}
	if evicted == 0 {
		t.Fatal("precondition: some later commit's operations are expected to be evicted")
	}
}
