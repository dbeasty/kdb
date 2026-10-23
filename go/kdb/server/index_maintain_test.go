package server

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// The stores' bookkeeping (RegistryIndexProvider.maintain) runs after the write gate is released,
// and a commit whose pass finds another running skips its own. Nothing a read sees may depend on
// it: with no pass run at all for hundreds of commits, index reads at the head - the composite
// walk a queue claim takes included - and at an older commit still answer exactly.
func TestIndexReadsAreExactWithoutMaintenance(t *testing.T) {
	p := newCompositePair(t, queueDocs(rand.New(rand.NewSource(5)), 300))
	provider := p.withRuntime.SQLIndexProvider().(*RegistryIndexProvider)
	provider.maintainMu.Lock() // every commit from here finds a pass "running" and skips its own
	defer provider.maintainMu.Unlock()

	claim := "UPDATE t SET status = 'running' WHERE _c = 'jobs' AND status = 'pending' ORDER BY updatedAt, kdb_id LIMIT 1 RETURNING kdb_id"
	pending := "SELECT kdb_id, updatedAt FROM t WHERE _c = 'jobs' AND status = 'pending' ORDER BY updatedAt DESC, kdb_id LIMIT 20"
	var pinned, pinnedAnswer string
	for i := 0; i < 120; i++ {
		got, want := p.rows(p.with, p.ws, claim), p.rows(p.bare, p.bs, claim)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("claim %d: %v with the composite index, %v without", i, got, want)
		}
		if i == 40 {
			r := p.with.sqlExec(t, "app/data", p.ws, "SELECT COUNT(*) FROM t")
			pinned = r.ResolvedCommitHex
			pinnedAnswer = strings.Join(p.rows(p.with, p.ws, pending)[0], ",")
		}
	}
	if plan := p.same(pending); !strings.Contains(plan, "index:c_status_updated") {
		t.Fatalf("pending jobs read by %q, not the composite index", plan)
	}
	p.same("SELECT COUNT(*) FROM t WHERE status = 'running'")

	// A snapshot read at a commit 80 claims back is answered as it was then.
	old := p.with.sessionBeginAt(t, "app/data", "SNAPSHOT", pinned).SessionID
	if got := strings.Join(p.rows(p.with, old, pending)[0], ","); got != pinnedAnswer {
		t.Fatalf("at the pinned commit: first pending row %s, it was %s", got, pinnedAnswer)
	}
}
