package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestKdbServerRuntimeCommitConcurrentSameDocumentConflict is the same-document half of
// component 38 spec §7 test 3 ("concurrent commits from multiple connections, same document -
// real conflict detection under real concurrency"). The wire protocol can't yet construct this
// scenario end to end (INSERT always mints a fresh document id; there's no UPDATE/explicit-docID
// write path parsed yet - see TestListenSqlWireConcurrentCommitsChainCleanly for the
// wire-reachable disjoint-document half), so this drives KdbServerRuntime.Commit directly with
// several goroutines racing a write to the exact same document id, all anchored on the same
// (now-stale-for-all-but-one) base version - the realistic shape of several clients that read
// the same snapshot before racing to update it.
func TestKdbServerRuntimeCommitConcurrentSameDocumentConflict(t *testing.T) {
	rt := newTestRuntime(t)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	base, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	baseTx := document.Transaction{
		ID:          mustRandomUUID(t),
		BaseVersion: base,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"base"}`}},
		Timestamp:   codec.TimestampNow(),
	}
	baseCommit, err := rt.Commit("app/data", baseTx, "setup", auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}

	const racers = 8
	type outcome struct {
		commit document.Commit
		err    error
	}
	outcomes := make([]outcome, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := document.Transaction{
				ID:          mustRandomUUID(t),
				BaseVersion: baseCommit.Hash,
				Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: fmt.Sprintf(`{"v":"racer-%d"}`, i)}},
				Timestamp:   codec.TimestampNow(),
			}
			commit, err := rt.Commit("app/data", tx, fmt.Sprintf("racer-%d", i), auth.Principal{})
			outcomes[i] = outcome{commit: commit, err: err}
		}(i)
	}
	wg.Wait()

	successes, conflicts := 0, 0
	for i, o := range outcomes {
		switch {
		case o.err == nil:
			successes++
		default:
			var conflictErr *ConflictError
			if !asError(o.err, &conflictErr) {
				t.Fatalf("racer %d: expected success or *ConflictError, got %T: %v", i, o.err, o.err)
			}
			conflicts++
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d, want exactly 1 (optimistic concurrency should admit only the first writer)", successes)
	}
	if conflicts != racers-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts, racers-1)
	}

	// The DAG must not have forked: head must be reachable and equal to the one successful
	// commit's hash, and the document's final content must be exactly that commit's write -
	// not corrupted, not overwritten by a "conflicting" write that should have been rejected.
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	headCommit, ok := rt.Runtime.DAG.GetCommit(head)
	if !ok {
		t.Fatalf("head %s not present in DAG", head.Hex())
	}
	doc, err := rt.Runtime.Storage.GetDocumentOrThrow("app/data", docID, headCommit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if doc.JSON == `{"v":"base"}` {
		t.Fatal("expected the winning racer's write to have landed, but doc is still the pre-race base value")
	}
}

func mustRandomUUID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
