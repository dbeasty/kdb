package dag

import (
	"errors"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestConcurrentReadsAndAppends exercises the RWMutex conversion (gap
// fix, docs/benchmarks/phases-1-6-summary.md): many concurrent readers
// (GetCommit/Walk/Head/ListBranches) running alongside serialized
// AppendCommit writers must never see a torn/partial state. Run with
// -race to catch any read/write method misclassified during the
// Lock->RLock conversion.
func TestConcurrentReadsAndAppends(t *testing.T) {
	d, err := NewInMemoryCommitDag("app/concurrency")
	if err != nil {
		t.Fatal(err)
	}

	const appends = 200
	const readers = 8

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers hammer every RLock-converted method concurrently with
	// writers advancing the branch.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				head, err := d.Head()
				if err != nil {
					t.Errorf("Head: %v", err)
					return
				}
				if !d.HasCommit(head) {
					t.Errorf("HasCommit(head=%s) = false", head.Hex())
					return
				}
				if _, ok := d.GetCommit(head); !ok {
					t.Errorf("GetCommit(head=%s) missing", head.Hex())
					return
				}
				_ = d.ListBranches()
				_ = d.Walk(head, nil, 16)
			}
		}()
	}

	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	empty := document.EmptyDocumentTree()
	for i := 0; i < appends; i++ {
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		tx := document.Transaction{
			ID: txID, BaseVersion: head, Operations: nil,
			Timestamp: codec.TimestampNow(), AuthorNodeID: author,
		}
		commit, err := d.AppendCommit(tx, head, empty, nil, "")
		if err != nil {
			t.Fatalf("AppendCommit #%d: %v", i, err)
		}
		head = commit.Hash
	}
	close(stop)
	wg.Wait()

	final, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	if final != head {
		t.Fatalf("final head %s != expected %s", final.Hex(), head.Hex())
	}
}

// TestAppendCommitRefusesAStaleHead is the regression test for the orphaned-commit hazard:
// AppendCommit used to advance the branch head unconditionally, so two writers that had both
// read the same head each produced a valid commit, but only the later one stayed reachable from
// "main". The earlier writer was told it succeeded. It must now lose the compare-and-swap
// instead, loudly.
func TestAppendCommitRefusesAStaleHead(t *testing.T) {
	d, err := NewInMemoryCommitDag("app/cas")
	if err != nil {
		t.Fatal(err)
	}
	stale, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	empty := document.EmptyDocumentTree()

	// Writer A wins the race.
	winner, err := d.AppendCommit(newTx(stale), stale, empty, nil, "winner")
	if err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Writer B planned against the same head and only now gets to append.
	_, err = d.AppendCommit(newTx(stale), stale, empty, nil, "loser")
	var conflict *HeadConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("append onto a stale head = %v, want *HeadConflictError", err)
	}
	if conflict.Expected != stale || conflict.Actual != winner.Hash {
		t.Fatalf("conflict reports %s -> %s, want %s -> %s",
			conflict.Expected.Hex(), conflict.Actual.Hex(), stale.Hex(), winner.Hash.Hex())
	}

	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != winner.Hash {
		t.Fatalf("head = %s, want the winner %s", head.Hex(), winner.Hash.Hex())
	}
}

// TestConcurrentAppendsProduceOneUnbrokenChain runs real concurrent writers against one DAG with
// no external serialization - the embedded-caller case the server's writeGate does not cover.
// Every append either lands on the tip or is refused; nothing is silently orphaned, so walking
// back from the final head must reach every commit that reported success.
func TestConcurrentAppendsProduceOneUnbrokenChain(t *testing.T) {
	d, err := NewInMemoryCommitDag("app/cas-race")
	if err != nil {
		t.Fatal(err)
	}
	empty := document.EmptyDocumentTree()

	const writers = 8
	const attempts = 40

	var mu sync.Mutex
	accepted := make(map[codec.Hash]struct{})

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < attempts; i++ {
				head, err := d.Head()
				if err != nil {
					t.Errorf("Head: %v", err)
					return
				}
				commit, err := d.AppendCommit(newTx(head), head, empty, nil, "")
				if err != nil {
					var conflict *HeadConflictError
					if !errors.As(err, &conflict) {
						t.Errorf("AppendCommit: %v", err)
						return
					}
					continue // lost the race - the only acceptable failure
				}
				mu.Lock()
				accepted[commit.Hash] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(accepted) == 0 {
		t.Fatal("no writer ever won; the test proves nothing")
	}
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	reachable := d.AncestorSet(head)
	for h := range accepted {
		if _, ok := reachable[h]; !ok {
			t.Fatalf("commit %s reported success but is not reachable from head %s (orphaned)",
				h.Hex(), head.Hex())
		}
	}
}
