package dag

import (
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
