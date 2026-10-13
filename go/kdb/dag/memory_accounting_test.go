package dag

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// GraphResidentBytes follows what the resident commit graph actually adds to the heap.
func TestGraphResidentBytesTracksHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	const n = 100_000
	txs := make([]document.Transaction, n)
	msgs := make([]string, n)
	for i := range txs {
		id, _ := codec.RandomUUID()
		txs[i] = document.Transaction{ID: id, Timestamp: codec.TimestampFromEpochMicros(1_700_000_000_000_000 + int64(i)*1000)}
		msgs[i] = fmt.Sprintf("commit number %08d", i)
	}
	heapNow := func() int64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int64(m.HeapAlloc)
	}
	before, reportedBefore := heapNow(), d.GraphResidentBytes()
	for i := range txs {
		txs[i].BaseVersion = parent
		c, err := d.AppendCommitDetached(txs[i], parent, document.EmptyDocumentTree(), nil, msgs[i])
		if err != nil {
			t.Fatal(err)
		}
		parent = c.Hash
	}
	heap := heapNow() - before
	runtime.KeepAlive(txs)
	runtime.KeepAlive(msgs)
	reported := d.GraphResidentBytes() - reportedBefore
	t.Logf("%d commits: heap %.1f MB, reported %.1f MB (%.2fx), %.0f B/commit",
		n, float64(heap)/1e6, float64(reported)/1e6, float64(reported)/float64(heap), float64(heap)/n)
	if r := float64(reported) / float64(heap); r < 0.6 || r > 1.4 {
		t.Fatalf("GraphResidentBytes is %.2fx the heap the graph occupies", r)
	}
}
