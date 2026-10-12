package dag

import (
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestGraphFileScale is the measurement behind docs/kdb-mmap-plan.md's results, not a
// correctness test: it builds a linear history of KDB_GRAPH_SCALE commits (one merge every 1000)
// and reports heap, GC mark time and read latencies with every commit resident, then again with
// the graph file attached. Skipped unless the variable is set, because a million-commit history
// takes a while and a lot of memory.
//
//	KDB_GRAPH_SCALE=1000000 go test ./kdb/dag -run TestGraphFileScale -v -timeout 30m
func TestGraphFileScale(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("KDB_GRAPH_SCALE"))
	if n <= 0 {
		t.Skip("set KDB_GRAPH_SCALE to run")
	}
	skipWithoutMmap(t)

	baseline := liveHeap()
	d, _ := NewInMemoryCommitDag("ns")
	d.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	path := filepath.Join(t.TempDir(), "graph")
	d.ConfigureGraphFile(path, false)
	root, _ := d.Head()
	hashes := make([]codec.Hash, 0, n)
	hashes = append(hashes, root)
	parent := root
	var side codec.Hash
	var ub [16]byte
	start := time.Now()
	for i := 1; i < n; i++ {
		ub[0], ub[1], ub[2], ub[3] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24)
		tx, _ := codec.UUIDFromBytes(ub[:])
		parents := []codec.Hash{parent}
		if i%1000 == 999 && side != (codec.Hash{}) {
			parents = append(parents, side) // a merge, so ancestry is not a straight line
		}
		c, err := document.BuildCommit(parents, "ns", tx, codec.Timestamp{EpochMillis: int64(i)}, tx, nil, codec.Hash{}, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 500 {
			side = c.Hash
		}
		parent = c.Hash
		hashes = append(hashes, c.Hash)
	}
	t.Logf("built %d commits in %v", n, time.Since(start).Round(time.Millisecond))

	report := func(label string) {
		heap := liveHeap() - baseline - int64(cap(hashes))*32
		gc := gcMark()
		rng := rand.New(rand.NewSource(1))
		const lookups = 200_000
		s := time.Now()
		for i := 0; i < lookups; i++ {
			if _, ok := d.GetCommit(hashes[rng.Intn(len(hashes))]); !ok {
				t.Fatal("lookup missed")
			}
		}
		get := time.Since(s) / lookups
		s = time.Now()
		const anc = 200
		for i := 0; i < anc; i++ {
			if !d.IsAncestor(hashes[rng.Intn(len(hashes)/2)], parent) {
				t.Fatal("ancestry wrong")
			}
		}
		isAnc := time.Since(s) / anc
		s = time.Now()
		walked := len(d.Walk(parent, nil, 10_000))
		walk := time.Since(s)
		tx, _ := d.GetCommit(hashes[len(hashes)/3])
		s = time.Now()
		for i := 0; i < lookups; i++ {
			d.GetCommitByTransactionID(tx.TransactionID)
		}
		byTx := time.Since(s) / lookups
		t.Logf("%-9s heap=%5.0fMB (%3.0f B/commit) gcMark=%-9v GetCommit=%-7v IsAncestor(deep)=%-9v Walk(%d)=%-9v byTx=%v",
			label, float64(heap)/1e6, float64(heap)/float64(n), gc.Round(time.Microsecond), get, isAnc.Round(time.Microsecond), walked, walk.Round(time.Microsecond), byTx)
	}

	report("resident")

	s := time.Now()
	if err := d.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	rebuild := time.Since(s)
	size := d.GraphFileStats().FileBytes
	t.Logf("rebuild (write + attach) %v, file %.0fMB (%.0f B/commit)", rebuild.Round(time.Millisecond), float64(size)/1e6, float64(size)/float64(n))
	report("file")

	// Reattach cost, as an open pays it: map, checksum, and move the resident tier out.
	d.CloseGraphFile()
	d2, _ := NewInMemoryCommitDag("ns")
	d2.SetGraphSettings(GraphSettings{FileEnabled: true})
	d2.ConfigureGraphFile(path, false)
	s = time.Now()
	if err := d2.OpenGraphFile(); err != nil {
		t.Fatal(err)
	}
	t.Logf("attach to an empty DAG (map + checksum + hide) %v", time.Since(s).Round(time.Millisecond))
	d2.CloseGraphFile()
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

func gcMark() time.Duration {
	var total time.Duration
	const reps = 5
	for i := 0; i < reps; i++ {
		s := time.Now()
		runtime.GC()
		total += time.Since(s)
	}
	return total / reps
}
