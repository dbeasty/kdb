package server

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Whatever the change log answers names every document the trees differ in - it may name more
// (a write that changed nothing), never fewer - for any two commits of a history of writes,
// rewrites and deletes made through the commit path.
func TestChangeLogCoversTheTreeDiff(t *testing.T) {
	for name, rt := range immediateRuntimes(t) {
		t.Run(name, func(t *testing.T) {
			ns := rt.Runtime.DefaultNamespace
			r := rand.New(rand.NewSource(3))
			var heads []codec.Hash
			for i := 0; i < 200; i++ {
				id := immediateJobID(r.Intn(40))
				var err error
				if r.Intn(6) == 0 {
					_, _, err = rt.CommitImmediate(auth.Principal{}, "", func(codec.Hash) ([]document.Op, error) {
						return []document.Op{document.DeleteOp{DocID: id}}, nil
					})
				} else {
					_, err = rt.PutJSON(ns, id, fmt.Sprintf(`{"_c":"jobs","n":%d}`, r.Intn(3)), nil, auth.Principal{})
				}
				if err != nil {
					t.Fatal(err)
				}
				h, err := rt.Runtime.DAG.Head()
				if err != nil {
					t.Fatal(err)
				}
				heads = append(heads, h)
			}
			answered := 0
			for i := 0; i < 300; i++ {
				a, b := r.Intn(len(heads)), r.Intn(len(heads))
				if a >= b {
					continue
				}
				logged, ok := rt.changes.changedSince(heads[a], heads[b], maxImmediateDiff, maxImmediateDiff)
				if !ok {
					continue
				}
				answered++
				diffed, ok := rt.treeChangedSince(heads[a], heads[b], maxImmediateDiff)
				if !ok {
					t.Fatal("the trees could not be diffed")
				}
				for _, id := range diffed {
					if !slices.Contains(logged, id) {
						t.Fatalf("commits %d..%d: %s changed in the trees but not in the log %v", a, b, id, logged)
					}
				}
			}
			if answered == 0 {
				t.Fatal("the log answered nothing: every commit went through the commit path")
			}
		})
	}
}

// A walk that meets a commit the log does not hold - a merge, an operation that names no
// document, one too old - cannot answer, so the caller diffs the trees.
func TestChangeLogRefusesWhatItDidNotRecord(t *testing.T) {
	h := func(n byte) codec.Hash { return codec.Hash{Bytes: [32]byte{n}} }
	id := func(n byte) codec.UUID { return codec.UUID{LSB: int64(n)} }
	var l changeLog
	l.record(document.Commit{Hash: h(1), ParentHashes: []codec.Hash{h(0)}, Operations: []document.Op{document.WriteOp{DocID: id(1)}}})
	l.record(document.Commit{Hash: h(2), ParentHashes: []codec.Hash{h(1)}, Operations: []document.Op{document.DeleteOp{DocID: id(2)}, document.WriteOp{DocID: id(1)}}})
	if got, ok := l.changedSince(h(0), h(2), 10, 10); !ok || len(got) != 2 {
		t.Fatalf("a recorded chain: %v %v", got, ok)
	}
	if _, ok := l.changedSince(h(0), h(2), 1, 10); ok {
		t.Fatal("answered past maxCommits")
	}
	if _, ok := l.changedSince(h(0), h(2), 10, 1); ok {
		t.Fatal("answered past maxDocs")
	}
	l.record(document.Commit{Hash: h(3), ParentHashes: []codec.Hash{h(2), h(9)}, Operations: []document.Op{document.WriteOp{DocID: id(3)}}})
	l.record(document.Commit{Hash: h(4), ParentHashes: []codec.Hash{h(2)}, Operations: []document.Op{document.SchemaMigrationOp{}}})
	for _, to := range []codec.Hash{h(3), h(4)} {
		if _, ok := l.changedSince(h(0), to, 10, 10); ok {
			t.Fatalf("answered through unrecorded commit %v", to.Bytes[0])
		}
	}
	if _, ok := l.changedSince(h(7), h(2), 10, 10); ok {
		t.Fatal("answered from a commit that is not an ancestor")
	}
}
