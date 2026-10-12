package peersync

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestV2MeshConvergesOnTheLastWriterWinsMap is TestMeshConvergesDeterministically under a version
// 2 [field-merge, last-write] chain: three nodes patch random fields of shared documents and sync
// in random pairs, both directions planned from the same moment. Every seed must end with one
// head on every node, and every document at the last-writer-wins map of the writes made to it.
func TestV2MeshConvergesOnTheLastWriterWinsMap(t *testing.T) {
	for seed := int64(1); seed <= 15; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { meshRunV2(t, seed) })
	}
}

func meshRunV2(t *testing.T, seed int64) {
	ns := fmt.Sprintf("app/mesh-v2-%d", seed)
	rng := rand.New(rand.NewSource(seed))
	opts := chainV(ChainVersionMergeBases, fieldMergeLastWrite...)
	nodes := []side{newSide(t, ns), newSide(t, ns), newSide(t, ns)}
	docs := []codec.UUID{newUUID(t), newUUID(t)}
	type write struct {
		commit codec.Hash
		doc    codec.UUID
		fields map[string]any
	}
	var writes []write
	exchange := func(i, j int) {
		hi, _ := nodes[i].dag.Head()
		hj, _ := nodes[j].dag.Head()
		toI, _, _ := MissingCommits(nodes[j].dag, hj, spreadAncestorsAll(nodes[i]), 1<<30)
		toJ, _, _ := MissingCommits(nodes[i].dag, hi, spreadAncestorsAll(nodes[j]), 1<<30)
		for _, x := range []struct {
			n  int
			cs []document.Commit
			h  codec.Hash
		}{{i, toI, hj}, {j, toJ, hi}} {
			env := IngestEnv{DAG: nodes[x.n].dag, Storage: nodes[x.n].storage, NamespaceID: ns, ApplyToStorage: true, Resolution: opts}
			res, err := Ingest(env, x.cs, x.h)
			if err != nil {
				t.Fatalf("ingest into %d: %v", x.n, err)
			}
			if res.Outcome.Kind == OutcomeConflict {
				t.Fatalf("a [field-merge, last-write] chain left a conflict: %+v", res.Outcome.Details)
			}
		}
	}
	read := func(s side, at codec.Hash, id codec.UUID) map[string]any {
		c, _ := s.dag.GetCommitOrThrow(at)
		d, _ := s.storage.GetDocument(ns, id, c.DocumentTreeHash)
		if d == nil {
			return nil
		}
		var m map[string]any
		_ = json.Unmarshal([]byte(d.JSON), &m)
		return m
	}
	for step := 0; step < 120; step++ {
		if rng.Intn(3) == 0 {
			i := rng.Intn(3)
			exchange(i, (i+1+rng.Intn(2))%3)
			continue
		}
		n := rng.Intn(3)
		id := docs[rng.Intn(len(docs))]
		head, _ := nodes[n].dag.Head()
		cur := read(nodes[n], head, id)
		if cur == nil {
			cur = map[string]any{}
		}
		set := map[string]any{}
		for _, f := range []string{"a", "b", "c"} {
			if rng.Intn(2) == 0 {
				set[f] = fmt.Sprintf("n%d-%d", n, step)
				cur[f] = set[f]
			}
		}
		if len(set) == 0 {
			continue
		}
		b, _ := json.Marshal(cur)
		c := writeAtHead(t, nodes[n], ns, id, string(b))
		writes = append(writes, write{commit: c.Hash, doc: id, fields: set})
	}
	for round := 0; ; round++ {
		if round > 10 {
			t.Fatalf("no convergence after %d all-pairs rounds", round)
		}
		before := heads(nodes)
		exchange(0, 1)
		exchange(1, 2)
		exchange(0, 2)
		after := heads(nodes)
		if after[0] == after[1] && after[1] == after[2] && before == after {
			break
		}
	}
	head := heads(nodes)[0]
	for _, id := range docs {
		latest := map[string]write{}
		for _, w := range writes {
			if w.doc != id {
				continue
			}
			for f := range w.fields {
				if o, ok := latest[f]; !ok || originLater(nodes[0].dag, w.commit, o.commit) {
					latest[f] = w
				}
			}
		}
		want := map[string]any{}
		for f, w := range latest {
			want[f] = w.fields[f]
		}
		got := read(nodes[0], head, id)
		if len(want) == 0 && got == nil {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("document %s converged on %v, want the last-writer-wins map %v", id, got, want)
		}
	}
}
