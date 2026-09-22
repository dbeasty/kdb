package peersync

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// writeAtHead commits one document write on s's current head, advancing it.
func writeAtHead(t *testing.T, s side, ns string, docID codec.UUID, json string) document.Commit {
	t.Helper()
	head, _ := s.dag.Head()
	c := writeDoc(t, s, ns, head, docID, json)
	setHead(t, s, c.Hash)
	return c
}

// pullInto is one direction of a sync, done in-process: dst takes everything src has that it
// lacks and decides its head, exactly as the wire client does.
func pullInto(t *testing.T, ns string, dst, src side, policy transaction.ConflictPolicy) IngestResult {
	t.Helper()
	srcHead, _ := src.dag.Head()
	commits, stubs, err := MissingCommits(src.dag, srcHead, spreadAncestorsAll(dst), math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	env := IngestEnv{DAG: dst.dag, Storage: dst.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution: ResolutionOptions{Policy: policy}}
	if _, err := StoreCommits(env, commits, stubs); err != nil {
		t.Fatal(err)
	}
	res, err := Adopt(env, srcHead)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func spreadAncestorsAll(s side) []codec.Hash {
	head, _ := s.dag.Head()
	return spreadAncestors(s.dag, head)
}

// TestMergeCommitIsIdenticalOnBothSides is D4: two nodes resolving the same divergence at the
// same time used to each build their own merge commit - random transaction id, random author,
// their own clock, local-first parents - and then had a new divergence to resolve between the
// two merges, forever. Now both build the same commit.
func TestMergeCommitIsIdenticalOnBothSides(t *testing.T) {
	ns := "app/merge-identical"
	a, b := forkTwoSides(t, ns)
	shared := newUUID(t)
	writeAtHead(t, a, ns, newUUID(t), `{"only":"a"}`)
	writeAtHead(t, a, ns, shared, `{"v":"a"}`)
	writeAtHead(t, b, ns, newUUID(t), `{"only":"b"}`)
	writeAtHead(t, b, ns, shared, `{"v":"b"}`)

	// Both fetch before either merges - the simultaneous case.
	aHead, _ := a.dag.Head()
	bHead, _ := b.dag.Head()
	toA, _, _ := MissingCommits(b.dag, bHead, spreadAncestorsAll(a), math.MaxInt)
	toB, _, _ := MissingCommits(a.dag, aHead, spreadAncestorsAll(b), math.MaxInt)
	lww := ResolutionOptions{Policy: transaction.ConflictPolicyLastWrite}
	ra, err := Ingest(IngestEnv{DAG: a.dag, Storage: a.storage, NamespaceID: ns, ApplyToStorage: true, Resolution: lww}, toA, bHead)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := Ingest(IngestEnv{DAG: b.dag, Storage: b.storage, NamespaceID: ns, ApplyToStorage: true, Resolution: lww}, toB, aHead)
	if err != nil {
		t.Fatal(err)
	}
	if ra.Outcome.Kind != OutcomeMerged || rb.Outcome.Kind != OutcomeMerged {
		t.Fatalf("expected both to merge, got %v and %v", ra.Outcome.Kind, rb.Outcome.Kind)
	}
	if ra.Head != rb.Head {
		t.Fatalf("the two nodes built different merge commits: %s vs %s", ra.Head.Hex(), rb.Head.Hex())
	}
	m := *ra.Outcome.MergeCommit
	if m.AuthorNodeID != MergeAuthorNodeID || m.Message != mergeMessage {
		t.Fatalf("merge commit carries node-specific fields: author=%s message=%q", m.AuthorNodeID, m.Message)
	}
}

// TestMergeReplaysFromEitherParent: a third node at either parent fast-forwards to the merge and
// the tree it builds is the one the merge declares.
func TestMergeReplaysFromEitherParent(t *testing.T) {
	ns := "app/merge-replay"
	a, b := forkTwoSides(t, ns)
	shared := newUUID(t)
	writeAtHead(t, a, ns, shared, `{"v":"a"}`)
	writeAtHead(t, a, ns, newUUID(t), `{"only":"a"}`)
	writeAtHead(t, b, ns, shared, `{"v":"b"}`)
	aTip, _ := a.dag.Head()
	bTip, _ := b.dag.Head()

	// Two observers, one following each side before the merge.
	atA, atB := newSide(t, ns), newSide(t, ns)
	pullInto(t, ns, atA, a, transaction.ConflictPolicyLastWrite)
	pullInto(t, ns, atB, b, transaction.ConflictPolicyLastWrite)

	res := pullInto(t, ns, a, b, transaction.ConflictPolicyLastWrite)
	if res.Outcome.Kind != OutcomeMerged {
		t.Fatalf("expected a merge, got %v", res.Outcome.Kind)
	}
	for name, obs := range map[string]side{"observer of a": atA, "observer of b": atB} {
		r := pullInto(t, ns, obs, a, transaction.ConflictPolicyLastWrite)
		if r.Outcome.Kind != OutcomeFastForwarded || r.Head != res.Head {
			t.Fatalf("%s: expected fast-forward to the merge, got %v at %s", name, r.Outcome.Kind, r.Head.Hex())
		}
	}
	_ = aTip
	_ = bTip
}

// TestMeshConvergesDeterministically is Phase 1's exit property: three nodes take random writes
// to a small set of shared documents and exchange in random pairs - both directions computed
// from the same moment, the case that never settled with random merges - then exchange until
// quiet. Every seed must end with one head, one tree, on every node.
func TestMeshConvergesDeterministically(t *testing.T) {
	seeds := 20
	if v, err := strconv.Atoi(os.Getenv("TEST_SEEDS")); err == nil && v > 0 {
		seeds = v
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { meshRun(t, int64(seed)) })
	}
}

func meshRun(t *testing.T, seed int64) {
	ns := fmt.Sprintf("app/mesh-%d", seed)
	rng := rand.New(rand.NewSource(seed))
	nodes := []side{newSide(t, ns), newSide(t, ns), newSide(t, ns)}
	docs := make([]codec.UUID, 6)
	for i := range docs {
		docs[i] = newUUID(t)
	}
	exchange := func(i, j int) {
		// Both directions planned from the same state, then applied - concurrent syncs.
		hi, _ := nodes[i].dag.Head()
		hj, _ := nodes[j].dag.Head()
		toI, _, _ := MissingCommits(nodes[j].dag, hj, spreadAncestorsAll(nodes[i]), math.MaxInt)
		toJ, _, _ := MissingCommits(nodes[i].dag, hi, spreadAncestorsAll(nodes[j]), math.MaxInt)
		lww := ResolutionOptions{Policy: transaction.ConflictPolicyLastWrite}
		if _, err := Ingest(IngestEnv{DAG: nodes[i].dag, Storage: nodes[i].storage, NamespaceID: ns, ApplyToStorage: true, Resolution: lww}, toI, hj); err != nil {
			t.Fatalf("seed %d: ingest into %d: %v", seed, i, err)
		}
		if _, err := Ingest(IngestEnv{DAG: nodes[j].dag, Storage: nodes[j].storage, NamespaceID: ns, ApplyToStorage: true, Resolution: lww}, toJ, hi); err != nil {
			t.Fatalf("seed %d: ingest into %d: %v", seed, j, err)
		}
	}
	for step := 0; step < 120; step++ {
		if rng.Intn(3) == 0 {
			i := rng.Intn(3)
			j := (i + 1 + rng.Intn(2)) % 3
			exchange(i, j)
			continue
		}
		n := rng.Intn(3)
		writeAtHead(t, nodes[n], ns, docs[rng.Intn(len(docs))], fmt.Sprintf(`{"node":%d,"step":%d}`, n, step))
	}
	for round := 0; ; round++ {
		if round > 10 {
			t.Fatalf("seed %d: no convergence after %d all-pairs rounds", seed, round)
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
	h := heads(nodes)
	c, _ := nodes[0].dag.GetCommitOrThrow(h[0])
	for i, n := range nodes {
		for _, id := range docs {
			want, _ := nodes[0].storage.GetDocument(ns, id, c.DocumentTreeHash)
			got, _ := n.storage.GetDocument(ns, id, c.DocumentTreeHash)
			if (want == nil) != (got == nil) || (want != nil && want.JSON != got.JSON) {
				t.Fatalf("seed %d: node %d disagrees on document %s", seed, i, id)
			}
		}
	}
}

func heads(nodes []side) [3]codec.Hash {
	var out [3]codec.Hash
	for i, n := range nodes {
		out[i], _ = n.dag.Head()
	}
	return out
}
