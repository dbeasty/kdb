package peersync

import (
	"errors"
	"math"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/wire"
)

// graftFixture is the case Phase 11 exists for: A was bootstrapped from B's snapshot and B is
// gone, so nobody holds the history below A's root; C has a history of its own. Both wrote since,
// including the same document x.
type graftFixture struct {
	ns         string
	a, c       *testNamespaces
	root       codec.Hash
	x, onlyA   codec.UUID
	onlyC      codec.UUID
	fromB      int
	aPre, cPre codec.Hash
}

func unrelatedChain() *ResolutionOptions {
	return &ResolutionOptions{
		Policy: transaction.ConflictPolicyStrict,
		Chain:  &ResolutionChain{AllowUnrelated: true, Rules: []ResolutionRule{{Kind: RuleLastWrite}}},
	}
}

func newGraftFixture(t *testing.T, hubPrefix string, res *ResolutionOptions) graftFixture {
	t.Helper()
	f := graftFixture{ns: "app/graft", x: newUUID(t), onlyA: newUUID(t), onlyC: newUUID(t)}
	b := newTestNamespaces(t, f.ns)
	seedHistory(t, b, f.ns, 12)
	writeAtHead(t, b.side(f.ns), f.ns, f.x, `{"v":"b"}`)
	v2Hub(t, hubPrefix+"-b", newHost(b), nil)

	f.a = newTestNamespaces(t, f.ns)
	f.a.resolution = res
	boot := syncV2(t, hubPrefix+"-b", f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull, PreferSnapshot: true})
	f.root, _ = codec.HashFromHex(boot.Namespaces[0].Snapshot)
	f.a.side(f.ns).storage.(interface {
		WalkTree(string, codec.Hash, func(codec.UUID, codec.Hash) bool) error
	}).WalkTree(f.ns, mustCommit(t, f.a.side(f.ns), f.root).DocumentTreeHash, func(codec.UUID, codec.Hash) bool { f.fromB++; return true })

	writeAtHead(t, f.a.side(f.ns), f.ns, f.onlyA, `{"on":"a"}`)
	writeAtHead(t, f.a.side(f.ns), f.ns, f.x, `{"v":"a"}`)

	f.c = newTestNamespaces(t, f.ns)
	f.c.resolution = res
	writeAtHead(t, f.c.side(f.ns), f.ns, f.onlyC, `{"on":"c"}`)
	writeAtHead(t, f.c.side(f.ns), f.ns, f.x, `{"v":"c"}`)
	f.aPre, f.cPre = mustHead(t, f.a.side(f.ns)), mustHead(t, f.c.side(f.ns))
	v2Hub(t, hubPrefix+"-a", newHost(f.a), nil)
	v2Hub(t, hubPrefix+"-c", newHost(f.c), nil)
	return f
}

func mustCommit(t *testing.T, s side, h codec.Hash) document.Commit {
	t.Helper()
	c, err := s.dag.GetCommitOrThrow(h)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// liveDocs is every document at s's head, id -> body.
func liveDocs(t *testing.T, s side, ns string) map[string]string {
	t.Helper()
	head := mustHead(t, s)
	tree := mustCommit(t, s, head).DocumentTreeHash
	out := map[string]string{}
	var ids []codec.UUID
	if err := s.storage.(storage.TreeWalker).WalkTree(ns, tree, func(id codec.UUID, _ codec.Hash) bool { ids = append(ids, id); return true }); err != nil {
		t.Fatal(err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	docs, err := s.storage.GetDocuments(ns, ids, tree)
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range docs {
		if d == nil {
			t.Fatalf("document %s at head is unreadable", ids[i])
		}
		out[d.ID.String()] = d.JSON
	}
	return out
}

func requireConverged(t *testing.T, f graftFixture) {
	t.Helper()
	a, c := f.a.side(f.ns), f.c.side(f.ns)
	if mustHead(t, a) != mustHead(t, c) {
		t.Fatalf("A and C did not converge: %s vs %s", mustHead(t, a).Hex(), mustHead(t, c).Hex())
	}
	da, dc := liveDocs(t, a, f.ns), liveDocs(t, c, f.ns)
	if len(da) != len(dc) {
		t.Fatalf("A holds %d documents, C %d", len(da), len(dc))
	}
	for id, body := range da {
		if dc[id] != body {
			t.Fatalf("document %s: A has %s, C has %s", id, body, dc[id])
		}
	}
	// Everything from B's snapshot, both sides' own documents, and one value of x.
	if want := f.fromB + 2; len(da) != want {
		t.Fatalf("merged namespace holds %d documents, want %d (B's %d + onlyA + onlyC)", len(da), want, f.fromB)
	}
	if da[f.onlyA.String()] != `{"on":"a"}` || da[f.onlyC.String()] != `{"on":"c"}` {
		t.Fatalf("a one-sided document was not adopted: %v", da)
	}
	if x := da[f.x.String()]; x != `{"v":"a"}` && x != `{"v":"c"}` {
		t.Fatalf("x resolved to %s, which neither side wrote", x)
	}
}

// TestGraftMergesAPeerRootedInASnapshotWhoseSourceIsGone: C syncs with A. C grafts A's root,
// merges the two unrelated histories, and hands A the merge; both end on the same head and tree.
func TestGraftMergesAPeerRootedInASnapshotWhoseSourceIsGone(t *testing.T) {
	f := newGraftFixture(t, "graft-c-first", unrelatedChain())
	res := syncV2(t, "graft-c-first-a", f.c, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncBoth})
	ns := res.Namespaces[0]
	if len(ns.Grafted) != 1 || ns.Grafted[0] != f.root.Hex() {
		t.Fatalf("C should have grafted A's root %s, grafted %v", f.root.Hex(), ns.Grafted)
	}
	if !f.c.side(f.ns).dag.IsShallow(f.root) {
		t.Fatal("the grafted root is not a shallow root on C")
	}
	if ns.Local["branch:main"] != wire.RefMerged {
		t.Fatalf("C's main should have merged, got %v", ns.Local["branch:main"])
	}
	requireConverged(t, f)
}

// TestGraftFromTheOtherSide: A pulls first and merges C's history into its own without any
// graft (it holds everything C's history needs); C then grafts A's root to take the merge.
func TestGraftFromTheOtherSide(t *testing.T) {
	f := newGraftFixture(t, "graft-a-first", unrelatedChain())
	resA := syncV2(t, "graft-a-first-c", f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull})
	if len(resA.Namespaces[0].Grafted) != 0 {
		t.Fatalf("A holds C's whole history and should not graft, grafted %v", resA.Namespaces[0].Grafted)
	}
	resC := syncV2(t, "graft-a-first-a", f.c, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull})
	if got := resC.Namespaces[0]; len(got.Grafted) != 1 || got.Local["branch:main"] != wire.RefFastForwarded {
		t.Fatalf("C should graft A's root and fast-forward to A's merge: %+v", got)
	}
	requireConverged(t, f)
}

// TestUnrelatedMergeIsTheSameOnBothNodes: each node merges the two pre-merge heads on its own -
// A holding C's history, C holding A's through the graft - and they build the identical commit.
func TestUnrelatedMergeIsTheSameOnBothNodes(t *testing.T) {
	f := newGraftFixture(t, "graft-same", unrelatedChain())
	resC := syncV2(t, "graft-same-a", f.c, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull})
	mergeOnC := mustHead(t, f.c.side(f.ns))
	if resC.Namespaces[0].Local["branch:main"] != wire.RefMerged {
		t.Fatalf("C did not merge: %+v", resC.Namespaces[0])
	}

	a, c := f.a.side(f.ns), f.c.side(f.ns)
	commits, stubs, err := MissingCommits(c.dag, f.cPre, spreadAncestorsAll(a), math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	env, _ := f.a.Env(f.ns, false)
	if _, err := StoreCommits(env, commits, stubs); err != nil {
		t.Fatal(err)
	}
	out, err := ResolveDivergence(a.dag, a.storage, f.ns, f.aPre, f.cPre, *unrelatedChain())
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != OutcomeMerged || out.MergeCommit.Hash != mergeOnC {
		t.Fatalf("A merged to %v (%v), C to %s", out.MergeCommit, out.Kind, mergeOnC.Hex())
	}
}

// TestUnrelatedHistoryRefusedWithoutAllowUnrelated: the default stays a typed refusal, and
// nothing is grafted.
func TestUnrelatedHistoryRefusedWithoutAllowUnrelated(t *testing.T) {
	f := newGraftFixture(t, "graft-refused", nil)
	cfg := V2ClientConfig{PeerURI: "memory://graft-refused-a", Local: f.c, NodeID: "c", Namespaces: []string{f.ns}, Mode: SyncPull}
	res, err := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var unrelated *UnrelatedHistoryError
	if !errors.As(res.Namespaces[0].Err, &unrelated) || unrelated.Root != f.root {
		t.Fatalf("want an UnrelatedHistoryError at %s, got %v", f.root.Hex(), res.Namespaces[0].Err)
	}
	if f.c.side(f.ns).dag.HasCommit(f.root) {
		t.Fatal("the root was stored without allowUnrelated")
	}
}

// TestGraftRejectsATamperedState: a root whose documents do not build its declared tree is not
// grafted, and the sync reports unrelated history rather than merging made-up state.
func TestGraftRejectsATamperedState(t *testing.T) {
	f := newGraftFixture(t, "graft-tamper", unrelatedChain())
	w := wire.NewCodec(wire.EncodingJSON)
	host := newHost(f.a)
	v2HubRaw(t, "graft-tamper-raw", func(frame []byte) []byte {
		out, _ := host.HandleFrame(frame)
		msg, _ := w.Decode(out)
		if page, ok := msg.(wire.SnapshotPageMessage); ok && len(page.Docs) > 0 {
			page.Docs[0].Body = `{"tampered":true}`
			out, _ = w.Encode(page)
		}
		return out
	})
	cfg := V2ClientConfig{PeerURI: "memory://graft-tamper-raw", Local: f.c, NodeID: "c", Namespaces: []string{f.ns}, Mode: SyncPull}
	res, err := SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var unrelated *UnrelatedHistoryError
	if !errors.As(res.Namespaces[0].Err, &unrelated) {
		t.Fatalf("want an UnrelatedHistoryError, got %v", res.Namespaces[0].Err)
	}
	if f.c.side(f.ns).dag.HasCommit(f.root) || mustHead(t, f.c.side(f.ns)) != f.cPre {
		t.Fatal("a tampered graft changed C")
	}
}

// TestPushWithoutAGraftIsSkippedNotFailed: A pushes to C, which holds nothing below A's root, and
// the namespace does not merge unrelated histories. The push of main is skipped with a reason
// instead of failing the whole sync on the peer's missing-parent error.
func TestPushWithoutAGraftIsSkippedNotFailed(t *testing.T) {
	f := newGraftFixture(t, "graft-push-skip", nil)
	res := syncV2(t, "graft-push-skip-c", f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPush})
	reason := res.Namespaces[0].RefErrors["branch:main"]
	if reason == "" || mustHead(t, f.c.side(f.ns)) != f.cPre {
		t.Fatalf("the push should be skipped with a reason, got %+v", res.Namespaces[0])
	}
}

// TestGraftPushThenPushStoresTheHistory: with allowUnrelated, the same push grafts A's root on C
// first; C then holds A's history and main is proposed normally.
func TestGraftPushThenPushStoresTheHistory(t *testing.T) {
	f := newGraftFixture(t, "graft-push", unrelatedChain())
	res := syncV2(t, "graft-push-c", f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPush})
	if len(res.Namespaces[0].RefErrors) != 0 {
		t.Fatalf("push: %+v", res.Namespaces[0])
	}
	c := f.c.side(f.ns)
	if !c.dag.IsShallow(f.root) || !c.dag.HasCommit(f.aPre) {
		t.Fatal("C should hold A's root as a graft and A's history above it")
	}
	// C merged A's proposal itself (an unrelated merge) - and a pull the other way converges.
	syncV2(t, "graft-push-c", f.a, V2ClientConfig{Namespaces: []string{f.ns}, Mode: SyncPull})
	requireConverged(t, f)
}
