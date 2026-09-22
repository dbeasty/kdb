package peersync

import (
	"math"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Regressions from review of the first deterministic-merge implementation, which decided what
// each side changed by replaying its operations from one chosen common ancestor.

func snapshotFor(t *testing.T, dst, src side) ([]document.Commit, []document.CommitStub, codec.Hash) {
	t.Helper()
	h, _ := src.dag.Head()
	c, s, err := MissingCommits(src.dag, h, spreadAncestorsAll(dst), math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	return c, s, h
}

func ingestWith(t *testing.T, ns string, dst side, c []document.Commit, s []document.CommitStub, h codec.Hash, pol transaction.ConflictPolicy) IngestResult {
	t.Helper()
	env := IngestEnv{DAG: dst.dag, Storage: dst.storage, NamespaceID: ns, ApplyToStorage: true, Resolution: ResolutionOptions{Policy: pol}}
	if _, err := StoreCommits(env, c, s); err != nil {
		t.Fatal(err)
	}
	r, err := Adopt(env, h)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestMergeEchoOfOwnWriteIsNotAChange: C writes d, B merges C's history (its merge restating d),
// C writes d again, then takes B's merge. C alone ever changed d, so there is nothing to
// resolve - no conflict under strict, and C's newer write survives under last-write even though
// B's clock runs ahead.
func TestMergeEchoOfOwnWriteIsNotAChange(t *testing.T) {
	for _, pol := range []transaction.ConflictPolicy{transaction.ConflictPolicyStrict, transaction.ConflictPolicyLastWrite} {
		for i := 0; i < 40; i++ {
			ns := "app/echo"
			b, c := forkTwoSides(t, ns)
			d := newUUID(t)
			now := codec.TimestampNow().EpochMicros()
			ch, _ := c.dag.Head()
			c1 := writeDocAt(t, c, ns, ch, d, `{"v":1}`, codec.TimestampFromEpochMicros(now))
			setHead(t, c, c1.Hash)
			bh, _ := b.dag.Head()
			b1 := writeDocAt(t, b, ns, bh, newUUID(t), `{"e":1}`, codec.TimestampFromEpochMicros(now+1000))
			setHead(t, b, b1.Hash)
			cc, cs, chh := snapshotFor(t, b, c)
			ingestWith(t, ns, b, cc, cs, chh, pol)
			ch2, _ := c.dag.Head()
			c2 := writeDocAt(t, c, ns, ch2, d, `{"v":2}`, codec.TimestampFromEpochMicros(now+10))
			setHead(t, c, c2.Hash)
			bc, bs, bhh := snapshotFor(t, c, b)
			r := ingestWith(t, ns, c, bc, bs, bhh, pol)
			if r.Outcome.Kind == OutcomeConflict {
				t.Fatalf("policy %v run %d: a document only C changed was reported as a conflict", pol, i)
			}
			_, hc, _, _ := c.dag.HeadCommit()
			doc, _ := c.storage.GetDocument(ns, d, hc.DocumentTreeHash)
			if doc == nil || doc.JSON != `{"v":2}` {
				t.Fatalf("policy %v run %d: C's newer write was lost to its own echo: %+v", pol, i, doc)
			}
		}
	}
}

// TestCrissCrossMergesAgreeWithoutConflict: A and B merge each other's history concurrently
// while writing on, producing two lowest common ancestors. Exchanging heads must neither report
// a conflict (nobody wrote the same document) nor produce two different merges.
func TestCrissCrossMergesAgreeWithoutConflict(t *testing.T) {
	for _, pol := range []transaction.ConflictPolicy{transaction.ConflictPolicyStrict, transaction.ConflictPolicyLastWrite} {
		for i := 0; i < 20; i++ {
			ns := "app/cc"
			a, b := forkTwoSides(t, ns)
			writeAtHead(t, a, ns, newUUID(t), `{"x":1}`)
			writeAtHead(t, b, ns, newUUID(t), `{"y":1}`)
			c, s, h := snapshotFor(t, a, b)
			ingestWith(t, ns, a, c, s, h, pol) // A: M1
			aAtM1c, aAtM1s, aAtM1h := snapshotFor(t, b, a)
			writeAtHead(t, b, ns, newUUID(t), `{"z":1}`) // b2
			bAtB2c, bAtB2s, bAtB2h := snapshotFor(t, a, b)
			writeAtHead(t, a, ns, newUUID(t), `{"w":1}`)      // a2
			ingestWith(t, ns, a, bAtB2c, bAtB2s, bAtB2h, pol) // A: M3 = (a2, b2)
			ingestWith(t, ns, b, aAtM1c, aAtM1s, aAtM1h, pol) // B: M2 = (b2, M1)
			c1, s1, h1 := snapshotFor(t, a, b)
			c2, s2, h2 := snapshotFor(t, b, a)
			ra := ingestWith(t, ns, a, c1, s1, h1, pol)
			rb := ingestWith(t, ns, b, c2, s2, h2, pol)
			if ra.Outcome.Kind == OutcomeConflict || rb.Outcome.Kind == OutcomeConflict {
				t.Fatalf("policy %v run %d: spurious conflict after a criss-cross", pol, i)
			}
			if ra.Head != rb.Head {
				t.Fatalf("policy %v run %d: the two nodes built different merges", pol, i)
			}
		}
	}
}

// replaceDocAt appends the transaction a whole-document replace makes: a delete of the document
// followed by a write of the new body, both in one commit. That is what KdbServerRuntime.PutJSON
// emits, and what any client replacing a document rather than patching it emits, so it is the
// shape most real commits have.
func replaceDocAt(t *testing.T, s side, ns string, parent codec.Hash, docID codec.UUID, json string) document.Commit {
	t.Helper()
	if err := s.storage.PutDocument(ns, document.Document{ID: docID, JSON: json}); err != nil {
		t.Fatalf("putDocument: %v", err)
	}
	parentCommit, err := s.dag.GetCommitOrThrow(parent)
	if err != nil {
		t.Fatalf("getCommitOrThrow(parent): %v", err)
	}
	tree, err := s.storage.CommitTree(ns, parentCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("commitTree: %v", err)
	}
	txID, _ := codec.RandomUUID()
	authorID, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: parent,
		Operations: []document.Op{
			document.DeleteOp{DocID: docID},
			document.WriteOp{DocID: docID, Patch: json},
		},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	commit, err := s.dag.AppendCommitDetached(tx, parent, tree, nil, "test replace")
	if err != nil {
		t.Fatalf("appendCommit: %v", err)
	}
	return commit
}

// TestMergeKeepsADocumentWrittenByAReplace is the regression for a merge reading a document's
// value from the first operation naming it in a commit rather than the last.
//
// A commit's operations are applied in order, so what a commit leaves a document holding is its
// last operation on it. A replace is a delete followed by a write, so reading the first one said
// "this document is not there" - and a document neither side appeared to have is a document the
// merge has nothing to carry across. The other side's write was dropped, silently, and the merge
// then declared a tree no other node could rebuild: every node that received that merge refused
// it as an integrity failure, for ever, so nothing about that divergence could ever travel.
func TestMergeKeepsADocumentWrittenByAReplace(t *testing.T) {
	ns := "app/replace"
	b, c := forkTwoSides(t, ns)

	// One side creates a document the way a replace does; the other writes a different one, so
	// the histories diverge without conflicting over anything.
	ch, _ := c.dag.Head()
	theirs := newUUID(t)
	c1 := replaceDocAt(t, c, ns, ch, theirs, `{"from":"c"}`)
	setHead(t, c, c1.Hash)

	bh, _ := b.dag.Head()
	mine := newUUID(t)
	b1 := writeDoc(t, b, ns, bh, mine, `{"from":"b"}`)
	setHead(t, b, b1.Hash)

	cc, cs, chh := snapshotFor(t, b, c)
	r := ingestWith(t, ns, b, cc, cs, chh, transaction.ConflictPolicyLastWrite)
	if r.Outcome.Kind == OutcomeConflict {
		t.Fatalf("two sides writing different documents reported a conflict: %+v", r.Outcome)
	}

	_, head, _, _ := b.dag.HeadCommit()
	for _, want := range []struct {
		id   codec.UUID
		body string
	}{{theirs, `{"from":"c"}`}, {mine, `{"from":"b"}`}} {
		doc, err := b.storage.GetDocument(ns, want.id, head.DocumentTreeHash)
		if err != nil {
			t.Fatalf("reading %s after the merge: %v", want.id, err)
		}
		if doc == nil {
			t.Fatalf("the merge dropped %s, which %s wrote", want.id, want.body)
		}
		if doc.JSON != want.body {
			t.Fatalf("after the merge %s holds %s, want %s", want.id, doc.JSON, want.body)
		}
	}
}

// TestMergeReadsTheLastOperationOnADocument is the same fault one level down: a commit that
// writes a document and then deletes it leaves it deleted, and a merge that read the write would
// resurrect it.
func TestMergeReadsTheLastOperationOnADocument(t *testing.T) {
	ns := "app/lastop"
	b, c := forkTwoSides(t, ns)

	// A document both sides start from.
	root, _ := b.dag.Head()
	shared := newUUID(t)
	seed := writeDoc(t, b, ns, root, shared, `{"v":1}`)
	setHead(t, b, seed.Hash)
	cc, cs, chh := snapshotFor(t, c, b)
	ingestWith(t, ns, c, cc, cs, chh, transaction.ConflictPolicyLastWrite)

	// C writes it once more and then deletes it, in one commit.
	ch, _ := c.dag.Head()
	parentCommit, err := c.dag.GetCommitOrThrow(ch)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	if err := c.storage.DeleteDocument(ns, shared); err != nil {
		t.Fatalf("delete: %v", err)
	}
	tree, err := c.storage.CommitTree(ns, parentCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("commitTree: %v", err)
	}
	txID, _ := codec.RandomUUID()
	authorID, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: ch,
		Operations: []document.Op{
			document.WriteOp{DocID: shared, Patch: `{"v":2}`},
			document.DeleteOp{DocID: shared},
		},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	c1, err := c.dag.AppendCommitDetached(tx, ch, tree, nil, "write then delete")
	if err != nil {
		t.Fatalf("appendCommit: %v", err)
	}
	setHead(t, c, c1.Hash)

	// B moves on elsewhere, so the two diverge.
	bh, _ := b.dag.Head()
	b1 := writeDoc(t, b, ns, bh, newUUID(t), `{"from":"b"}`)
	setHead(t, b, b1.Hash)

	cc, cs, chh = snapshotFor(t, b, c)
	ingestWith(t, ns, b, cc, cs, chh, transaction.ConflictPolicyLastWrite)

	_, head, _, _ := b.dag.HeadCommit()
	doc, err := b.storage.GetDocument(ns, shared, head.DocumentTreeHash)
	if err != nil {
		t.Fatalf("reading the deleted document: %v", err)
	}
	if doc != nil {
		t.Fatalf("the merge brought back a document its writer had deleted: %s", doc.JSON)
	}
}
