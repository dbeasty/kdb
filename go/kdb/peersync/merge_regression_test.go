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
