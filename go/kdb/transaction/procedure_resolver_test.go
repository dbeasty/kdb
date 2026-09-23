package transaction_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/script"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// A stored procedure as transaction.ConflictPolicyCustom's resolver, driven through the real
// Engine rather than calling script.ConflictResolver.Resolve directly (that is resolver_test.go,
// in kdb/script). These tests exist because a procedure sitting in the resolver seat changes what
// the *engine* does around it - which write it stages, whether it commits atomically with
// everything else in the transaction, and what "the head did not move" actually means when the
// thing that decided the content was a sandboxed script rather than a fixed rule.

// sumCounters merges two divergent increments of the same counter by adding both deltas to the
// base, rather than picking a side and losing one side's increment. See the corpus case this
// mirrors: go/testdata/golden/conflict_corpus/sum-two-counters.json.
const sumCounters = `function main(c) {
	var base = c.base.count;
	return { doc: { count: base + (c.local.count - base) + (c.remote.count - base) } };
}`

func TestCustomResolverProcedureSumsCountersOnConflictAndCommits(t *testing.T) {
	ns := "app/counters"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()

	doc, err := document.FromJSON(`{"count":10}`)
	if err != nil {
		t.Fatal(err)
	}
	strictEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	seeded := mustCommit(t, strictEngine, newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON}), d, store).(transaction.ResultSuccess)
	head := seeded.Commit.Hash

	customEngine := transaction.NewEngine(transaction.ConflictPolicyCustom, script.ConflictResolver{Source: sumCounters})

	// Both transactions are based on the same head, so the second one lands after the first and
	// conflicts against it.
	landed := mustCommit(t, customEngine, newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":14}`}), d, store)
	if _, ok := landed.(transaction.ResultSuccess); !ok {
		t.Fatalf("the first write should not conflict with anything, got %T", landed)
	}

	res, err := customEngine.Commit(newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":17}`}), d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := res.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected the procedure's merge to commit, got %T: %+v", res, res)
	}

	got, err := store.GetDocument(ns, doc.ID, success.NewTreeHash)
	if err != nil || got == nil {
		t.Fatalf("got %v %v", got, err)
	}
	if got.JSON != `{"count":21}` {
		t.Fatalf("got %s, want the procedure's summed count (10 base + 4 + 7)", got.JSON)
	}
}

// TestCustomResolverThrowAndDeferAreBothJustAConflict documents a real sharp edge: prepare.go
// collapses "the resolver returned an error" and "the resolver returned nil" into exactly the
// same ResultConflict, with the same report the engine would have produced with no resolver at
// all. A procedure that throws and one that returns {defer:true} are indistinguishable from the
// Engine's own result type - the difference only survives in whatever logged the resolver's own
// error, which the engine itself does not do. Neither commits, and neither moves the head.
func TestCustomResolverThrowAndDeferAreBothJustAConflict(t *testing.T) {
	for name, src := range map[string]string{
		"throws": `function main(c) { throw new Error("cannot decide"); }`,
		"defers": `function main(c) { return { defer: true }; }`,
	} {
		t.Run(name, func(t *testing.T) {
			ns := "app/counters-" + name
			d, err := dag.NewInMemoryCommitDag(ns)
			if err != nil {
				t.Fatal(err)
			}
			store := mem.NewInMemoryStorageAdapter()
			base, _ := d.Head()
			doc, err := document.FromJSON(`{"count":1}`)
			if err != nil {
				t.Fatal(err)
			}
			strictEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
			seeded := mustCommit(t, strictEngine, newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON}), d, store).(transaction.ResultSuccess)
			head := seeded.Commit.Hash

			customEngine := transaction.NewEngine(transaction.ConflictPolicyCustom, script.ConflictResolver{Source: src})
			landed := mustCommit(t, customEngine, newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":2}`}), d, store)
			if _, ok := landed.(transaction.ResultSuccess); !ok {
				t.Fatalf("the first write should not conflict, got %T", landed)
			}
			headBeforeConflict, err := d.Head()
			if err != nil {
				t.Fatal(err)
			}

			res, err := customEngine.Commit(newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":3}`}), d, store, schema.None(), nil, "")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := res.(transaction.ResultConflict); !ok {
				t.Fatalf("expected the conflict to be reported (not committed, not a hard error), got %T: %+v", res, res)
			}
			if headNow, _ := d.Head(); headNow != headBeforeConflict {
				t.Fatal("a resolver that could not decide must not move the head")
			}
		})
	}
}

// TestCustomResolverResolvedWriteRollsBackWithRestOfTransactionOnStorageFailure: a procedure's
// successful resolution is not exempt from the rest of its transaction failing. Two documents
// land in one transaction - one conflicts and is resolved by the procedure, the other does not
// conflict at all - and the storage write for the *non-conflicting* document is made to fail.
// Both must be rolled back: the resolver already did its job correctly, but "all or nothing" means
// its answer only ever lands as part of one atomic commit, never on its own.
func TestCustomResolverResolvedWriteRollsBackWithRestOfTransactionOnStorageFailure(t *testing.T) {
	ns := "app/counters-abort"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	inner := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()

	conflictingDoc, err := document.FromJSON(`{"count":1}`)
	if err != nil {
		t.Fatal(err)
	}
	siblingDoc, err := document.FromJSON(`{"v":"untouched"}`)
	if err != nil {
		t.Fatal(err)
	}
	strictEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	seedTx := newTx(base,
		document.WriteOp{DocID: conflictingDoc.ID, Patch: conflictingDoc.JSON},
		document.WriteOp{DocID: siblingDoc.ID, Patch: siblingDoc.JSON},
	)
	seeded := mustCommit(t, strictEngine, seedTx, d, inner).(transaction.ResultSuccess)
	head := seeded.Commit.Hash

	// Move the conflicting doc on, so the transaction under test - based on the older head -
	// conflicts on it.
	bump := mustCommit(t, strictEngine, newTx(head, document.WriteOp{DocID: conflictingDoc.ID, Patch: `{"count":2}`}), d, inner).(transaction.ResultSuccess)

	// The sibling document's storage write is the one that fails - deliberately not the one the
	// resolver touches, so a green resolution is what gets rolled back, not a red one.
	store := &failingStorageAdapter{Adapter: inner, failOnPutDocID: siblingDoc.ID}
	customEngine := transaction.NewEngine(transaction.ConflictPolicyCustom, script.ConflictResolver{Source: sumCounters})

	tx := newTx(head,
		document.WriteOp{DocID: conflictingDoc.ID, Patch: `{"count":3}`},
		document.WriteOp{DocID: siblingDoc.ID, Patch: `{"v":"changed"}`},
	)
	res, err := customEngine.Commit(tx, d, store, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(transaction.ResultAborted); !ok {
		t.Fatalf("expected the whole transaction aborted, got %T: %+v", res, res)
	}

	if headNow, _ := d.Head(); headNow != bump.Commit.Hash {
		t.Fatal("head must not move when the write phase fails after the resolver already ran")
	}
	got, err := inner.GetDocument(ns, conflictingDoc.ID, bump.NewTreeHash)
	if err != nil || got == nil || got.JSON != `{"count":2}` {
		t.Fatalf("the procedure's resolved write must not have landed: %v %v", got, err)
	}

	// And the abort left nothing behind for a retry to trip over: the same transaction, with the
	// storage failure lifted, commits cleanly.
	retried, err := transaction.NewEngine(transaction.ConflictPolicyCustom, script.ConflictResolver{Source: sumCounters}).
		Commit(tx, d, inner, schema.None(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := retried.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("expected the retry to succeed, got %T: %+v", retried, retried)
	}
	// The conflict's base is the value at tx's own BaseVersion (the pre-bump seed, count 1), not
	// the bump - so the procedure sees base=1, local (the landed bump)=2 (delta 1), remote (this
	// tx's own write)=3 (delta 2), and sums to 1+1+2=4.
	resolved, err := inner.GetDocument(ns, conflictingDoc.ID, success.NewTreeHash)
	if err != nil || resolved == nil || resolved.JSON != `{"count":4}` {
		t.Fatalf("got %v %v, want count 4", resolved, err)
	}
}

// TestCustomResolverProcedureRewritingASchemaViolatingDocumentIsRejected: the resolver's answer
// is validated like any other write, not trusted because it came from conflict resolution. A
// procedure that "resolves" a conflict into a document that breaks the namespace's schema must
// not commit - it should look exactly like any other schema violation to the caller.
func TestCustomResolverProcedureRewritingASchemaViolatingDocumentIsRejected(t *testing.T) {
	ns := "app/counters-schema"
	sch, err := schema.Build([]schema.Field{
		schema.MustField("count", schema.Int64Type{}, true, false, false),
	}, 1, codec.Timestamp{}, "counters")
	if err != nil {
		t.Fatal(err)
	}
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	base, _ := d.Head()
	doc, err := document.FromJSON(`{"count":1}`)
	if err != nil {
		t.Fatal(err)
	}
	strictEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	seeded, err := strictEngine.Commit(newTx(base, document.WriteOp{DocID: doc.ID, Patch: doc.JSON}), d, store, sch, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	success, ok := seeded.(transaction.ResultSuccess)
	if !ok {
		t.Fatalf("seed write failed: %T %+v", seeded, seeded)
	}
	head := success.Commit.Hash

	// Returns a document with no "count" field at all - valid JSON, invalid under the schema.
	const dropsRequiredField = `function main(c) { return { doc: { renamedByMistake: c.local.count } }; }`
	customEngine := transaction.NewEngine(transaction.ConflictPolicyCustom, script.ConflictResolver{Source: dropsRequiredField})
	mustCommit(t, customEngine, newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":2}`}), d, store)

	res, err := customEngine.Commit(newTx(head, document.WriteOp{DocID: doc.ID, Patch: `{"count":3}`}), d, store, sch, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	schemaErr, ok := res.(transaction.ResultSchemaError)
	if !ok {
		t.Fatalf("expected the resolved document to be schema-checked and rejected, got %T: %+v", res, res)
	}
	if len(schemaErr.Violations) == 0 {
		t.Fatal("expected at least one violation naming the dropped field")
	}
}
