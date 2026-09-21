package transaction

import (
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Preparer is implemented by engines that can split a commit into a check phase and a write
// phase. Engine.Commit is exactly PrepareCommit followed by Apply; the split exists for callers
// that must check several transactions before writing any of them - a transaction spanning
// namespaces, which has to know every participant will land before it lets the first one.
//
// An optional interface rather than a method on Engine, so that wrapping engines elsewhere keep
// compiling; the default engine implements it.
type Preparer interface {
	// PrepareCommit runs every check Commit runs - base version, file preflight, schema,
	// preconditions, conflict policy, unique keys - against the current head of d, and stages
	// nothing. A rejection comes back as a non-nil TransactionResult (ResultConflict or
	// ResultSchemaError) with a nil *PreparedCommit; a hard failure as an error.
	//
	// The result is only valid while nothing else writes to d or store: the caller must hold
	// whatever serializes writers to this namespace (the server's write gate) from here until
	// Apply or Discard. Unlike Commit, this does not look for an earlier commit of the same
	// transaction id - a prepared transaction is always new.
	PrepareCommit(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema) (*PreparedCommit, TransactionResult, error)
}

// ApplyOptions are the write-phase settings of a prepared commit.
type ApplyOptions struct {
	// Message is recorded on the commit, inside its hash.
	Message string
	// Provisional marks the commit as one part of a group whose outcome is not yet decided. The
	// DAG will not let a checkpoint capture it until SettleProvisional is called for it - see
	// dag.InMemoryCommitDag.SettleProvisional.
	Provisional bool
}

// PreparedCommit is a transaction that has passed every check and is ready to be written. Apply
// writes it; Discard abandons it. Exactly one of the two should be called.
type PreparedCommit struct {
	e     *defaultEngine
	tx    document.Transaction
	d     *dag.InMemoryCommitDag
	store storage.Adapter

	anchorCommit codec.Hash
	extendingTip bool
	writes       map[int]document.Document
	schemaFrame  schemaOutcome
	uniques      uniquePlan
	finished     bool
}

// Namespace is the namespace this commit will land in.
func (p *PreparedCommit) Namespace() string { return p.d.NamespaceID }

// Anchor is the commit this one will be appended onto.
func (p *PreparedCommit) Anchor() codec.Hash { return p.anchorCommit }

// PrepareCommit implements Preparer.
func (e *defaultEngine) PrepareCommit(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	sch schema.KdbSchema,
) (*PreparedCommit, TransactionResult, error) {
	head, err := d.Head()
	if err != nil {
		return nil, nil, err
	}
	if !d.HasCommit(tx.BaseVersion) {
		return nil, nil, NewBaseNotFoundError("missing base commit", tx.ID, tx.BaseVersion)
	}
	baseCommit, err := d.GetCommitOrThrow(tx.BaseVersion)
	if err != nil {
		return nil, nil, err
	}
	targetCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return nil, nil, err
	}
	p, result, err := e.plan(tx, d, store, sch, head, baseCommit.DocumentTreeHash, baseCommit.DocumentTreeHash, targetCommit.DocumentTreeHash)
	if err != nil || result != nil {
		return nil, result, err
	}
	return &p, nil, nil
}

// plan is the check half of finalizeTransaction: everything that can reject a transaction, and
// nothing that writes. Returning a rejection from here costs nothing to unwind, which is what
// lets a caller check several transactions before committing to any.
//
// It returns the prepared commit by value so the ordinary commit path - plan then Apply, in one
// call - keeps it on the stack. Returning a pointer cost every single commit an extra heap
// allocation of the whole plan (~450 bytes) for a split only cross-namespace commits use.
func (e *defaultEngine) plan(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	incomingSchema schema.KdbSchema,
	anchorCommit, baseDocTreeHash, baselineDocTreeHash, targetDocTreeHash codec.Hash,
) (PreparedCommit, TransactionResult, error) {
	// Whether this transaction extends the branch tip or deliberately forks off an older commit
	// is decided here, before any work is staged, because that is when the caller's intent is
	// still legible. If the anchor is the tip right now, the append must still find it there
	// (AppendCommit's compare-and-swap) - anything else means another writer advanced the branch
	// while this transaction was being planned, and appending anyway would leave this commit
	// stored but unreachable. If the anchor was already behind the tip, the caller asked for a
	// fork (Replay onto a named target) and the branch head is not this transaction's business.
	extendingTip := d.HeadIs(anchorCommit)
	if fileViolations := preflightFileWrites(tx, store); len(fileViolations) > 0 {
		return PreparedCommit{}, ResultSchemaError{Violations: fileViolations}, nil
	}
	schemaFrame := runSchemaPhase(tx, store, d.NamespaceID, incomingSchema, baselineDocTreeHash)
	if len(schemaFrame.violations) > 0 {
		return PreparedCommit{}, ResultSchemaError{Violations: schemaFrame.violations}, nil
	}
	writes := schemaFrame.writesByOpIndex

	// Preconditions are evaluated before - and independently of - the conflict policy. A client
	// that said "only if absent" or "only if the hash is still X" asked a question the policy has
	// no standing to answer for it: ConflictPolicyLastWrite exists to make ordinary writes
	// converge, not to wave through an assertion the client explicitly asked to be checked.
	// That matters most for the Upsert path, which runs on LastWrite and is exactly where
	// insert-if-absent is used.
	guarded := map[int]struct{}{}
	if e.preconditions && len(tx.Preconditions) > 0 {
		preFailures, preViolations := evaluatePreconditions(tx, d.NamespaceID, store, targetDocTreeHash)
		if len(preViolations) > 0 {
			return PreparedCommit{}, ResultSchemaError{Violations: preViolations}, nil
		}
		if len(preFailures) > 0 {
			return PreparedCommit{}, ResultConflict{Report: toReport(tx, anchorCommit, preFailures), ConflictingOps: preFailures}, nil
		}
		guarded = guardedOpIndexes(tx)
	}

	var conflicts []OperationConflict
	switch e.conflictPolicy {
	case ConflictPolicyAppendOnly, ConflictPolicyLastWrite:
		conflicts = nil
	default:
		conflicts = detectConflicts(tx, d.NamespaceID, store, baseDocTreeHash, targetDocTreeHash, writes, guarded)
	}

	if len(conflicts) > 0 && e.conflictPolicy == ConflictPolicyStrict {
		return PreparedCommit{}, ResultConflict{Report: toReport(tx, anchorCommit, conflicts), ConflictingOps: conflicts}, nil
	}

	if len(conflicts) > 0 && e.conflictPolicy == ConflictPolicyCustom {
		if e.customResolver == nil {
			return PreparedCommit{}, ResultConflict{Report: toReport(tx, anchorCommit, conflicts), ConflictingOps: conflicts}, nil
		}
		for _, c := range conflicts {
			if _, ok := c.Op.(document.WriteOp); !ok {
				return PreparedCommit{}, ResultConflict{Report: toReport(tx, anchorCommit, conflicts), ConflictingOps: conflicts}, nil
			}
			w := c.Op.(document.WriteOp)
			resolved, err := e.customResolver.Resolve(DocumentConflict{
				DocID: w.DocID, OperationType: c.Type,
				ExistingDoc: c.ExistingDoc, IncomingDoc: c.IncomingDoc, BaseDoc: c.BaseDoc,
			})
			if err != nil || resolved == nil {
				return PreparedCommit{}, ResultConflict{Report: toReport(tx, anchorCommit, conflicts), ConflictingOps: conflicts}, nil
			}
			vr := schema.Validate(*resolved, schemaFrame.rollingSchema)
			if vr.IsFailure() {
				violations := []kdberr.FieldViolation{{
					FieldName: "", ViolationType: kdberr.CustomConstraint,
					Detail: vr.Exception().Error(),
				}}
				if sve, ok := vr.Exception().(*kdberr.SchemaViolationError); ok {
					violations = sve.Violations
				}
				return PreparedCommit{}, ResultSchemaError{Violations: []OperationViolation{{
					OpIndex: c.OpIndex, Op: c.Op, Violations: violations,
				}}}, nil
			}
			writes[c.OpIndex], _ = vr.Value()
		}
	}

	// Unique-constraint enforcement runs after conflict resolution, so a custom resolver's
	// rewritten document is the one checked, and before any write is staged, so a violation
	// costs nothing to unwind. It is evaluated against targetDocTreeHash - what this transaction
	// is actually landing on - not the base it was built against.
	uPlan, uniqueViolations := planUniqueKeys(
		tx, d.NamespaceID, store, targetDocTreeHash, schemaFrame.rollingSchema, e.uniqueKeys, writes,
	)
	if len(uniqueViolations) > 0 {
		return PreparedCommit{}, ResultSchemaError{Violations: uniqueViolations}, nil
	}
	return PreparedCommit{
		e:            e,
		tx:           tx,
		d:            d,
		store:        store,
		anchorCommit: anchorCommit,
		extendingTip: extendingTip,
		writes:       writes,
		schemaFrame:  schemaFrame,
		uniques:      uPlan,
	}, nil, nil
}

// Discard abandons a prepared commit. Nothing was staged by PrepareCommit, so there is nothing
// to undo; this exists so a caller's intent is explicit and so Apply cannot run afterwards.
func (p *PreparedCommit) Discard() { p.finished = true }

// Apply is the write half: stage every write, build the new tree, append the commit, and move
// the unique-key registry. Rejections are impossible by now - PrepareCommit ran every check - so
// a failure here is a storage or consistency fault, reported as ResultAborted (the write phase
// failed and was rolled back) or an error.
func (p *PreparedCommit) Apply(opts ApplyOptions) (TransactionResult, error) {
	if p.finished {
		return nil, fmt.Errorf("kdb: prepared commit for transaction %s was already applied or discarded", p.tx.ID)
	}
	p.finished = true
	tx, d, store, writes := p.tx, p.d, p.store, p.writes

	if abortErr := func() error {
		for idx, op := range tx.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				if doc, ok := writes[idx]; ok {
					if err := store.PutDocument(d.NamespaceID, doc); err != nil {
						return err
					}
				}
			case document.DeleteOp:
				if err := store.DeleteDocument(d.NamespaceID, o.DocID); err != nil {
					return err
				}
			}
		}
		return nil
	}(); abortErr != nil {
		// The write phase failed after validation/conflict checks passed -
		// roll back whatever was staged rather than leaving a half-applied
		// transaction, and report it distinctly from a hard error so
		// callers can retry cleanly.
		_ = store.DiscardPending(d.NamespaceID)
		return ResultAborted{Cause: abortErr}, nil
	}

	// Record what was stored, not what was asked for.
	//
	// A WriteOp's patch is merged over the existing document on the way in (baseDoc.Merge above),
	// but replay reads the same op as the whole document (embed/delta_replay.go) and so does the
	// historical-tree fold (embed.applyCommitToTree). Committing the request rather than the
	// result made those disagree: a merge patch naming fewer keys than the stored document
	// reconstructed as a *smaller* document, which hashes differently, which means the tree replay
	// rebuilds is not the tree this commit records - and every later read at that commit resolves
	// a tree nothing ever wrote. The namespace comes back empty rather than failing loudly.
	//
	// Substituting the staged document closes that by construction: there is exactly one reading
	// of the op left, and it is the one the write actually produced. It also matches the engine's
	// own principle that the document is the truth - a commit should record a fact about the data,
	// not the request that produced it.
	//
	// See the regression tests: server.TestUpsertMergeSurvivesRestart drives the documented public
	// merge path, and control.TestEngineWriteAndReplayAgree drives it over HTTP.
	tx.Operations = operationsAsStored(tx.Operations, writes)

	anchor, err := d.GetCommitOrThrow(p.anchorCommit)
	if err != nil {
		return nil, err
	}
	newTree, err := store.CommitTree(d.NamespaceID, anchor.DocumentTreeHash)
	if err != nil {
		return nil, err
	}
	var schemaHashWire *codec.Hash
	if !p.schemaFrame.rollingSchema.IsNone() {
		h := p.schemaFrame.rollingSchema.SchemaHash
		schemaHashWire = &h
	}
	commit, err := d.AppendCommitWith(tx, p.anchorCommit, newTree, schemaHashWire, opts.Message, dag.AppendOptions{
		Detached:    !p.extendingTip,
		Provisional: opts.Provisional,
	})
	if err != nil {
		// A lost compare-and-swap leaves this transaction's staged writes behind on the adapter,
		// where the next transaction to call CommitTree would silently absorb them. Drop them -
		// same rollback the write phase does when it fails partway.
		var headConflict *dag.HeadConflictError
		if errors.As(err, &headConflict) {
			_ = store.DiscardPending(d.NamespaceID)
		}
		return nil, err
	}
	// The registry moves only once the commit is in the DAG. Applying earlier would leave a
	// phantom claim behind if AppendCommit failed; applying later - after the caller's durability
	// wait - would open a window in which the next writer, already serialized behind this one at
	// the write gate, sees a key this commit has taken as still free. Erring toward "claimed
	// slightly too early" costs at most a spurious rejection of a write that raced a failing
	// commit; erring the other way costs a duplicate that the constraint exists to prevent.
	if !p.uniques.empty() {
		p.e.uniqueKeys.Apply(p.uniques.retract, p.uniques.claim)
	}
	return ResultSuccess{Commit: commit, NewTreeHash: commit.DocumentTreeHash}, nil
}

var _ Preparer = (*defaultEngine)(nil)
