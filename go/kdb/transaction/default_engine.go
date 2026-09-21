package transaction

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

type defaultEngine struct {
	conflictPolicy ConflictPolicy
	customResolver ConflictResolver
	// uniqueKeys is nil when unique-constraint enforcement is disabled; see EngineOptions.
	uniqueKeys *UniqueKeyRegistry
	// preconditions gates per-operation precondition evaluation; see EngineOptions.
	preconditions bool
}

func (e *defaultEngine) ConflictPolicy() ConflictPolicy { return e.conflictPolicy }
func (e *defaultEngine) CustomResolver() ConflictResolver {
	return e.customResolver
}

func (e *defaultEngine) Commit(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	sch schema.KdbSchema,
	targetHead *codec.Hash,
	message string,
) (TransactionResult, error) {
	head, err := d.Head()
	if err != nil {
		return nil, err
	}
	if targetHead != nil {
		head = *targetHead
	}
	if !d.HasCommit(tx.BaseVersion) {
		return nil, NewBaseNotFoundError("missing base commit", tx.ID, tx.BaseVersion)
	}
	if existing := e.findExistingCommit(tx, d, head, []codec.Hash{head}); existing != nil {
		return ResultSuccess{Commit: *existing, NewTreeHash: existing.DocumentTreeHash}, nil
	}
	baseCommit, err := d.GetCommitOrThrow(tx.BaseVersion)
	if err != nil {
		return nil, err
	}
	targetCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return nil, err
	}
	return e.finalizeTransaction(tx, d, store, sch, head, baseCommit.DocumentTreeHash, baseCommit.DocumentTreeHash, targetCommit.DocumentTreeHash, message)
}

func (e *defaultEngine) Replay(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	sch schema.KdbSchema,
	replayTarget codec.Hash,
	message string,
) (TransactionResult, error) {
	if !d.HasCommit(replayTarget) {
		return nil, kdberr.NewVersionNotFoundError("replay target missing", d.NamespaceID, replayTarget.Hex())
	}
	if existing := e.findExistingCommit(tx, d, replayTarget, []codec.Hash{replayTarget}); existing != nil {
		return ResultSuccess{Commit: *existing, NewTreeHash: existing.DocumentTreeHash}, nil
	}
	baseline, err := d.GetCommitOrThrow(replayTarget)
	if err != nil {
		return nil, err
	}
	tree := baseline.DocumentTreeHash
	return e.finalizeTransaction(tx, d, store, sch, replayTarget, tree, tree, tree, message)
}

func (e *defaultEngine) Merge(
	primaryHead, mergedHead codec.Hash,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	sch schema.KdbSchema,
	message string,
) (TransactionResult, error) {
	ancestor := d.CommonAncestor(primaryHead, mergedHead)
	if ancestor == nil {
		return nil, NewMergeBaseNotFoundError("branches disjoint", primaryHead, mergedHead)
	}
	exclude := map[codec.Hash]struct{}{*ancestor: {}}
	branchCommits := d.CommitsSince(mergedHead, exclude)
	commitSet := make(map[codec.Hash]struct{}, len(branchCommits))
	for _, h := range branchCommits {
		commitSet[h] = struct{}{}
	}
	if len(commitSet) == 0 {
		commitSet[mergedHead] = struct{}{}
	}
	ordered := topoSort(d, commitSet)

	head := primaryHead
	for _, hash := range ordered {
		mc, err := d.GetCommitOrThrow(hash)
		if err != nil {
			return nil, err
		}
		if len(mc.Operations) == 0 {
			continue
		}
		step := document.Transaction{
			ID:           mc.TransactionID,
			BaseVersion:  head,
			Operations:   mc.Operations,
			Timestamp:    mc.Timestamp,
			AuthorNodeID: mc.AuthorNodeID,
		}
		res, err := e.Replay(step, d, store, sch, head, fmt.Sprintf("merge-step:%s", mc.Hash.Hex()))
		if err != nil {
			return nil, err
		}
		switch r := res.(type) {
		case ResultSuccess:
			head = r.Commit.Hash
		case ResultConflict:
			return r, nil
		case ResultSchemaError:
			return r, nil
		case ResultAborted:
			return r, nil
		}
	}

	headCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return nil, err
	}
	mergedTree, err := d.GetDocumentTreeOrThrow(headCommit.DocumentTreeHash)
	if err != nil {
		return nil, err
	}
	mergedCommit, err := d.GetCommitOrThrow(mergedHead)
	if err != nil {
		return nil, err
	}
	tipSchema := headCommit.SchemaHash
	markerID, _ := codec.RandomUUID()
	mergeMarker := document.Transaction{
		ID:           markerID,
		BaseVersion:  primaryHead,
		Operations:   nil,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: mergedCommit.AuthorNodeID,
	}
	// AppendMergeCommitOnto, not AppendMergeCommit: the replay loop above walked the branch head
	// forward through a chain of scratch commits, and the marker deliberately roots back at
	// primaryHead. head - the tip of that replay chain - is what must not have moved under us.
	mergeCommit, err := d.AppendMergeCommitOnto(&head, mergeMarker, primaryHead, mergedHead, mergedTree, tipSchema, message)
	if err != nil {
		return nil, err
	}
	return ResultSuccess{Commit: mergeCommit, NewTreeHash: mergeCommit.DocumentTreeHash}, nil
}

func (e *defaultEngine) Validate(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	sch schema.KdbSchema,
) ([]OperationViolation, error) {
	if !d.HasCommit(tx.BaseVersion) {
		return nil, NewBaseNotFoundError("missing base commit", tx.ID, tx.BaseVersion)
	}
	base, err := d.GetCommitOrThrow(tx.BaseVersion)
	if err != nil {
		return nil, err
	}
	out := runSchemaPhase(tx, store, d.NamespaceID, sch, base.DocumentTreeHash)
	return out.violations, nil
}

func (e *defaultEngine) finalizeTransaction(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	incomingSchema schema.KdbSchema,
	anchorCommit, baseDocTreeHash, baselineDocTreeHash, targetDocTreeHash codec.Hash,
	message string,
) (TransactionResult, error) {
	prepared, result, err := e.plan(tx, d, store, incomingSchema, anchorCommit, baseDocTreeHash, baselineDocTreeHash, targetDocTreeHash)
	if err != nil || result != nil {
		return result, err
	}
	return prepared.Apply(ApplyOptions{Message: message})
}

// guardedOpIndexes returns the operations carrying a real (non-ExpectAny) precondition. Those
// operations are exempt from base-version conflict detection - see detectConflicts.
func guardedOpIndexes(tx document.Transaction) map[int]struct{} {
	out := make(map[int]struct{}, len(tx.Preconditions))
	for _, p := range tx.Preconditions {
		if p.Kind != document.ExpectAny {
			out[p.OpIndex] = struct{}{}
		}
	}
	return out
}

func detectConflicts(
	tx document.Transaction,
	namespaceID string,
	store storage.Adapter,
	baseTreeHash, targetTreeHash codec.Hash,
	projectedWrites map[int]document.Document,
	guarded map[int]struct{},
) []OperationConflict {
	var out []OperationConflict
	for index, op := range tx.Operations {
		// An operation with an explicit precondition has already been checked against the tree
		// this transaction is landing on, and it passed. Re-checking it against the transaction's
		// *base* version would answer a question the caller did not ask and does not care about:
		// "has this document changed since the version my client last happened to commit". For a
		// compare-and-set that check is not merely redundant, it is fatal - a client whose cached
		// base version is stale (which, under contention, is every client that just lost a round)
		// would be refused for a conflict its precondition already ruled out, and no amount of
		// retrying would help, because losing a round is exactly what makes the base stale.
		if _, exempt := guarded[index]; exempt {
			continue
		}
		switch o := op.(type) {
		case document.WriteOp:
			baseDoc, _ := store.GetDocument(namespaceID, o.DocID, baseTreeHash)
			existingDoc, _ := store.GetDocument(namespaceID, o.DocID, targetTreeHash)
			if contentHashEqual(baseDoc, existingDoc) {
				continue
			}
			if baseDoc == nil && existingDoc == nil {
				continue
			}
			var ctype kdberr.ConflictOperationType
			switch {
			case existingDoc != nil && baseDoc == nil:
				ctype = kdberr.DeleteWrite
			case existingDoc == nil && baseDoc != nil:
				ctype = kdberr.WriteDelete
			default:
				ctype = kdberr.ConcurrentWrite
			}
			incoming := projectedWrites[index]
			out = append(out, OperationConflict{
				OpIndex: index, Op: op, Type: ctype,
				ExistingDoc: existingDoc, IncomingDoc: &incoming, BaseDoc: baseDoc,
			})
		case document.DeleteOp:
			baseDoc, _ := store.GetDocument(namespaceID, o.DocID, baseTreeHash)
			existingDoc, _ := store.GetDocument(namespaceID, o.DocID, targetTreeHash)
			if baseDoc == nil && existingDoc == nil {
				continue
			}
			if contentHashEqual(baseDoc, existingDoc) {
				continue
			}
			ctype := kdberr.ConcurrentWrite
			if existingDoc != nil {
				ctype = kdberr.ConcurrentWrite
			} else {
				ctype = kdberr.DeleteWrite
			}
			out = append(out, OperationConflict{
				OpIndex: index, Op: op, Type: ctype,
				ExistingDoc: existingDoc, BaseDoc: baseDoc,
			})
		}
	}
	return out
}

func preflightFileWrites(tx document.Transaction, store storage.Adapter) []OperationViolation {
	var violations []OperationViolation
	for index, op := range tx.Operations {
		fw, ok := op.(document.FileWriteOp)
		if !ok {
			continue
		}
		data, err := store.ReadBlob(fw.BlobHash)
		if err != nil || data == nil {
			violations = append(violations, OperationViolation{
				OpIndex: index,
				Op:      op,
				Violations: []kdberr.FieldViolation{{
					FieldName: fw.Path, ViolationType: kdberr.CustomConstraint,
					Detail: "blob not found: " + fw.BlobHash.Hex(),
				}},
			})
		}
	}
	return violations
}

type schemaOutcome struct {
	rollingSchema   schema.KdbSchema
	violations      []OperationViolation
	writesByOpIndex map[int]document.Document
}

func runSchemaPhase(
	tx document.Transaction,
	store storage.Adapter,
	namespaceID string,
	initialSchema schema.KdbSchema,
	baselineTreeHash codec.Hash,
) schemaOutcome {
	rolling := initialSchema
	var violations []OperationViolation
	writes := make(map[int]document.Document)

	// pending is the state each document has reached *within this transaction*: the value an
	// earlier operation left it at, or a nil entry where an earlier operation deleted it. Present
	// with a nil value and absent are different answers, which is why this is a pointer map and
	// not a document map.
	//
	// Every operation used to be resolved against the baseline tree independently, so a
	// transaction could not see its own earlier writes. Two consequences, both wrong:
	//
	//   - A second write to the same document merged over the *original*, silently dropping the
	//     fields the first one had set.
	//   - There was no way to express replacement at all. A delete followed by a write is the
	//     obvious spelling of "make the document exactly this", and the commit fold already reads
	//     it that way (embed.applyCommitToTree cancels a delete when a later write names the same
	//     document) - but staging merged the write over the document the delete was removing, so
	//     what got committed still carried the old keys.
	//
	// Reading against the rolling state fixes both, and gives replacement without a new operation
	// kind - which matters because Op is a cross-language union with golden fixtures behind it,
	// so a new branch is a format change and this is not.
	pending := make(map[codec.UUID]*document.Document)

	for index, op := range tx.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			baseDoc, touched := pending[o.DocID]
			if !touched {
				baseDoc, _ = store.GetDocument(namespaceID, o.DocID, baselineTreeHash)
			}
			var candidate document.Document
			var err error
			if baseDoc != nil {
				candidate, err = baseDoc.Merge(o.Patch)
			} else {
				candidate, err = document.FromJSONWithID(o.DocID, o.Patch)
			}
			if err != nil {
				violations = append(violations, OperationViolation{
					OpIndex: index, Op: op,
					Violations: []kdberr.FieldViolation{{
						FieldName: o.DocID.String(), ViolationType: kdberr.CustomConstraint,
						Detail: "invalid write payload",
					}},
				})
				continue
			}
			vr := schema.Validate(candidate, rolling)
			if vr.IsFailure() {
				var fieldViolations []kdberr.FieldViolation
				if sve, ok := vr.Exception().(*kdberr.SchemaViolationError); ok {
					fieldViolations = sve.Violations
				} else {
					fieldViolations = []kdberr.FieldViolation{{
						FieldName: "", ViolationType: kdberr.CustomConstraint, Detail: vr.Exception().Error(),
					}}
				}
				violations = append(violations, OperationViolation{
					OpIndex: index, Op: op, Violations: fieldViolations,
				})
				continue
			}
			stored, _ := vr.Value()
			writes[index] = stored
			// Copied out of the loop variable's scope: the map has to hold this operation's
			// document, not whatever the next iteration puts in that slot.
			settled := stored
			pending[o.DocID] = &settled
		case document.DeleteOp:
			// Recorded so a later write in the same transaction starts from nothing rather than
			// from the document being removed. That is what makes delete-then-write a replace.
			pending[o.DocID] = nil
		case document.SchemaMigrationOp:
			mig, err := DecodeMigration(o.MigrationPayload)
			if err != nil {
				violations = append(violations, OperationViolation{
					OpIndex: index, Op: op,
					Violations: []kdberr.FieldViolation{{
						FieldName: "migration", ViolationType: kdberr.CustomConstraint, Detail: err.Error(),
					}},
				})
				continue
			}
			mr := schema.ApplyMigration(rolling, mig)
			if mr.IsFailure() {
				detail := mr.Exception().Error()
				violations = append(violations, OperationViolation{
					OpIndex: index, Op: op,
					Violations: []kdberr.FieldViolation{{
						FieldName: "", ViolationType: kdberr.CustomConstraint, Detail: detail,
					}},
				})
				continue
			}
			rolling, _ = mr.Value()
		}
	}
	return schemaOutcome{rollingSchema: rolling, violations: violations, writesByOpIndex: writes}
}

// findExistingCommit detects an idempotent retry: the same transaction id already produced a
// commit with these exact parents, so the caller should see that original result rather than
// attempt a duplicate commit. O(1) via the DAG's transaction index (GetCommitByTransactionID) -
// this used to walk up to 8192 commits of history on every single Commit/Replay call regardless
// of whether a retry was actually happening, which measured as ~88% of all allocation in a
// profiled run and was the dominant cause of kdb-service getting OOM-killed under sustained
// write load once history grew past a few thousand commits (docs/benchmarks/lightsail-sim/
// README.md). anchorCommit is unused now that the lookup is keyed by transaction id rather than
// a history walk from it, but stays in the signature to avoid touching call sites.
func (e *defaultEngine) findExistingCommit(
	tx document.Transaction,
	d *dag.InMemoryCommitDag,
	anchorCommit codec.Hash,
	parents []codec.Hash,
) *document.Commit {
	_ = anchorCommit
	commit, ok := d.GetCommitByTransactionID(tx.ID)
	if !ok || !parentHashesEqual(commit.ParentHashes, parents) {
		return nil
	}
	return &commit
}

func toReport(tx document.Transaction, anchor codec.Hash, conflicts []OperationConflict) kdberr.ConflictReport {
	items := make([]kdberr.ConflictItem, 0, len(conflicts))
	for _, c := range conflicts {
		var docID string
		var incoming *string
		switch op := c.Op.(type) {
		case document.WriteOp:
			docID = op.DocID.String()
			if c.IncomingDoc != nil {
				s := c.IncomingDoc.JSON
				incoming = &s
			}
		case document.DeleteOp:
			docID = op.DocID.String()
		}
		var local *string
		if c.ExistingDoc != nil {
			s := c.ExistingDoc.JSON
			local = &s
		}
		var actual *string
		if c.ActualContentHash != nil {
			s := c.ActualContentHash.Hex()
			actual = &s
		}
		items = append(items, kdberr.ConflictItem{
			DocumentID: docID, OperationType: c.Type, LocalDoc: local, IncomingDoc: incoming,
			ActualContentHash: actual,
		})
	}
	return kdberr.ConflictReport{
		TransactionID: tx.ID.String(),
		BaseHash:      tx.BaseVersion.Hex(),
		TargetHash:    anchor.Hex(),
		Conflicts:     items,
	}
}

func contentHashEqual(a, b *document.Document) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	ha, err1 := a.ContentHash()
	hb, err2 := b.ContentHash()
	if err1 != nil || err2 != nil {
		return false
	}
	return ha == hb
}

func parentHashesEqual(a, b []codec.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func topoSort(d *dag.InMemoryCommitDag, hashes map[codec.Hash]struct{}) []codec.Hash {
	set := make(map[codec.Hash]struct{})
	for h := range hashes {
		if d.HasCommit(h) {
			set[h] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	indegree := make(map[codec.Hash]int)
	children := make(map[codec.Hash][]codec.Hash)
	for h := range set {
		c, err := d.GetCommitOrThrow(h)
		if err != nil {
			continue
		}
		n := 0
		for _, p := range c.ParentHashes {
			if _, in := set[p]; in {
				n++
				children[p] = append(children[p], h)
			}
		}
		indegree[h] = n
	}
	var queue []codec.Hash
	for h, deg := range indegree {
		if deg == 0 {
			queue = append(queue, h)
		}
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i].Hex() < queue[j].Hex() })
	var out []codec.Hash
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		out = append(out, h)
		kids := children[h]
		sort.Slice(kids, func(i, j int) bool { return kids[i].Hex() < kids[j].Hex() })
		for _, down := range kids {
			indegree[down]--
			if indegree[down] == 0 {
				queue = append(queue, down)
			}
		}
	}
	if len(out) != len(set) {
		hexes := make([]codec.Hash, 0, len(set))
		for h := range set {
			hexes = append(hexes, h)
		}
		sort.Slice(hexes, func(i, j int) bool { return hexes[i].Hex() < hexes[j].Hex() })
		return hexes
	}
	return out
}

// operationsAsStored replaces each WriteOp's patch with the document that was actually staged for
// it, leaving every other operation untouched.
//
// A new slice rather than an in-place edit: Operations is the caller's slice, and a transaction
// that was rejected further down should not come back mutated.
func operationsAsStored(ops []document.Op, writes map[int]document.Document) []document.Op {
	if len(writes) == 0 {
		return ops
	}
	out := make([]document.Op, len(ops))
	copy(out, ops)
	for index, doc := range writes {
		if index < 0 || index >= len(out) {
			continue
		}
		w, ok := out[index].(document.WriteOp)
		if !ok {
			continue
		}
		out[index] = document.WriteOp{DocID: w.DocID, Patch: doc.JSON}
	}
	return out
}
