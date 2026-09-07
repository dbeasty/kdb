package transaction

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// UniqueKey identifies one claimed value tuple of one unique constraint in one namespace
// (kdb-spec-layer16 §9.6). Fields is the constraint's ordered field names joined by
// FieldSeparator - a single-field UNIQUE is the 1-tuple case, so its key is just the field name -
// and Value is a canonical JSON array of the parts (see canonicalTuple), so two documents whose
// parts decode to the same values collide regardless of how the JSON that produced them was
// spelled: `{"n": 1}` and `{"n": 1.0}` claim the same key.
type UniqueKey struct {
	NamespaceID string
	Fields      string
	Value       string
}

// FieldSeparator joins a constraint's field names into UniqueKey.Fields. ASCII unit separator:
// it cannot appear in a schema field name (see schema.NewField's identifier rule), so the joined
// form is unambiguous.
const FieldSeparator = "\x1f"

// NewUniqueKey builds the key for one constraint tuple and its canonical value.
func NewUniqueKey(namespaceID string, fields []string, value string) UniqueKey {
	return UniqueKey{NamespaceID: namespaceID, Fields: strings.Join(fields, FieldSeparator), Value: value}
}

// FieldNames returns the constraint's field names, in order.
func (k UniqueKey) FieldNames() []string {
	if k.Fields == "" {
		return nil
	}
	return strings.Split(k.Fields, FieldSeparator)
}

// FieldName is the human-readable constraint name: the field for a single-field constraint,
// "(a, b)" for a compound one. This is what violations and error messages show.
func (k UniqueKey) FieldName() string {
	names := k.FieldNames()
	if len(names) == 1 {
		return names[0]
	}
	return "(" + strings.Join(names, ", ") + ")"
}

func (k UniqueKey) String() string {
	return k.NamespaceID + "." + k.FieldName() + "=" + k.Value
}

// UniqueKeyRegistry is the authoritative owner map for every unique constraint's claimed value
// tuple in a runtime: (namespace, fields tuple, canonical value tuple) -> the single document id
// holding it.
//
// It is what makes concurrent writers safe on a natural key. KDB's ordinary conflict detection
// is content-addressed and per-document (detectConflicts in default_engine.go): it answers "did
// *this document* change under me", which two clients creating two *different* documents that
// happen to claim the same email never trips. The registry answers the different question - "is
// this value already spoken for by someone else" - and is checked and mutated only from inside
// the caller's write serialization (KdbServerRuntime's writeGate), so the check and the claim
// cannot interleave.
//
// The registry is derived state, rebuilt from the document tree at open (see Rebuild) rather
// than persisted alongside it. A derived structure with its own persistence path is a second
// source of truth and a second recovery bug; rebuilding costs one scan per open.
type UniqueKeyRegistry struct {
	mu     sync.Mutex
	owners map[UniqueKey]codec.UUID
}

// NewUniqueKeyRegistry returns an empty registry.
func NewUniqueKeyRegistry() *UniqueKeyRegistry {
	return &UniqueKeyRegistry{owners: make(map[UniqueKey]codec.UUID)}
}

// Owner returns the document currently holding key, if any.
func (r *UniqueKeyRegistry) Owner(key UniqueKey) (codec.UUID, bool) {
	if r == nil {
		return codec.UUID{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.owners[key]
	return id, ok
}

// Len reports how many keys are currently claimed (tests and metrics).
func (r *UniqueKeyRegistry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.owners)
}

// Apply retracts then claims, as one atomic step. Retraction runs first so a transaction that
// moves a value from one document to another - or rewrites the same document's value - cannot
// transiently drop a claim another goroutine could slip into. A retraction whose key is owned by
// a different document than the one retracting it is ignored, so a stale retraction can never
// silently free somebody else's key.
func (r *UniqueKeyRegistry) Apply(retract map[UniqueKey]codec.UUID, claim map[UniqueKey]codec.UUID) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, owner := range retract {
		if cur, ok := r.owners[key]; ok && cur == owner {
			delete(r.owners, key)
		}
	}
	for key, owner := range claim {
		r.owners[key] = owner
	}
}

// Reset drops every claim. Used before a full Rebuild.
func (r *UniqueKeyRegistry) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owners = make(map[UniqueKey]codec.UUID)
}

// Rebuild repopulates the registry from every document in namespaceID at treeHash. Called at
// runtime open, and again after a migration turns a field unique.
//
// A duplicate found during the scan is returned as an error rather than silently keeping the
// first-seen owner: reaching this state means data on disk already violates a constraint the
// schema declares, and quietly picking a winner would make the violation permanent and
// invisible. Callers decide whether that is fatal (a migration adding unique=true: reject it)
// or reportable (opening an existing runtime: surface it to the operator).
func (r *UniqueKeyRegistry) Rebuild(
	namespaceID string,
	store storage.Adapter,
	treeHash codec.Hash,
	sch schema.KdbSchema,
) error {
	if r == nil {
		return nil
	}
	fresh := make(map[UniqueKey]codec.UUID)
	if !sch.HasUniqueConstraints() {
		r.mu.Lock()
		r.owners = fresh
		r.mu.Unlock()
		return nil
	}
	var scanErr error
	err := store.ScanDocuments(namespaceID, treeHash, 256, func(batch []document.Document) error {
		for _, doc := range batch {
			keys, err := UniqueKeysFor(namespaceID, sch, doc)
			if err != nil {
				// A document whose JSON no longer parses cannot claim a key; it also cannot be
				// silently skipped, since that would let a later write claim a value this
				// document may in fact hold. Surface it.
				scanErr = fmt.Errorf("kdb unique: document %s: %w", doc.ID, err)
				return scanErr
			}
			for _, key := range keys {
				if owner, dup := fresh[key]; dup && owner != doc.ID {
					scanErr = &UniqueConstraintError{Key: key, OwnerDocID: owner, DocID: doc.ID}
					return scanErr
				}
				fresh[key] = doc.ID
			}
		}
		return nil
	})
	if scanErr != nil {
		return scanErr
	}
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.owners = fresh
	r.mu.Unlock()
	return nil
}

// UniqueConstraintError reports two documents claiming one unique value tuple. It names the
// constraint's fields and carries both document ids, because "who already has it" is the one
// thing a client needs to act on and the one thing a bare violation message never tells them.
type UniqueConstraintError struct {
	Key        UniqueKey
	OwnerDocID codec.UUID
	DocID      codec.UUID
}

func (e *UniqueConstraintError) Error() string {
	return fmt.Sprintf(
		"unique constraint violated on %s: value already held by document %s (attempted by %s)",
		e.Key.String(), e.OwnerDocID, e.DocID,
	)
}

// UniqueKeysFor returns the keys doc claims under every unique constraint in sch - one per
// tuple from sch.UniqueTuples(): the single-field UNIQUE flags first, then the compound
// constraints.
//
// Sparse semantics (§9.6): a tuple in which *any* part is absent or JSON null claims nothing -
// matching SQL, where NULLs do not collide with each other. This is a deliberate choice, not an
// oversight: a schema with an optional unique field would otherwise let exactly one document
// omit it, and a compound constraint would otherwise treat "(a, missing)" as a value.
func UniqueKeysFor(namespaceID string, sch schema.KdbSchema, doc document.Document) ([]UniqueKey, error) {
	if !sch.HasUniqueConstraints() {
		return nil, nil
	}
	var out []UniqueKey
	for _, fields := range sch.UniqueTuples() {
		parts := make([]any, 0, len(fields))
		complete := true
		for _, name := range fields {
			raw, err := schema.FieldValue(doc.JSON, name)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				complete = false
				break
			}
			parts = append(parts, raw)
		}
		if !complete {
			continue
		}
		canonical, err := canonicalTuple(parts)
		if err != nil {
			return nil, err
		}
		out = append(out, NewUniqueKey(namespaceID, fields, canonical))
	}
	return out, nil
}

// canonicalTuple renders the decoded parts of one constraint tuple as the registry's comparison
// key: a JSON array, so a 1-tuple's key is `["a@b.c"]` and a 2-tuple's `["a@b.c",1]`.
// encoding/json sorts object keys and normalizes number formatting on re-marshal, so equal
// values always render identically regardless of their original spelling. String comparison
// stays byte-wise: a case-insensitive unique index is a schema decision the schema layer would
// have to express, not a default this layer is entitled to impose.
func canonicalTuple(parts []any) (string, error) {
	b, err := json.Marshal(parts)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// uniquePlan is one transaction's net effect on the registry: the keys it frees and the keys it
// takes, both resolved before anything is applied.
type uniquePlan struct {
	retract map[UniqueKey]codec.UUID
	claim   map[UniqueKey]codec.UUID
}

func (p uniquePlan) empty() bool { return len(p.retract) == 0 && len(p.claim) == 0 }

// planUniqueKeys resolves tx's effect on the registry and reports any violation, without
// mutating anything. Runs against targetTreeHash - the tree the transaction is actually landing
// on, not the session's base - so a stale-based transaction is still checked against reality.
//
// Three distinct ways a claim is legal:
//   - nobody holds the key;
//   - this same document already holds it (rewriting a document without changing the field);
//   - the current holder is releasing it within this same transaction (a swap, or a delete plus
//     a re-create).
//
// Everything else is a violation, including two operations inside one transaction claiming the
// same key: a transaction that would violate the constraint against itself must not be allowed
// to launder that through atomicity.
func planUniqueKeys(
	tx document.Transaction,
	namespaceID string,
	store storage.Adapter,
	targetTreeHash codec.Hash,
	sch schema.KdbSchema,
	registry *UniqueKeyRegistry,
	projectedWrites map[int]document.Document,
) (uniquePlan, []OperationViolation) {
	plan := uniquePlan{
		retract: make(map[UniqueKey]codec.UUID),
		claim:   make(map[UniqueKey]codec.UUID),
	}
	if registry == nil || !sch.HasUniqueConstraints() {
		return plan, nil
	}

	// Pass one: every key this transaction releases, from the pre-transaction state of each
	// document it touches. Collected first so pass two can tell "the holder is stepping aside"
	// from "the holder is still there".
	releasedBy := make(map[UniqueKey]codec.UUID)
	for _, op := range tx.Operations {
		var docID codec.UUID
		switch o := op.(type) {
		case document.WriteOp:
			docID = o.DocID
		case document.DeleteOp:
			docID = o.DocID
		default:
			continue
		}
		existing, _ := store.GetDocument(namespaceID, docID, targetTreeHash)
		if existing == nil {
			continue
		}
		oldKeys, err := UniqueKeysFor(namespaceID, sch, *existing)
		if err != nil {
			// Pre-existing unparseable content is not this transaction's fault and must not
			// block it; it simply releases nothing. Rebuild is where that surfaces.
			continue
		}
		for _, key := range oldKeys {
			releasedBy[key] = docID
			plan.retract[key] = docID
		}
	}

	// Pass two: every key this transaction claims, checked against the registry, against the
	// releases from pass one, and against the other claims in this same transaction.
	var violations []OperationViolation
	for index, op := range tx.Operations {
		write, ok := op.(document.WriteOp)
		if !ok {
			continue
		}
		doc, staged := projectedWrites[index]
		if !staged {
			continue
		}
		newKeys, err := UniqueKeysFor(namespaceID, sch, doc)
		if err != nil {
			violations = append(violations, OperationViolation{
				OpIndex: index, Op: op,
				Violations: []kdberr.FieldViolation{{
					FieldName:     write.DocID.String(),
					ViolationType: kdberr.CustomConstraint,
					Detail:        "cannot evaluate unique constraints: " + err.Error(),
				}},
			})
			continue
		}
		for _, key := range newKeys {
			if claimant, taken := plan.claim[key]; taken && claimant != write.DocID {
				violations = append(violations, uniqueViolation(index, op, key, claimant))
				continue
			}
			owner, held := registry.Owner(key)
			switch {
			case !held:
			case owner == write.DocID:
			case releasedBy[key] == owner:
				// The holder is giving it up in this same transaction.
			default:
				violations = append(violations, uniqueViolation(index, op, key, owner))
				continue
			}
			plan.claim[key] = write.DocID
			// A key this document is re-claiming must not also be retracted, or Apply's
			// retract-then-claim would record a retraction against the previous owner that
			// fires after the new owner has already claimed it.
			if plan.retract[key] == write.DocID {
				delete(plan.retract, key)
			}
		}
	}
	return plan, violations
}

func uniqueViolation(index int, op document.Op, key UniqueKey, owner codec.UUID) OperationViolation {
	return OperationViolation{
		OpIndex: index,
		Op:      op,
		Violations: []kdberr.FieldViolation{{
			FieldName:     key.FieldName(),
			ViolationType: kdberr.UniqueConstraint,
			Detail:        "value already held by document " + owner.String(),
		}},
	}
}
