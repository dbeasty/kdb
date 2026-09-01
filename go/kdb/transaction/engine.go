package transaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Engine commits, replays, merges, and validates transactions.
type Engine interface {
	ConflictPolicy() ConflictPolicy
	CustomResolver() ConflictResolver

	Commit(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema, targetHead *codec.Hash, message string) (TransactionResult, error)
	Replay(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema, replayTarget codec.Hash, message string) (TransactionResult, error)
	Merge(primaryHead, mergedHead codec.Hash, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema, message string) (TransactionResult, error)
	Validate(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema) ([]OperationViolation, error)
}

// NewEngine returns a DefaultTransactionEngine with no unique-constraint enforcement. Equivalent
// to NewEngineWithOptions with a zero EngineOptions.
func NewEngine(policy ConflictPolicy, resolver ConflictResolver) Engine {
	return NewEngineWithOptions(policy, resolver, EngineOptions{})
}

// EngineOptions carries the cross-cutting state an engine needs beyond its conflict policy.
type EngineOptions struct {
	// UniqueKeys enforces unique-declared schema fields on every commit. nil disables
	// enforcement entirely, which is the historical behavior (`unique=true` was schema metadata
	// no write path consulted).
	//
	// Every engine operating on one runtime must share ONE registry instance - a runtime's
	// Commit and Upsert engines both write documents into the same namespace, so two registries
	// would each be blind to the other's claims and neither would be authoritative.
	UniqueKeys *UniqueKeyRegistry

	// Preconditions evaluates per-operation write preconditions (compare-and-set,
	// insert-if-absent) against the tree a transaction actually lands on. nil disables
	// evaluation, in which case a transaction carrying preconditions commits as if it had none -
	// so this must be wired anywhere untrusted transactions are accepted.
	Preconditions bool
}

// NewEngineWithOptions returns a DefaultTransactionEngine with the given cross-cutting options.
func NewEngineWithOptions(policy ConflictPolicy, resolver ConflictResolver, opts EngineOptions) Engine {
	return &defaultEngine{
		conflictPolicy: policy,
		customResolver: resolver,
		uniqueKeys:     opts.UniqueKeys,
		preconditions:  opts.Preconditions,
	}
}

// NewBuilder creates a transaction builder at the DAG head.
func NewBuilder(namespaceID string, d *dag.InMemoryCommitDag, authorNodeID codec.UUID, sch schema.KdbSchema) (*Builder, error) {
	head, err := d.Head()
	if err != nil {
		return nil, err
	}
	return &Builder{
		NamespaceID:  namespaceID,
		BaseVersion:  head,
		AuthorNodeID: authorNodeID,
		Schema:       sch,
	}, nil
}
