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

// NewEngine returns a DefaultTransactionEngine.
func NewEngine(policy ConflictPolicy, resolver ConflictResolver) Engine {
	return &defaultEngine{conflictPolicy: policy, customResolver: resolver}
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
