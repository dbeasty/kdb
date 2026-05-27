package transaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// ConflictPolicy controls optimistic concurrency behavior.
type ConflictPolicy int

const (
	ConflictPolicyAppendOnly ConflictPolicy = iota
	ConflictPolicyLastWrite
	ConflictPolicyStrict
	ConflictPolicyCustom
)

// ConflictResolver resolves a document conflict when policy is Custom.
type ConflictResolver interface {
	Resolve(conflict DocumentConflict) (*document.Document, error)
}

// DocumentConflict is input to a custom resolver.
type DocumentConflict struct {
	DocID         codec.UUID
	OperationType kdberr.ConflictOperationType
	ExistingDoc   *document.Document
	IncomingDoc   *document.Document
	BaseDoc       *document.Document
}

// TransactionResult is the outcome of commit, replay, or merge.
type TransactionResult interface {
	isTransactionResult()
}

type ResultSuccess struct {
	Commit      document.Commit
	NewTreeHash codec.Hash
}

func (ResultSuccess) isTransactionResult() {}

type ResultConflict struct {
	Report         kdberr.ConflictReport
	ConflictingOps []OperationConflict
}

func (ResultConflict) isTransactionResult() {}

type ResultSchemaError struct {
	Violations []OperationViolation
}

func (ResultSchemaError) isTransactionResult() {}

// OperationConflict describes one conflicting operation.
type OperationConflict struct {
	OpIndex     int
	Op          document.Op
	Type        kdberr.ConflictOperationType
	ExistingDoc *document.Document
	IncomingDoc *document.Document
	BaseDoc     *document.Document
}

// OperationViolation describes schema or preflight failure for one op.
type OperationViolation struct {
	OpIndex    int
	Op         document.Op
	Violations []kdberr.FieldViolation
}
