package transaction

import (
	"encoding/json"

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
	// ExistingOrigin and IncomingOrigin name the writes that produced each side's value, when
	// the conflict comes from a peer-sync merge: which node wrote it, in which commit, and when.
	// Zero when unknown.
	ExistingOrigin ConflictOrigin
	IncomingOrigin ConflictOrigin
}

// ConflictOrigin is the write that produced one side of a conflict.
type ConflictOrigin struct {
	NodeID          codec.UUID
	Commit          codec.Hash
	TimestampMicros int64
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

// ResultAborted is returned when the write phase fails after validation passed; any staged
// storage writes were rolled back via Adapter.DiscardPending.
type ResultAborted struct {
	Cause error
}

func (ResultAborted) isTransactionResult() {}

// OperationConflict describes one conflicting operation.
type OperationConflict struct {
	OpIndex     int
	Op          document.Op
	Type        kdberr.ConflictOperationType
	ExistingDoc *document.Document
	IncomingDoc *document.Document
	BaseDoc     *document.Document
	// ActualContentHash is the hash actually found at the document, set only for
	// kdberr.PreconditionFailed. A compare-and-set client needs the value that beat it, not just
	// the fact that something did.
	ActualContentHash *codec.Hash
}

// OperationViolation describes schema or preflight failure for one op.
type OperationViolation struct {
	OpIndex    int
	Op         document.Op
	Violations []kdberr.FieldViolation
}

// conflictOriginJSON is ConflictOrigin as the control plane and the conflict queue carry it: ids
// as their text forms, zero values omitted.
type conflictOriginJSON struct {
	NodeID          string `json:"nodeId,omitempty"`
	Commit          string `json:"commit,omitempty"`
	TimestampMicros int64  `json:"timestampMicros,omitempty"`
}

// MarshalJSON writes the origin with its node id and commit as text.
func (o ConflictOrigin) MarshalJSON() ([]byte, error) {
	var j conflictOriginJSON
	if o.NodeID != (codec.UUID{}) {
		j.NodeID = o.NodeID.String()
	}
	if o.Commit != (codec.Hash{}) {
		j.Commit = o.Commit.Hex()
	}
	j.TimestampMicros = o.TimestampMicros
	return json.Marshal(j)
}

// UnmarshalJSON reads what MarshalJSON writes.
func (o *ConflictOrigin) UnmarshalJSON(b []byte) error {
	var j conflictOriginJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*o = ConflictOrigin{TimestampMicros: j.TimestampMicros}
	if j.NodeID != "" {
		id, err := codec.ParseUUID(j.NodeID)
		if err != nil {
			return err
		}
		o.NodeID = id
	}
	if j.Commit != "" {
		h, err := codec.HashFromHex(j.Commit)
		if err != nil {
			return err
		}
		o.Commit = h
	}
	return nil
}
