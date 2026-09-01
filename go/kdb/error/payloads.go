package error

import "encoding/json"

// ViolationType classifies schema violations.
type ViolationType int

const (
	RequiredFieldMissing ViolationType = iota
	TypeMismatch
	UniqueConstraint
	EnumValueNotDeclared
	CustomConstraint
)

func (t ViolationType) String() string {
	switch t {
	case RequiredFieldMissing:
		return "REQUIRED_FIELD_MISSING"
	case TypeMismatch:
		return "TYPE_MISMATCH"
	case UniqueConstraint:
		return "UNIQUE_CONSTRAINT"
	case EnumValueNotDeclared:
		return "ENUM_VALUE_NOT_DECLARED"
	case CustomConstraint:
		return "CUSTOM_CONSTRAINT"
	default:
		return "UNKNOWN"
	}
}

// FieldViolation describes one schema field problem.
type FieldViolation struct {
	FieldName     string
	ViolationType ViolationType
	Detail        string
}

// ConflictReport is returned on transaction conflicts. JSON tags match the wire's
// lowerCamelCase convention (see go/kdb/wire) - this type is marshaled directly into
// ConflictReportMessage.ReportBytes (component 38 spec §6), so its JSON shape is part of the
// wire contract, not just Go-internal.
type ConflictReport struct {
	TransactionID string         `json:"transactionId"`
	BaseHash      string         `json:"baseHash"`
	TargetHash    string         `json:"targetHash"`
	Conflicts     []ConflictItem `json:"conflicts"`
}

// ConflictItem is one conflicting document operation.
type ConflictItem struct {
	DocumentID    string                `json:"documentId"`
	OperationType ConflictOperationType `json:"operationType"`
	LocalDoc      *string               `json:"localDoc,omitempty"`
	IncomingDoc   *string               `json:"incomingDoc,omitempty"`
	// ActualContentHash is the content hash actually found at DocumentID, populated for
	// PreconditionFailed. It is what lets a compare-and-set caller retry against the value that
	// beat it rather than re-reading blind. Omitted (and ignored) for every other conflict kind,
	// so the JSON shape stays compatible with readers that predate preconditions.
	ActualContentHash *string `json:"actualContentHash,omitempty"`
}

// ConflictOperationType classifies conflict kinds.
type ConflictOperationType int

const (
	ConcurrentWrite ConflictOperationType = iota
	WriteDelete
	DeleteWrite
	SchemaIncompatible
	// PreconditionFailed: the operation declared a precondition (compare-and-set,
	// insert-if-absent) that did not hold against the tree the transaction landed on. Appended
	// last deliberately - these constants are iota-based and their ordinals are wire-visible
	// through peer sync, so inserting one anywhere else would renumber the rest.
	PreconditionFailed
)

func (t ConflictOperationType) String() string {
	switch t {
	case ConcurrentWrite:
		return "CONCURRENT_WRITE"
	case WriteDelete:
		return "WRITE_DELETE"
	case DeleteWrite:
		return "DELETE_WRITE"
	case SchemaIncompatible:
		return "SCHEMA_INCOMPATIBLE"
	case PreconditionFailed:
		return "PRECONDITION_FAILED"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON encodes as the enum's name, matching Kotlin's default enum JSON serialization
// (kotlinx.serialization emits the enum constant name, not an ordinal) - so a Go-produced
// ConflictReport decodes the same way on either side, string not ordinal.
func (t ConflictOperationType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

// UnmarshalJSON is MarshalJSON's inverse - needed for any Go-side reader of a ConflictReport
// (e.g. a peer sync host decoding a wire-carried report), not just Go-side producers.
func (t *ConflictOperationType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "CONCURRENT_WRITE":
		*t = ConcurrentWrite
	case "WRITE_DELETE":
		*t = WriteDelete
	case "DELETE_WRITE":
		*t = DeleteWrite
	case "SCHEMA_INCOMPATIBLE":
		*t = SchemaIncompatible
	case "PRECONDITION_FAILED":
		*t = PreconditionFailed
	default:
		*t = ConcurrentWrite
	}
	return nil
}
