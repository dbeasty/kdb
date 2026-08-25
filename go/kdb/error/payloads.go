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
	DocumentID    string                 `json:"documentId"`
	OperationType ConflictOperationType  `json:"operationType"`
	LocalDoc      *string                `json:"localDoc,omitempty"`
	IncomingDoc   *string                `json:"incomingDoc,omitempty"`
}

// ConflictOperationType classifies conflict kinds.
type ConflictOperationType int

const (
	ConcurrentWrite ConflictOperationType = iota
	WriteDelete
	DeleteWrite
	SchemaIncompatible
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
	default:
		*t = ConcurrentWrite
	}
	return nil
}
