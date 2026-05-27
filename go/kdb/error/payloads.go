package error

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

// ConflictReport is returned on transaction conflicts.
type ConflictReport struct {
	TransactionID string
	BaseHash      string
	TargetHash    string
	Conflicts     []ConflictItem
}

// ConflictItem is one conflicting document operation.
type ConflictItem struct {
	DocumentID    string
	OperationType ConflictOperationType
	LocalDoc      *string
	IncomingDoc   *string
}

// ConflictOperationType classifies conflict kinds.
type ConflictOperationType int

const (
	ConcurrentWrite ConflictOperationType = iota
	WriteDelete
	DeleteWrite
	SchemaIncompatible
)
