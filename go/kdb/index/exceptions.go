package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// NotFoundError is returned when an index lookup misses.
type NotFoundError struct {
	*kdberr.SchemaError
	NamespaceID string
	FieldName   string
	Type        IndexType
}

func NewNotFoundError(msg, ns, field string, typ IndexType) *NotFoundError {
	return &NotFoundError{
		SchemaError: kdberr.NewSchemaError(msg, nil),
		NamespaceID: ns,
		FieldName:   field,
		Type:        typ,
	}
}

// TypeMismatchError is returned when an operation does not match index type.
type TypeMismatchError struct {
	*kdberr.SchemaError
	FieldName    string
	ExpectedType IndexType
	ActualType   IndexType
}

func NewTypeMismatchError(msg, field string, expected, actual IndexType) *TypeMismatchError {
	return &TypeMismatchError{
		SchemaError:  kdberr.NewSchemaError(msg, nil),
		FieldName:    field,
		ExpectedType: expected,
		ActualType:   actual,
	}
}

// UniqueViolationError is returned on unique index conflicts.
type UniqueViolationError struct {
	*kdberr.SchemaViolationError
	NamespaceID   string
	FieldName     string
	Key           Key
	ExistingDocID codec.UUID
	IncomingDocID codec.UUID
}
