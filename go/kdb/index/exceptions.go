package index

import (
	"fmt"

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

// DimensionMismatchError is the VectorDimensionMismatch of Layer 16 §7: a document's vector
// has the wrong length for its index. It is a schema violation, so the commit path rejects the
// commit rather than half-applying it.
type DimensionMismatchError struct {
	*kdberr.SchemaViolationError
	FieldName string
	Expected  int
	Actual    int
}

func NewDimensionMismatchError(field string, expected, actual int) *DimensionMismatchError {
	msg := fmt.Sprintf("VectorDimensionMismatch: field %s expects %d dimensions, got %d", field, expected, actual)
	return &DimensionMismatchError{
		SchemaViolationError: kdberr.NewSchemaViolationError(msg, []kdberr.FieldViolation{{
			FieldName:     field,
			ViolationType: kdberr.TypeMismatch,
			Detail:        msg,
		}}),
		FieldName: field,
		Expected:  expected,
		Actual:    actual,
	}
}

// NewUniqueViolationError reports a unique index conflict between two documents.
func NewUniqueViolationError(ns, field string, key Key, existing, incoming codec.UUID) *UniqueViolationError {
	msg := fmt.Sprintf("unique index violation on %s: documents %s and %s share key %s", field, existing, incoming, KeyString(key))
	return &UniqueViolationError{
		SchemaViolationError: kdberr.NewSchemaViolationError(msg, []kdberr.FieldViolation{{
			FieldName:     field,
			ViolationType: kdberr.UniqueConstraint,
			Detail:        msg,
		}}),
		NamespaceID:   ns,
		FieldName:     field,
		Key:           key,
		ExistingDocID: existing,
		IncomingDocID: incoming,
	}
}
