package error

import (
	"errors"
	"fmt"
)

// Exception is the base typed error for KDB.
type Exception interface {
	error
	Code() Code
}

type base struct {
	code  Code
	msg   string
	cause error
}

func (e *base) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.msg, e.cause)
	}
	return e.msg
}

func (e *base) Unwrap() error { return e.cause }
func (e *base) Code() Code    { return e.code }

// DecodeError is a Layer 0 typed codec decode error.
type DecodeError struct {
	*base
	Offset int
}

func NewDecodeError(msg string, offset int, cause error) *DecodeError {
	return &DecodeError{base: &base{code: KdbDecodeError, msg: msg, cause: cause}, Offset: offset}
}

// EncodeError is a Layer 0 typed codec encode error.
type EncodeError struct{ *base }

func NewEncodeError(msg string, cause error) *EncodeError {
	return &EncodeError{base: &base{code: KdbEncodeError, msg: msg, cause: cause}}
}

// SchemaError is a Layer 0 schema registry error.
type SchemaError struct{ *base }

func NewSchemaError(msg string, cause error) *SchemaError {
	return &SchemaError{base: &base{code: KdbSchemaError, msg: msg, cause: cause}}
}

// JsonPathError is a Layer 1 JSON path error.
type JsonPathError struct {
	*base
	Path string
}

func NewJsonPathError(msg, path string, cause error) *JsonPathError {
	return &JsonPathError{base: &base{code: JSONPathError, msg: msg, cause: cause}, Path: path}
}

// VersionNotFoundError is a DAG version lookup error.
type VersionNotFoundError struct {
	*base
	NamespaceName string
	Reference     string
}

func NewVersionNotFoundError(msg, ns, ref string) *VersionNotFoundError {
	return &VersionNotFoundError{
		base:          &base{code: VersionNotFound, msg: msg},
		NamespaceName: ns,
		Reference:     ref,
	}
}

// IceStorageError indicates a commit was archived to cold storage.
type IceStorageError struct {
	*base
	NamespaceName   string
	Hash            string
	ArchiveLocation string
}

func NewIceStorageError(msg, ns, hash, location string) *IceStorageError {
	return &IceStorageError{
		base:            &base{code: IceStorage, msg: msg},
		NamespaceName:   ns,
		Hash:            hash,
		ArchiveLocation: location,
	}
}

// ConflictError is a transaction conflict.
type ConflictError struct {
	*base
	Report ConflictReport
}

func NewConflictError(msg string, report ConflictReport) *ConflictError {
	return &ConflictError{base: &base{code: Conflict, msg: msg}, Report: report}
}

// DocumentLockedError indicates a document is held by another session's transaction.
type DocumentLockedError struct {
	*base
	NamespaceName string
	DocID         string
	Owner         string
}

func NewDocumentLockedError(msg, ns, docID, owner string) *DocumentLockedError {
	return &DocumentLockedError{
		base:          &base{code: DocumentLocked, msg: msg},
		NamespaceName: ns,
		DocID:         docID,
		Owner:         owner,
	}
}

// NamespaceNotFoundError indicates a missing namespace.
type NamespaceNotFoundError struct {
	*base
	NamespaceName string
}

func NewNamespaceNotFoundError(msg, ns string) *NamespaceNotFoundError {
	return &NamespaceNotFoundError{base: &base{code: NamespaceNotFound, msg: msg}, NamespaceName: ns}
}

// TransportErr is a transport-layer failure.
type TransportErr struct{ *base }

func NewTransportErr(msg string, cause error) *TransportErr {
	return &TransportErr{base: &base{code: TransportError, msg: msg, cause: cause}}
}

// IsException reports whether err is, or wraps, a KDB Exception.
//
// Unwrapping matters: an exception that has passed through a `fmt.Errorf("...: %w", err)`
// anywhere on its way up is still the same failure, and a plain type assertion stops seeing it
// the moment any layer adds context. That is how CodeOf below started answering 0 - "no code" -
// for errors that carry a perfectly good one, which reaches a client as a generic failure
// instead of the typed one it should be able to act on. Same reasoning, same fix as the wire
// layer's asError.
func IsException(err error) bool {
	var e Exception
	return errors.As(err, &e)
}

// CodeOf returns the code of err or of the first Exception it wraps, or zero if there is none.
func CodeOf(err error) Code {
	var e Exception
	if errors.As(err, &e) {
		return e.Code()
	}
	return 0
}

// AsException returns the first Exception in err's chain, if any.
func AsException(err error) (Exception, bool) {
	var e Exception
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
