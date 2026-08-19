package error

import "fmt"

// Exception is the base typed error for KDB.
type Exception interface {
	error
	Code() Code
}

type base struct {
	code Code
	msg  string
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

// IsException reports whether err is a KDB Exception.
func IsException(err error) bool {
	_, ok := err.(Exception)
	return ok
}

// CodeOf returns the error code or zero.
func CodeOf(err error) Code {
	if e, ok := err.(Exception); ok {
		return e.Code()
	}
	return 0
}
