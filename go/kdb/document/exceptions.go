package document

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DecodeError is a document-layer decode failure.
type DecodeError struct {
	msg   string
	docID *codec.UUID
	cause error
}

func (e *DecodeError) Error() string {
	if e.cause != nil {
		return e.msg + ": " + e.cause.Error()
	}
	return e.msg
}

func (e *DecodeError) Unwrap() error { return e.cause }
func (e *DecodeError) Code() kdberr.Code {
	return kdberr.KdbDecodeError
}

func NewDecodeError(msg string, docID *codec.UUID, cause error) *DecodeError {
	return &DecodeError{msg: msg, docID: docID, cause: cause}
}

// CommitDecodeError is a commit payload decode failure.
type CommitDecodeError struct {
	msg   string
	hash  *codec.Hash
	cause error
}

func (e *CommitDecodeError) Error() string {
	if e.cause != nil {
		return e.msg + ": " + e.cause.Error()
	}
	return e.msg
}

func (e *CommitDecodeError) Unwrap() error { return e.cause }
func (e *CommitDecodeError) Code() kdberr.Code {
	return kdberr.KdbDecodeError
}

func NewCommitDecodeError(msg string, cause error) *CommitDecodeError {
	return &CommitDecodeError{msg: msg, cause: cause}
}
