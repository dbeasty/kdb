package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// ConsistencyError indicates DAG invariant violation.
type ConsistencyError struct {
	msg       string
	Namespace string
	Hash      *codec.Hash
	cause     error
}

func (e *ConsistencyError) Error() string { return e.msg }
func (e *ConsistencyError) Unwrap() error { return e.cause }
func (e *ConsistencyError) Code() kdberr.Code {
	return kdberr.VersionNotFound
}

func NewConsistencyError(msg, ns string, hash *codec.Hash) *ConsistencyError {
	return &ConsistencyError{msg: msg, Namespace: ns, Hash: hash}
}

// BranchNotFoundError indicates a missing branch.
type BranchNotFoundError struct {
	msg       string
	Namespace string
	Branch    string
}

func (e *BranchNotFoundError) Error() string { return e.msg }
func (e *BranchNotFoundError) Code() kdberr.Code {
	return kdberr.VersionNotFound
}

func NewBranchNotFoundError(msg, ns, branch string) *BranchNotFoundError {
	return &BranchNotFoundError{msg: msg, Namespace: ns, Branch: branch}
}

// TagNotFoundError indicates a missing tag.
type TagNotFoundError struct {
	msg       string
	Namespace string
	Tag       string
}

func (e *TagNotFoundError) Error() string { return e.msg }
func (e *TagNotFoundError) Code() kdberr.Code {
	return kdberr.VersionNotFound
}

func NewTagNotFoundError(msg, ns, tag string) *TagNotFoundError {
	return &TagNotFoundError{msg: msg, Namespace: ns, Tag: tag}
}

// CompactionSafetyError blocks unsafe compaction.
type CompactionSafetyError struct {
	msg       string
	Namespace string
	Blocker   codec.Hash
	Reason    string
}

func (e *CompactionSafetyError) Error() string { return e.msg }
func (e *CompactionSafetyError) Code() kdberr.Code {
	return kdberr.CompactionBoundary
}

func NewCompactionSafetyError(msg, ns string, blocker codec.Hash, reason string) *CompactionSafetyError {
	return &CompactionSafetyError{msg: msg, Namespace: ns, Blocker: blocker, Reason: reason}
}
