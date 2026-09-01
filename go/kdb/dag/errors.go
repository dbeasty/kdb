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

// HeadConflictError reports that a branch head moved between the moment a writer read it and
// the moment that writer tried to append onto it - the compare-and-swap AppendCommit performs
// against the parent it was handed.
//
// Before this existed, the append simply succeeded and overwrote the branch head: two writers
// racing on the same stale head each produced a valid commit, but only the later one stayed
// reachable from the branch. The earlier writer was told its commit succeeded while its commit
// was silently orphaned - no error anywhere, and the data only "missing" on the next read.
// A conflict a client can see and retry is strictly better than a lost write it cannot.
type HeadConflictError struct {
	Namespace string
	Branch    string
	// Expected is the head the writer planned against; Actual is where the branch had already
	// moved to by the time the append ran.
	Expected codec.Hash
	Actual   codec.Hash
}

func (e *HeadConflictError) Error() string {
	return "branch " + e.Branch + " moved from " + e.Expected.Hex() + " to " + e.Actual.Hex() +
		" while the commit was being prepared"
}

func (e *HeadConflictError) Code() kdberr.Code { return kdberr.Conflict }

func NewHeadConflictError(ns, branch string, expected, actual codec.Hash) *HeadConflictError {
	return &HeadConflictError{Namespace: ns, Branch: branch, Expected: expected, Actual: actual}
}
