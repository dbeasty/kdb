package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TraversalEntry is one walk result.
type TraversalEntry interface {
	isTraversalEntry()
}

// FullEntry contains a full commit.
type FullEntry struct {
	Commit document.Commit
}

func (FullEntry) isTraversalEntry() {}

// StubbedEntry contains an archived commit stub.
type StubbedEntry struct {
	Stub document.CommitStub
}

func (StubbedEntry) isTraversalEntry() {}

// CommitDiff is a document identity diff across two commits.
type CommitDiff struct {
	FromHash codec.Hash
	ToHash   codec.Hash
	Entries  []DiffEntry
}

func (d CommitDiff) Added() []DiffAdded {
	var out []DiffAdded
	for _, e := range d.Entries {
		if a, ok := e.(DiffAdded); ok {
			out = append(out, a)
		}
	}
	return out
}

func (d CommitDiff) Removed() []DiffRemoved {
	var out []DiffRemoved
	for _, e := range d.Entries {
		if r, ok := e.(DiffRemoved); ok {
			out = append(out, r)
		}
	}
	return out
}

func (d CommitDiff) Modified() []DiffModified {
	var out []DiffModified
	for _, e := range d.Entries {
		if m, ok := e.(DiffModified); ok {
			out = append(out, m)
		}
	}
	return out
}

func (d CommitDiff) IsEmpty() bool { return len(d.Entries) == 0 }

// DiffEntry classifies one document change.
type DiffEntry interface {
	isDiffEntry()
}

type DiffAdded struct {
	DocID       codec.UUID
	ContentHash codec.Hash
}

func (DiffAdded) isDiffEntry() {}

type DiffRemoved struct {
	DocID       codec.UUID
	ContentHash codec.Hash
}

func (DiffRemoved) isDiffEntry() {}

type DiffModified struct {
	DocID           codec.UUID
	FromContentHash codec.Hash
	ToContentHash   codec.Hash
}

func (DiffModified) isDiffEntry() {}

// CommitRef is a user-facing symbolic revision.
type CommitRef interface {
	isCommitRef()
}

type RefByHash struct{ Hex string }

func (RefByHash) isCommitRef() {}

type RefByBranch struct{ Name string }

func (RefByBranch) isCommitRef() {}

type RefByTag struct{ Name string }

func (RefByTag) isCommitRef() {}

type RefByTime struct{ Timestamp codec.Timestamp }

func (RefByTime) isCommitRef() {}
