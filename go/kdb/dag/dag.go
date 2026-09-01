package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// CommitDAG is the primary interface for commit DAG operations within one namespace.
type CommitDAG interface {
	GetCommit(hash codec.Hash) (document.Commit, bool)
	GetCommitOrThrow(hash codec.Hash) (document.Commit, error)
	PutCommit(commit document.Commit, requireParents bool) error
	GetDocumentTree(treeHash codec.Hash) (document.DocumentTree, bool)
	GetDocumentTreeOrThrow(treeHash codec.Hash) (document.DocumentTree, error)
	PutDocumentTree(tree document.DocumentTree)
	AppendCommit(
		tx document.Transaction,
		parentHash codec.Hash,
		newDocumentTree document.DocumentTree,
		schemaHash *codec.Hash,
		message string,
	) (document.Commit, error)
	Head() (codec.Hash, error)
	// HeadCommit returns the head hash and the commit it names from one consistent read.
	// Prefer it over Head followed by GetCommit on any hot path: it is a single atomic
	// snapshot load in the in-memory implementation, and the hash and commit are
	// guaranteed to agree, which the two separate calls cannot promise.
	HeadCommit() (codec.Hash, document.Commit, bool, error)
	SetHead(branchName string, hash codec.Hash) error
	GetBranch(name string) (document.Branch, bool)
	ListBranches() []document.Branch
	CreateBranch(name string, fromHash codec.Hash) (document.Branch, error)
	Walk(from codec.Hash, until *codec.Hash, limit int) []TraversalEntry
}
