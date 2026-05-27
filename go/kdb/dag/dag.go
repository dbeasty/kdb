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
	Head() (codec.Hash, error)
	SetHead(branchName string, hash codec.Hash) error
	GetBranch(name string) (document.Branch, bool)
}
