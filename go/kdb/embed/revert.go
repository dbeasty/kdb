package embed

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// RevertResult reports what a revert put back.
type RevertResult struct {
	// Commit is the new commit the revert wrote. History moves forward,
	// never backward, so this is always a fresh hash.
	Commit codec.Hash
	// Target is the commit whose state was restored.
	Target codec.Hash
	// Restored is how many documents were written back, Removed how many
	// were deleted because the target did not have them.
	Restored int
	Removed  int
}

// treeResolver is the optional capability of a storage adapter that can
// resolve a whole document tree, historical ones included.
type treeResolver interface {
	TreeAt(treeHash codec.Hash) (document.DocumentTree, bool, error)
}

// RevertTo restores the state a namespace had at a past commit, by writing
// a *new* commit whose document tree is the old one's.
//
// This is the only durable undo this engine can offer, and the reason is
// structural rather than a matter of taste:
//
//   - Commits are not invertible. A WriteOp is {DocID, Patch} and a
//     DeleteOp is a document id; neither records what the value was
//     before, so no commit can be run backwards.
//   - Moving a branch head backwards would not work either. The storage
//     engine keeps one live tree that every write continues from (see
//     engine.ServerEngine.CommitTree, which builds on e.tree regardless of
//     the parent tree hash it is handed), so after a backwards SetHead
//     reads would resolve the old tree while the next write still built on
//     the newest one - reads and writes silently disagreeing about what
//     the database is.
//
// Going forward avoids both. The DAG stays append-only, peer sync stays
// coherent, the revert is itself in the history, and the revert is itself
// revertible.
//
// The commit this produces names a tree with exactly the target's
// (document id -> content hash) mapping, so its tree hash equals the
// target's tree hash. That equality is the operation's postcondition and
// the cheapest way to check it did what it says.
func RevertTo(rt *EmbeddedKdbRuntime, namespaceID, spec string) (RevertResult, error) {
	if err := rt.AssertWritable(); err != nil {
		return RevertResult{}, err
	}
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		return RevertResult{}, fmt.Errorf(
			"kdb: namespace %q cannot revert: its commit graph does not navigate", namespaceID)
	}
	target, err := nav.ResolveRevision(spec)
	if err != nil {
		return RevertResult{}, explainHistoryFailure(rt, namespaceID, spec, err)
	}
	targetCommit, err := rt.DAG.GetCommitOrThrow(target)
	if err != nil {
		return RevertResult{}, explainHistoryFailure(rt, namespaceID, spec, err)
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return RevertResult{}, err
	}
	headCommit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return RevertResult{}, err
	}
	if targetCommit.DocumentTreeHash == headCommit.DocumentTreeHash {
		// Already there. Writing an empty commit would be honest but
		// noisy; reporting the state as reached is what the caller means.
		return RevertResult{Commit: head, Target: target}, nil
	}

	targetTree, err := resolveTree(rt, targetCommit.DocumentTreeHash)
	if err != nil {
		// The commit is still in the graph but its tree is not
		// reconstructable, which under history=none is exactly what a
		// reclaimed segment looks like.
		return RevertResult{}, explainHistoryFailure(rt, namespaceID, spec, err)
	}
	headTree, err := resolveTree(rt, headCommit.DocumentTreeHash)
	if err != nil {
		return RevertResult{}, err
	}

	targetEntries := targetTree.MaterializedEntries()
	headEntries := headTree.MaterializedEntries()

	var ops []document.Op
	restored := 0
	for docID, want := range targetEntries {
		if have, ok := headEntries[docID]; ok && have == want {
			continue
		}
		doc, err := rt.Storage.GetDocument(namespaceID, docID, targetCommit.DocumentTreeHash)
		if err != nil {
			return RevertResult{}, err
		}
		if doc == nil {
			// The tree names a version whose bytes are gone. Refuse rather
			// than commit a partial restore that would look like a
			// successful one.
			return RevertResult{}, fmt.Errorf(
				"kdb: cannot revert namespace %q to %s: document %s is named by that commit's tree "+
					"but its content is no longer retained",
				namespaceID, target.Hex(), docID.String())
		}
		if err := rt.Storage.PutDocument(namespaceID, *doc); err != nil {
			return RevertResult{}, err
		}
		ops = append(ops, document.WriteOp{DocID: docID, Patch: doc.JSON})
		restored++
	}
	removed := 0
	for docID := range headEntries {
		if _, ok := targetEntries[docID]; ok {
			continue
		}
		if err := rt.Storage.DeleteDocument(namespaceID, docID); err != nil {
			return RevertResult{}, err
		}
		ops = append(ops, document.DeleteOp{DocID: docID})
		removed++
	}

	tree, err := rt.Storage.CommitTree(namespaceID, headCommit.DocumentTreeHash)
	if err != nil {
		return RevertResult{}, err
	}
	if tree.TreeHash != targetCommit.DocumentTreeHash {
		// The postcondition, checked rather than assumed: a revert that
		// produced a different tree has restored something other than the
		// state asked for, and committing it would record that mistake
		// permanently.
		_ = rt.Storage.DiscardPending(namespaceID)
		return RevertResult{}, fmt.Errorf(
			"kdb: revert of namespace %q to %s produced tree %s, want %s",
			namespaceID, target.Hex(), tree.TreeHash.Hex(), targetCommit.DocumentTreeHash.Hex())
	}

	txID, err := codec.RandomUUID()
	if err != nil {
		return RevertResult{}, err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return RevertResult{}, err
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   ops,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: author,
	}
	c, err := rt.DAG.AppendCommit(tx, head, tree, nil, "revert to "+target.Hex())
	if err != nil {
		return RevertResult{}, err
	}
	return RevertResult{Commit: c.Hash, Target: target, Restored: restored, Removed: removed}, nil
}

// DiffCommits reports which documents differ between two revisions.
//
// Unlike dag.Diff it works at any two points in history, because it
// resolves each tree through the storage engine - object lookup or a fold
// over the log - rather than only finding the ones that happen to be
// cached. dag.Diff cannot do that: it holds the DAG lock, and resolving a
// historical tree re-enters the DAG.
func DiffCommits(rt *EmbeddedKdbRuntime, fromSpec, toSpec string) (dag.CommitDiff, error) {
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		return dag.CommitDiff{}, fmt.Errorf("kdb: this runtime's commit graph does not navigate")
	}
	from, err := nav.ResolveRevision(fromSpec)
	if err != nil {
		return dag.CommitDiff{}, explainHistoryFailure(rt, rt.DefaultNamespace, fromSpec, err)
	}
	to, err := nav.ResolveRevision(toSpec)
	if err != nil {
		return dag.CommitDiff{}, explainHistoryFailure(rt, rt.DefaultNamespace, toSpec, err)
	}
	if from == to {
		return dag.CommitDiff{FromHash: from, ToHash: to}, nil
	}
	fromCommit, err := rt.DAG.GetCommitOrThrow(from)
	if err != nil {
		return dag.CommitDiff{}, err
	}
	toCommit, err := rt.DAG.GetCommitOrThrow(to)
	if err != nil {
		return dag.CommitDiff{}, err
	}
	fromTree, err := resolveTree(rt, fromCommit.DocumentTreeHash)
	if err != nil {
		return dag.CommitDiff{}, explainHistoryFailure(rt, rt.DefaultNamespace, fromSpec, err)
	}
	toTree, err := resolveTree(rt, toCommit.DocumentTreeHash)
	if err != nil {
		return dag.CommitDiff{}, explainHistoryFailure(rt, rt.DefaultNamespace, toSpec, err)
	}
	return dag.DiffTrees(from, to, fromTree, toTree), nil
}

// resolveTree gets a document tree by hash, using the adapter's own
// resolution (object lookup, or a fold over the log) when it has it and
// the DAG's map when it does not - which is the pure in-memory runtime,
// where every tree it ever built is still there.
func resolveTree(rt *EmbeddedKdbRuntime, treeHash codec.Hash) (document.DocumentTree, error) {
	if r, ok := rt.Storage.(treeResolver); ok {
		tree, found, err := r.TreeAt(treeHash)
		if err != nil {
			return document.DocumentTree{}, err
		}
		if found {
			return tree, nil
		}
	}
	tree, ok := rt.DAG.GetDocumentTree(treeHash)
	if !ok {
		return document.DocumentTree{}, fmt.Errorf(
			"kdb: document tree %s is not retained", treeHash.Hex())
	}
	return tree, nil
}
