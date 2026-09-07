package embed

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// maxTreeFoldDepth bounds how far back a rebuild walks looking for an
// ancestor tree it can fold forward from.
//
// A ceiling, not a tuning knob: without one, asking for a tree near the
// beginning of a long history with nothing cached would walk the entire
// commit graph before doing any work. Hitting it reports a miss, which is
// the truthful answer - the alternative is an unbounded stall.
const maxTreeFoldDepth = 100_000

// rebuildTreeByFolding reconstructs one historical document tree, named by
// its hash, for namespaces on the replay strategy where there are no tree
// objects to look it up in.
//
// It folds forward from the nearest *cached* ancestor rather than from the
// empty tree, and that single choice decides what reading history costs.
// Restarting from genesis makes a tree at position k cost O(k), so walking
// a history of H commits costs 1 + 2 + ... + H - quadratic. Folding from
// the nearest cached ancestor makes the ordinary pattern, walking oldest to
// newest, cost O(1) per step: the tree built for the previous commit is
// still resident, one commit behind. Random access costs the distance to
// whatever is cached.
//
// What it cannot avoid is that the commits' operations may themselves have
// been evicted, and getting them back needs the commit-hash-to-frame index,
// built by one pass over the log. That happens once per process on the
// first historical read, not once per miss. Under the objects strategy none
// of this runs at all - the tree is a lookup.
//
// The tree this produces has to match what the engine built commit by
// commit or its hash will not be the one any commit names, which is what
// applyCommitToTree's staging order is for. The result is checked against
// the requested hash before being returned, so a mismatch is a miss rather
// than a wrong answer.
func rebuildTreeByFolding(
	d *dag.InMemoryCommitDag,
	eng *engine.ServerEngine,
	want codec.Hash,
) (document.DocumentTree, bool, error) {
	commitHash, ok := d.CommitForTree(want)
	if !ok {
		// No commit claims this tree. Not an error - it is what asking for
		// a tree this namespace never produced should look like.
		return document.DocumentTree{}, false, nil
	}

	// Back to the nearest ancestor whose tree is resident, collecting the
	// commits to fold forward. A root commit grounds the walk on the empty
	// tree, so a namespace with nothing cached still terminates.
	var path []codec.Hash
	base := document.EmptyDocumentTree()
	grounded := false
	cursor := commitHash
	for depth := 0; depth < maxTreeFoldDepth; depth++ {
		commit, err := d.GetCommitOrThrow(cursor)
		if err != nil {
			return document.DocumentTree{}, false, err
		}
		if commit.DocumentTreeHash != want {
			// CachedTree, not the engine's ordinary lookup: probing must
			// not promote every tree it passes to most-recently-used, or
			// the walk would evict the ones it is walking towards.
			if cached, ok := eng.CachedTree(commit.DocumentTreeHash); ok {
				base = cached
				grounded = true
				break
			}
		}
		path = append(path, cursor)
		if len(commit.ParentHashes) == 0 {
			grounded = true
			break
		}
		cursor = commit.ParentHashes[0]
	}
	if !grounded {
		return document.DocumentTree{}, false, nil
	}

	// path runs from the target back towards the base, so fold it in
	// reverse.
	tree := base
	for i := len(path) - 1; i >= 0; i-- {
		commit, err := d.GetCommitOrThrow(path[i])
		if err != nil {
			return document.DocumentTree{}, false, err
		}
		next, _, _, err := applyCommitToTree(tree, commit)
		if err != nil {
			return document.DocumentTree{}, false, err
		}
		tree = next
	}
	if tree.TreeHash != want {
		return document.DocumentTree{}, false, nil
	}
	return tree, true, nil
}

// applyCommitToTree folds one commit's operations into tree, reproducing
// the staging semantics the write path used. Documents are hashed from
// their text, which is the reason this is expensive: the content hash a
// tree stores cannot be recovered from the log any other way.
func applyCommitToTree(tree document.DocumentTree, c document.Commit) (document.DocumentTree, []engine.TreeChange, []codec.UUID, error) {
	puts := make(map[codec.UUID]document.Document)
	deletes := make(map[codec.UUID]struct{})
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			doc, err := document.FromJSONWithID(o.DocID, o.Patch)
			if err != nil {
				doc = document.Document{ID: o.DocID, JSON: o.Patch}
			}
			puts[o.DocID] = doc
			delete(deletes, o.DocID)
		case document.DeleteOp:
			deletes[o.DocID] = struct{}{}
			delete(puts, o.DocID)
		}
	}
	out := tree
	var err error
	removed := make([]codec.UUID, 0, len(deletes))
	for id := range deletes {
		removed = append(removed, id)
		if out, err = out.Without(id); err != nil {
			return document.DocumentTree{}, nil, nil, err
		}
	}
	changed := make([]engine.TreeChange, 0, len(puts))
	for _, doc := range puts {
		h, err := doc.ContentHash()
		if err != nil {
			return document.DocumentTree{}, nil, nil, err
		}
		changed = append(changed, engine.TreeChange{DocID: doc.ID, ContentHash: h})
		if out, err = out.With(doc.ID, h); err != nil {
			return document.DocumentTree{}, nil, nil, err
		}
	}
	return out, changed, removed, nil
}

// recordTreeObjectsForHistory walks the whole delta log and records a tree
// object for every commit in it - the conversion behind
// MigrateHistoryStrategy's move to the objects strategy.
//
// Uses the same tree-folding code the rebuild path uses, so the trees it
// records are the ones the write path would have produced; anything else
// would file objects under hashes no commit names.
func recordTreeObjectsForHistory(eng *engine.ServerEngine, r storage.DeltaSegmentReader) error {
	if r == nil {
		return nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return err
	}
	tree := document.EmptyDocumentTree()
	for _, seg := range segments {
		streamErr := streamSegmentCommits(r, seg, func(c document.Commit) error {
			next, puts, deletes, err := applyCommitToTree(tree, c)
			if err != nil {
				return err
			}
			if len(puts) > 0 || len(deletes) > 0 {
				eng.RecordTreeObject(tree, next, puts, deletes)
			}
			// The versions themselves, too: a tree object gets a read as
			// far as a content hash, and something has to hold the bytes.
			for _, op := range c.Operations {
				w, isWrite := op.(document.WriteOp)
				if !isWrite {
					continue
				}
				doc := documentFromPatch(w.DocID, w.Patch)
				h, err := doc.ContentHash()
				if err != nil {
					continue
				}
				eng.RecordDocumentObject(h, doc)
			}
			tree = next
			return nil
		})
		var corrupt *delta.CorruptFrameError
		if streamErr != nil && !errors.As(streamErr, &corrupt) {
			return streamErr
		}
	}
	return nil
}

// documentFromPatch mirrors how replay turns a WriteOp back into a
// document (see applyReplayedCommit): a patch that will not parse is still
// the document's bytes and is kept as-is, so the content hash computed
// from it matches the one the write path recorded.
func documentFromPatch(docID codec.UUID, patch string) document.Document {
	doc, err := document.FromJSONWithID(docID, patch)
	if err != nil {
		return document.Document{ID: docID, JSON: patch}
	}
	return doc
}
