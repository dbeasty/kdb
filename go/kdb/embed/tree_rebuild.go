package embed

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// rebuildHistoricalTrees replays the whole delta log for its *tree* state
// alone - which document each commit pointed at, by content hash - and
// registers every tree it passes through, so a read at a historical commit
// can resolve after a checkpoint restore left only the live tree resident.
//
// This is the expensive path the checkpoint exists to avoid, and it is
// deliberately still here: it runs at most once per process, only when
// something actually reads at a historical commit, and a namespace only
// ever read at its head never runs it at all. Trading a rare slow read for
// an open that does not touch history is the whole point.
//
// The tree it builds has to match what the engine built commit by commit,
// or the tree hashes will not match the ones commits name and the lookup
// will miss. Two details carry that: within one commit, deletes are
// applied before puts, and a document written and deleted in the same
// commit resolves last-operation-wins - both mirroring
// shardedPendingStore's staging and CommitTree's flush order.
func rebuildHistoricalTrees(eng *engine.ServerEngine, r storage.DeltaSegmentReader) error {
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
			next, err := applyCommitToTree(tree, c)
			if err != nil {
				return err
			}
			tree = next
			eng.RegisterHistoricalTree(tree)
			return nil
		})
		var corrupt *delta.CorruptFrameError
		if streamErr != nil && !errors.As(streamErr, &corrupt) {
			return streamErr
		}
	}
	return nil
}

// applyCommitToTree folds one commit's operations into tree, reproducing
// the staging semantics the write path used. Documents are hashed from
// their text, which is the reason this is expensive: the content hash a
// tree stores cannot be recovered from the log any other way.
func applyCommitToTree(tree document.DocumentTree, c document.Commit) (document.DocumentTree, error) {
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
	for id := range deletes {
		if out, err = out.Without(id); err != nil {
			return document.DocumentTree{}, err
		}
	}
	for _, doc := range puts {
		h, err := doc.ContentHash()
		if err != nil {
			return document.DocumentTree{}, err
		}
		if out, err = out.With(doc.ID, h); err != nil {
			return document.DocumentTree{}, err
		}
	}
	return out, nil
}
