package engine

import (
	"unsafe"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Snapshot bootstrap, both ends (peersync.InstallSnapshot and snapshotPage). A snapshot is the
// whole namespace, so anything either end does per document that the ordinary paths do per
// version - admit it to docsByHash, pin it until a log write that never comes, hold it for a
// final pass - is memory proportional to the namespace. These two entry points do the same work
// as GetDocuments and PutDocument+CommitTree without any of that.

// ReadDocumentsThrough implements storage.ThroughReader: GetDocuments at the tree treeHash, except
// that a version read from durable storage is not admitted to docsByHash. A snapshot page reads
// each document once; admitting it would only evict something a later read wants, and churn the
// arena's chunks while doing it. A version already resident is served from there as usual.
func (e *ServerEngine) ReadDocumentsThrough(namespaceID string, docIDs []codec.UUID, treeHash codec.Hash) ([]*document.Document, error) {
	out := make([]*document.Document, len(docIDs))
	tree, ok, err := e.treeAt(treeHash)
	if err != nil || !ok {
		return out, err
	}
	for i, id := range docIDs {
		h, ok := tree.HashFor(id)
		if !ok {
			continue
		}
		if d, ok := e.docsByHash.Get(h); ok {
			out[i] = &d
			continue
		}
		if obj, found := e.documentFromObject(id, h); found {
			e.coldLoads.Add(1)
			out[i] = &obj
			continue
		}
		d, found, err := e.readCold(id, h)
		if err != nil {
			return nil, err
		}
		if found {
			out[i] = &d
		}
	}
	return out, nil
}

// ApplySnapshotBatch implements storage.SnapshotApplier: PutDocument for each of docs then
// CommitTree, for a namespace being bootstrapped from a peer's snapshot, without the costs that
// grow with the namespace:
//
//   - Each body is written to the blob store as it arrives (the memtable, flushed at its usual
//     budget), where MaterializeLiveBodies would otherwise have to find it at the end. The bodies
//     came with no delta log behind them, so the blob store is their only durable home anyway.
//   - Under bounded live bodies a body is neither admitted to docsByHash nor pinned. CommitTree
//     pins each new version until NoteCommitDurable reports its commit logged - which for a
//     snapshot batch never happens, so every body of the namespace stayed pinned for the life of
//     the process. The blob store serves them instead (bodiesExternal is set here, not later).
//     Unbounded, docsByHash holds and pins the live tree as always.
//   - No document locations are stashed: there is no log position to file them under.
//
// The tree is the one CommitTree would build (DocumentTree.WithAll builds With's tree, hashing
// each node once per batch; a tree object per batch), so the tree hash a caller checks against
// the snapshot's commit means the same thing.
//
// An engine with no durable blob store (an in-memory target) takes the ordinary path.
func (e *ServerEngine) ApplySnapshotBatch(namespaceID string, docs []document.Document) (document.DocumentTree, error) {
	if e.memTable == nil || e.config.IOShim == nil {
		for _, d := range docs {
			e.pending.Put(d)
		}
		return e.CommitTree(namespaceID, codec.Hash{})
	}
	e.bodiesExternal.Store(true)
	tree, err := e.applySnapshotBatchLocked(docs)
	if err != nil {
		return tree, err
	}
	e.maybeFlushMemtable()
	return tree, nil
}

func (e *ServerEngine) applySnapshotBatchLocked(docs []document.Document) (document.DocumentTree, error) {
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	bounded := e.boundedLiveBodies()
	// The bodies of this tree are all in the blob store only if every batch since the empty tree
	// came through here; see MaterializeLiveBodies.
	chained := e.tree.Size() == 0 || (e.snapshotBodiesSet && e.tree.TreeHash == e.snapshotBodies)
	e.snapshotBodiesSet = false
	base := e.tree
	// A batch of distinct ids in order - what a snapshot sends - is added to the tree in one pass,
	// each node it touches hashed once rather than once per document: with every insert rehashing
	// its path to the root, the tree was most of an install's time at 10M documents. Anything
	// else is added a document at a time, as CommitTree would.
	ascending := true
	for i := 1; i < len(docs) && ascending; i++ {
		ascending = idAfter(docs[i].ID, docs[i-1].ID)
	}
	changed := make([]TreeChange, 0, len(docs))
	var entries []document.TreeEntry
	if ascending {
		entries = make([]document.TreeEntry, 0, len(docs))
	}
	for _, doc := range docs {
		h := hasher.Hash(doc.ID, doc.JSON)
		changed = append(changed, TreeChange{DocID: doc.ID, ContentHash: h})
		// Put copies the value into the memtable's arena, so the body's own bytes can be lent.
		e.memTable.Put(h, unsafe.Slice(unsafe.StringData(doc.JSON), len(doc.JSON)))
		if !bounded {
			// CommitTree's pin discipline, unbounded: the live tree's versions stay pinned.
			// Distinct ids read the same previous version from the batch's base as from the
			// tree with the batch's earlier documents in it.
			prev, hadPrev := e.tree.HashFor(doc.ID)
			if !hadPrev || prev != h {
				e.docsByHash.Pin(h)
			}
			e.docsByHash.Put(h, doc)
			if hadPrev && prev != h {
				e.docsByHash.Unpin(prev)
			}
		}
		if ascending {
			entries = append(entries, document.TreeEntry{DocID: doc.ID, ContentHash: h})
			continue
		}
		var err error
		if e.tree, err = e.tree.With(doc.ID, h); err != nil {
			return document.DocumentTree{}, err
		}
	}
	if ascending {
		var err error
		if e.tree, err = e.tree.WithAll(entries); err != nil {
			return document.DocumentTree{}, err
		}
	}
	if len(changed) > 0 {
		e.putTreeObject(base, e.tree, changed, nil)
	}
	if chained {
		e.snapshotBodies, e.snapshotBodiesSet = e.tree.TreeHash, true
	}
	e.treesMu.Lock()
	e.publishTreeLocked(e.tree, len(changed))
	e.treesMu.Unlock()
	return e.tree, nil
}

// idAfter reports whether a sorts after b in a tree's order (the order of their bytes).
func idAfter(a, b codec.UUID) bool {
	if a.MSB != b.MSB {
		return uint64(a.MSB) > uint64(b.MSB)
	}
	return uint64(a.LSB) > uint64(b.LSB)
}

// snapshotBodiesCover reports whether every version tree names is already in the blob store
// because the tree was built from empty by ApplySnapshotBatch alone.
func (e *ServerEngine) snapshotBodiesCover(tree document.DocumentTree) bool {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	return e.snapshotBodiesSet && e.snapshotBodies == tree.TreeHash
}

// StreamTreeBodies implements storage.TreeBodyStreamer: it reads the delta log front to back, one
// frame at a time, and hands fn every write whose version is the one tree names for its document.
// A version written more than once is handed over more than once; one tree does not name (an
// overwritten or deleted version) is skipped; one the log does not hold - its segment truncated,
// or a body that only ever reached the blob store - is simply never handed over.
//
// This is what makes serving a large snapshot cheap. Read in id order, every document is a random
// read: a location record out of the blob store and a frame out of the log, each a disk read
// whenever the namespace is larger than the caches, which at a million documents is most of what
// a bootstrap cost its host. In log order every frame is read once, sequentially.
func (e *ServerEngine) StreamTreeBodies(namespaceID string, tree document.DocumentTree, fn func(docID codec.UUID, body string) error) error {
	if e.deltaLoader == nil {
		return storage.ErrTreeBodiesUnsupported
	}
	streamer, ok := e.deltaLoader.reader.(storage.DeltaCommitStreamer)
	if !ok {
		return storage.ErrTreeBodiesUnsupported
	}
	segments, err := e.deltaLoader.reader.ListSegments()
	if err != nil {
		return err
	}
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	for _, seg := range segments {
		err := streamer.StreamCommits(seg, func(c document.Commit, _ int64) error {
			for _, op := range c.Operations {
				w, isWrite := op.(document.WriteOp)
				if !isWrite {
					continue
				}
				want, ok := tree.HashFor(w.DocID)
				if !ok || hasher.Hash(w.DocID, w.Patch) != want {
					continue
				}
				if err := fn(w.DocID, w.Patch); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
