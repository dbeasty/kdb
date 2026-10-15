package server

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/storage"
)

// catchUpTreeNodeEntries is the subtree size at which DiffTrees compares entries directly.
const catchUpTreeNodeEntries = 64

// catchUp brings the stores restored from snapshots behind the DAG head up to it, by applying
// only the documents that differ between the snapshot's tree and the head's (qtask production plan
// U5). It returns the ids it could not catch up - an unresolvable tree, a failed read - which
// the caller rebuilds from scan as before, after clearing them.
//
// Holds persistMu and applyMu like a rebuild: no commit reaches the stores, and no snapshot is
// captured, while they are between two commits' states.
func (p *RegistryIndexProvider) catchUp(behind []index.BehindStore) (failed []codec.UUID) {
	p.persistMu.Lock()
	defer p.persistMu.Unlock()
	p.applyMu.Lock()
	defer p.applyMu.Unlock()

	head, err := p.runtime.Runtime.DAG.Head()
	if err != nil {
		return clearAll(p, behind)
	}
	byFrom := map[codec.Hash][]codec.UUID{}
	var order []codec.Hash
	for _, b := range behind {
		if _, seen := byFrom[b.Head]; !seen {
			order = append(order, b.Head)
		}
		byFrom[b.Head] = append(byFrom[b.Head], b.Descriptor.IndexID)
	}
	for _, from := range order {
		ids := byFrom[from]
		start := time.Now()
		n, err := p.catchUpFrom(from, head, ids)
		if err != nil {
			slog.Warn("index catch-up failed; rebuilding from the documents", "namespace", p.registry.NamespaceID(), "indexes", len(ids), "error", err)
			for _, id := range ids {
				if st, ok := p.registry.ByID(id); ok {
					_ = st.Clear()
				}
			}
			failed = append(failed, ids...)
			continue
		}
		p.caughtUpDocs.Add(int64(n))
		slog.Info("indexes caught up from their snapshot", "namespace", p.registry.NamespaceID(), "indexes", len(ids), "documents", n, "took", time.Since(start).Round(time.Millisecond))
	}
	return failed
}

func clearAll(p *RegistryIndexProvider, behind []index.BehindStore) []codec.UUID {
	out := make([]codec.UUID, 0, len(behind))
	for _, b := range behind {
		if st, ok := p.registry.ByID(b.Descriptor.IndexID); ok {
			_ = st.Clear()
		}
		out = append(out, b.Descriptor.IndexID)
	}
	return out
}

// catchUpFrom applies to ids the documents that differ between commit from's tree and head's.
func (p *RegistryIndexProvider) catchUpFrom(from, head codec.Hash, ids []codec.UUID) (int, error) {
	rt := p.runtime.Runtime
	resolver, ok := rt.Storage.(storage.TreeResolver)
	if !ok {
		return 0, fmt.Errorf("storage cannot resolve trees")
	}
	fromCommit, ok := rt.DAG.GetCommit(from)
	if !ok {
		return 0, fmt.Errorf("snapshot commit %s not in the DAG", from.Hex())
	}
	headCommit, ok := rt.DAG.GetCommit(head)
	if !ok {
		return 0, fmt.Errorf("head commit %s not in the DAG", head.Hex())
	}
	fromTree, ok, err := resolver.TreeAt(fromCommit.DocumentTreeHash)
	if err != nil || !ok {
		return 0, fmt.Errorf("snapshot tree %s unresolvable: %v", fromCommit.DocumentTreeHash.Hex(), err)
	}
	headTree, ok, err := resolver.TreeAt(headCommit.DocumentTreeHash)
	if err != nil || !ok {
		return 0, fmt.Errorf("head tree %s unresolvable: %v", headCommit.DocumentTreeHash.Hex(), err)
	}
	diffs, err := document.DiffTrees(document.LocalNodes(fromTree, catchUpTreeNodeEntries), document.LocalNodes(headTree, catchUpTreeNodeEntries))
	if err != nil {
		return 0, err
	}
	ns := rt.DefaultNamespace
	stats, err := p.registry.CatchUpStores(ids, head, func(yield func(index.DocumentChange) bool) error {
		for _, d := range diffs {
			if d.Remote == (codec.Hash{}) {
				if !yield(index.DocumentChange{DocID: d.DocID, Deleted: true}) {
					return nil
				}
				continue
			}
			doc, err := rt.Storage.GetDocument(ns, d.DocID, headCommit.DocumentTreeHash)
			if err != nil {
				return err
			}
			if doc == nil {
				return fmt.Errorf("document %s named by the head tree is unreadable", d.DocID)
			}
			if !yield(index.DocumentChange{DocID: d.DocID, JSONText: doc.JSON}) {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return stats.Documents, nil
}

// catchUpCounter counts the documents catch-ups applied (tests).
type catchUpCounter struct{ caughtUpDocs atomic.Int64 }
