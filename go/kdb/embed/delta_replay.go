package embed

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

func replayDeltaNamespace(d *dag.InMemoryCommitDag, store storage.Adapter, r storage.DeltaSegmentReader) error {
	if r == nil {
		return nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return err
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].SegmentID.String() < segments[j].SegmentID.String()
	})

	var commits []document.Commit
	for _, seg := range segments {
		records, err := r.ReadAll(seg)
		if err != nil {
			return err
		}
		for _, rec := range records {
			c, err := document.FromPayloadBytes(rec.CommitPayload)
			if err != nil {
				return err
			}
			commits = append(commits, c)
		}
	}

	for _, c := range commits {
		if d.HasCommit(c.Hash) {
			continue
		}
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				doc, err := document.FromJSONWithID(o.DocID, o.Patch)
				if err != nil {
					doc = document.Document{ID: o.DocID, JSON: o.Patch}
				}
				if err := store.PutDocument(d.NamespaceID, doc); err != nil {
					return err
				}
			case document.DeleteOp:
				if err := store.DeleteDocument(d.NamespaceID, o.DocID); err != nil {
					return err
				}
			default:
				// ignore v1 ops not yet ported
			}
		}
		parentTree := document.EmptyDocumentTree().TreeHash
		if len(c.ParentHashes) > 0 {
			parentTree = c.ParentHashes[0]
		}
		tree, err := store.CommitTree(d.NamespaceID, parentTree)
		if err != nil {
			return err
		}
		d.PutDocumentTree(tree)
		if err := d.PutCommit(c, true); err != nil {
			return err
		}
		if err := d.SetHead("main", c.Hash); err != nil {
			return err
		}
	}
	return nil
}

