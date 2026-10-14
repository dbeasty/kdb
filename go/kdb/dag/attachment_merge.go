package dag

import (
	"bytes"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Merging attachment tables.
//
// A commit's attachment table is its first parent's with its operations applied, so a merge
// commit must carry the attachment operations that turn its first parent's table into the merged
// one - just as it carries the document writes that turn the first parent's tree into the merged
// tree. Every merge builder (peer sync's convergent merges, the transaction engine's merge marker)
// goes through appendCommitLocked, which adds them here: whoever builds the merge, and on whichever
// node, the same parents give the same operations and so the same commit hash.
//
// The merge is three-way, per attachment ID, against the parents' merge base (the first by hash
// when there are several; an empty table when there is none):
//   - a side that left an attachment as the base had it takes the other side's version (changed,
//     added, or removed);
//   - both sides changing it differently is a conflict: a version beats a removal (no bytes are
//     lost), and between two versions the greater (content hash, name, size, mime) wins - arbitrary,
//     but the same on every node;
//   - two attachments of a document ending with the same name keep it on the smaller ID; the other
//     is renamed "<name>~<first 8 hex of its ID>".
//
// A document absent from the merged document tree keeps no attachments. A document the merge's
// own operations already change attachments of (the builder decided) is left to them.

// mergeAttachmentOpsLocked returns the attachment operations a two-parent commit needs beyond its
// own. Must hold mu.
func (d *InMemoryCommitDag) mergeAttachmentOpsLocked(tx document.Transaction, parents []codec.Hash, tree document.DocumentTree) ([]document.Op, error) {
	if len(parents) != 2 || tx.Attachments != nil || !d.sawAttachments {
		return nil, nil
	}
	ours, err := d.attachmentTableLocked(parents[0])
	if err != nil {
		return nil, err
	}
	theirs, err := d.attachmentTableLocked(parents[1])
	if err != nil {
		return nil, err
	}
	if sameRoot(ours.Root(), theirs.Root()) {
		return nil, nil
	}
	base := document.EmptyAttachmentTable()
	if bases := d.mergeBasesLocked([]codec.Hash{parents[0]}, []codec.Hash{parents[1]}); len(bases) > 0 {
		if base, err = d.attachmentTableLocked(bases[0]); err != nil {
			return nil, err
		}
	}
	decided := map[codec.UUID]bool{}
	for _, op := range tx.Operations {
		switch o := op.(type) {
		case document.AttachmentPutOp:
			decided[o.DocID] = true
		case document.AttachmentDeleteOp:
			decided[o.DocID] = true
		}
	}
	// The documents whose sets differ between the parents.
	differ := map[codec.UUID]bool{}
	theirSets := map[codec.UUID]codec.Hash{}
	theirs.WalkDocuments(func(id codec.UUID, h codec.Hash) bool {
		theirSets[id] = h
		return true
	})
	ours.WalkDocuments(func(id codec.UUID, h codec.Hash) bool {
		if th, ok := theirSets[id]; !ok || th != h {
			differ[id] = true
		}
		delete(theirSets, id)
		return true
	})
	for id := range theirSets {
		differ[id] = true
	}
	docs := make([]codec.UUID, 0, len(differ))
	for id := range differ {
		if !decided[id] {
			docs = append(docs, id)
		}
	}
	sort.Slice(docs, func(i, j int) bool { return uuidLess(docs[i], docs[j]) })

	var ops []document.Op
	for _, doc := range docs {
		var merged document.AttachmentSet
		if tree.Contains(doc) {
			merged = mergeAttachmentSets(base.List(doc), ours.List(doc), theirs.List(doc))
		}
		ops = append(ops, attachmentSetOps(doc, ours.List(doc), merged)...)
	}
	return ops, nil
}

func sameRoot(a, b *codec.Hash) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func uuidLess(a, b codec.UUID) bool {
	if a.MSB != b.MSB {
		return uint64(a.MSB) < uint64(b.MSB)
	}
	return uint64(a.LSB) < uint64(b.LSB)
}

func byID(set document.AttachmentSet) map[codec.UUID]document.Attachment {
	m := make(map[codec.UUID]document.Attachment, len(set))
	for _, a := range set {
		m[a.ID] = a
	}
	return m
}

// attachmentGreater orders two conflicting versions deterministically.
func attachmentGreater(a, b document.Attachment) bool {
	if c := bytes.Compare(a.ContentHash.Bytes[:], b.ContentHash.Bytes[:]); c != 0 {
		return c > 0
	}
	if a.Name != b.Name {
		return a.Name > b.Name
	}
	if a.Size != b.Size {
		return a.Size > b.Size
	}
	return a.Mime > b.Mime
}

// mergeAttachmentSets merges one document's attachments three-way.
func mergeAttachmentSets(base, ours, theirs document.AttachmentSet) document.AttachmentSet {
	b, o, t := byID(base), byID(ours), byID(theirs)
	ids := map[codec.UUID]bool{}
	for _, m := range []map[codec.UUID]document.Attachment{b, o, t} {
		for id := range m {
			ids[id] = true
		}
	}
	var out []document.Attachment
	for id := range ids {
		bv, inB := b[id]
		ov, inO := o[id]
		tv, inT := t[id]
		var keep document.Attachment
		var present bool
		switch {
		case inO == inT && (!inO || ov == tv):
			keep, present = ov, inO
		case inO == inB && (!inO || ov == bv):
			keep, present = tv, inT // ours left it alone: theirs decides
		case inT == inB && (!inT || tv == bv):
			keep, present = ov, inO
		case !inO:
			keep, present = tv, true // a version beats a removal
		case !inT:
			keep, present = ov, true
		case attachmentGreater(ov, tv):
			keep, present = ov, true
		default:
			keep, present = tv, true
		}
		if present {
			out = append(out, keep)
		}
	}
	// Names unique: the smaller ID keeps a contested name.
	sort.Slice(out, func(i, j int) bool { return uuidLess(out[i].ID, out[j].ID) })
	taken := map[string]bool{}
	for i := range out {
		if taken[out[i].Name] {
			suffix := "~" + out[i].ID.String()[:8]
			name := out[i].Name
			if len(name)+len(suffix) > document.MaxAttachmentNameBytes {
				name = name[:document.MaxAttachmentNameBytes-len(suffix)]
			}
			out[i].Name = name + suffix
		}
		taken[out[i].Name] = true
	}
	return out
}

// attachmentSetOps are the operations that turn doc's attachments from before into after: removals
// and renames first (as deletes), so no put lands on a name still taken, then the puts.
func attachmentSetOps(doc codec.UUID, before, after document.AttachmentSet) []document.Op {
	prev, next := byID(before), byID(after)
	var dels, puts []document.Op
	for _, a := range before {
		n, ok := next[a.ID]
		if !ok || n.Name != a.Name {
			dels = append(dels, document.AttachmentDeleteOp{DocID: doc, AttachmentID: a.ID})
		}
	}
	sorted := append(document.AttachmentSet(nil), after...)
	sort.Slice(sorted, func(i, j int) bool { return uuidLess(sorted[i].ID, sorted[j].ID) })
	for _, a := range sorted {
		if p, ok := prev[a.ID]; ok && p == a {
			continue
		}
		puts = append(puts, document.AttachmentPutOp{DocID: doc, AttachmentID: a.ID, Name: a.Name,
			Scheme: a.Scheme, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime})
	}
	return append(dels, puts...)
}
