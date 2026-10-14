package document

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Attachments: one binary attachment per document (docs/kdb-attachments-plan.md).
//
// A namespace's attachments at a commit are an AttachmentTable: for each document that has one,
// the AttachmentRef describing its bytes. The table is kept apart from the DocumentTree on
// purpose - an attachment never changes a document's JSON or its content hash - but it is hashed
// all the same, so peers comparing state can see an attachment change: its root is the commit's
// AttachmentTreeHash, next to DocumentTreeHash.
//
// The table reuses the document trie as its Merkle structure, mapping each document id to the
// hash of its AttachmentRef (AttachmentRef.Hash), so a commit that changes one attachment costs
// O(depth) to re-hash and shares every other node with its parent's table. The refs themselves
// sit in a pool keyed by that hash, shared by every version of the table.

// AttachmentRef describes one attachment: its bytes' content hash (SHA-256), length and media type.
type AttachmentRef struct {
	ContentHash codec.Hash
	Size        int64
	Mime        string
}

// Hash is the ref's identity in the table's Merkle tree: SHA-256 over a fixed encoding of its
// fields (domain byte 0x41 'A', the content hash, the size as 8 big-endian bytes, the mime's
// length and bytes). Kotlin and any other implementation must compute the same.
func (r AttachmentRef) Hash() codec.Hash {
	b := make([]byte, 0, 1+32+8+4+len(r.Mime))
	b = append(b, 'A')
	b = append(b, r.ContentHash.Bytes[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(r.Size))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Mime)))
	b = append(b, r.Mime...)
	h, _ := codec.HashFromBytes(SHA256Digest(b))
	return h
}

// AttachmentTable is a namespace's attachments at one commit. The zero value is the empty table.
// Tables are values: Put and Delete return a new table and leave the receiver as it was.
type AttachmentTable struct {
	tree DocumentTree // docID -> AttachmentRef.Hash; zero value until the first Put
	refs *refPool
}

// refPool holds every AttachmentRef any version of a table has used, by hash. It only grows;
// refs are tiny, and a namespace has as many as it has attachment versions.
type refPool struct {
	mu   sync.RWMutex
	refs map[codec.Hash]AttachmentRef
}

func (p *refPool) put(r AttachmentRef) codec.Hash {
	h := r.Hash()
	p.mu.Lock()
	p.refs[h] = r
	p.mu.Unlock()
	return h
}

func (p *refPool) get(h codec.Hash) (AttachmentRef, bool) {
	p.mu.RLock()
	r, ok := p.refs[h]
	p.mu.RUnlock()
	return r, ok
}

// EmptyAttachmentTable returns a table with no attachments.
func EmptyAttachmentTable() AttachmentTable {
	return AttachmentTable{tree: EmptyDocumentTree(), refs: &refPool{refs: map[codec.Hash]AttachmentRef{}}}
}

func (t AttachmentTable) init() AttachmentTable {
	if t.refs == nil {
		return EmptyAttachmentTable()
	}
	return t
}

// Len is the number of documents with an attachment.
func (t AttachmentTable) Len() int {
	if t.refs == nil {
		return 0
	}
	return t.tree.Size()
}

// Root is the commit's AttachmentTreeHash for this table: nil when it is empty, so a commit with
// no attachments carries no field and hashes as it always did.
func (t AttachmentTable) Root() *codec.Hash {
	if t.Len() == 0 {
		return nil
	}
	h := t.tree.TreeHash
	return &h
}

// Get returns docID's attachment.
func (t AttachmentTable) Get(docID codec.UUID) (AttachmentRef, bool) {
	if t.Len() == 0 {
		return AttachmentRef{}, false
	}
	h, ok := t.tree.HashFor(docID)
	if !ok {
		return AttachmentRef{}, false
	}
	return t.refs.get(h)
}

// Put returns the table with docID's attachment set to ref.
func (t AttachmentTable) Put(docID codec.UUID, ref AttachmentRef) (AttachmentTable, error) {
	t = t.init()
	h := t.refs.put(ref)
	tree, err := t.tree.With(docID, h)
	if err != nil {
		return t, err
	}
	return AttachmentTable{tree: tree, refs: t.refs}, nil
}

// Delete returns the table without docID's attachment. Deleting one that is not there is a no-op.
func (t AttachmentTable) Delete(docID codec.UUID) (AttachmentTable, error) {
	if t.Len() == 0 || !t.tree.Contains(docID) {
		return t, nil
	}
	tree, err := t.tree.Without(docID)
	if err != nil {
		return t, err
	}
	return AttachmentTable{tree: tree, refs: t.refs}, nil
}

// Walk visits every attachment, in no particular order, until visit returns false.
func (t AttachmentTable) Walk(visit func(docID codec.UUID, ref AttachmentRef) bool) {
	if t.Len() == 0 {
		return
	}
	t.tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		ref, ok := t.refs.get(h)
		if !ok {
			return true
		}
		return visit(id, ref)
	})
}

// Apply returns the table after ops: an AttachmentPutOp sets, an AttachmentDeleteOp removes, and a
// DeleteOp removes the deleted document's attachment with it. Other operations do not touch it.
// Within ops, order matters as written: a put after a delete of the same document leaves the put.
func (t AttachmentTable) Apply(ops []Op) (AttachmentTable, error) {
	var err error
	for _, op := range ops {
		switch o := op.(type) {
		case AttachmentPutOp:
			if o.Size < 0 {
				return t, fmt.Errorf("attachment for %s: negative size %d", o.DocID, o.Size)
			}
			t, err = t.Put(o.DocID, AttachmentRef{ContentHash: o.ContentHash, Size: o.Size, Mime: o.Mime})
		case AttachmentDeleteOp:
			t, err = t.Delete(o.DocID)
		case DeleteOp:
			t, err = t.Delete(o.DocID)
		}
		if err != nil {
			return t, err
		}
	}
	return t, nil
}

// TouchesAttachments reports whether applying ops could change a table that has entries: an
// attachment op, or a document delete (which removes the document's attachment).
func TouchesAttachments(ops []Op, tableNonEmpty bool) bool {
	for _, op := range ops {
		switch op.(type) {
		case AttachmentPutOp, AttachmentDeleteOp:
			return true
		case DeleteOp:
			if tableNonEmpty {
				return true
			}
		}
	}
	return false
}
