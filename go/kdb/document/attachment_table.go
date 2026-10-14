package document

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Attachments: binary attachments on a document (docs/kdb-attachments-plan.md).
//
// A document can have any number of attachments. Each has an ID - a UUID assigned when it is
// created, unique and never changed - and a name, unique within the document, which defaults to
// the ID's string form. Either one references it: a reference by name is resolved to the ID when a
// change is staged, so operations and the table always speak IDs. Writing to an existing ID
// replaces that attachment's bytes (and may rename it); the document's other attachments are left
// alone.
//
// A namespace's attachments at a commit are an AttachmentTable. It is kept apart from the
// DocumentTree on purpose - an attachment never changes a document's JSON or its content hash -
// but it is hashed all the same, so peers comparing state can see an attachment change: its root
// is the commit's AttachmentTreeHash, next to DocumentTreeHash.
//
// The table reuses the document trie as its Merkle structure, mapping each document id to the
// hash of the document's attachment set (AttachmentSet.Hash). Changing one attachment re-hashes
// that document's set and O(depth) trie nodes, and shares every other node with the parent's
// table. Sets sit in a pool keyed by that hash, shared by every version of the table.

// MaxAttachmentNameBytes bounds an attachment name.
const MaxAttachmentNameBytes = 255

// ValidateAttachmentName checks a name: 1 to MaxAttachmentNameBytes bytes of UTF-8 with no
// control characters.
func ValidateAttachmentName(name string) error {
	if name == "" {
		return fmt.Errorf("attachment name is empty")
	}
	if len(name) > MaxAttachmentNameBytes {
		return fmt.Errorf("attachment name is %d bytes, over %d", len(name), MaxAttachmentNameBytes)
	}
	if !utf8.ValidString(name) {
		return fmt.Errorf("attachment name is not valid UTF-8")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("attachment name contains a control character")
		}
	}
	return nil
}

// AttachmentRef describes one attachment's bytes: how its content hash is computed (Scheme - see
// attachment_hash.go), the content hash itself, its length and its media type.
type AttachmentRef struct {
	Scheme      byte
	ContentHash codec.Hash
	Size        int64
	Mime        string
}

// Hash is the ref's identity: SHA-256 over a fixed encoding of its fields (domain byte 0x41 'A',
// the scheme byte, the content hash, the size as 8 big-endian bytes, the mime's length and bytes).
// Kotlin and any other implementation must compute the same.
func (r AttachmentRef) Hash() codec.Hash {
	b := make([]byte, 0, 2+32+8+4+len(r.Mime))
	b = append(b, 'A', r.Scheme)
	b = append(b, r.ContentHash.Bytes[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(r.Size))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Mime)))
	b = append(b, r.Mime...)
	h, _ := codec.HashFromBytes(SHA256Digest(b))
	return h
}

// Attachment is one attachment of a document: its permanent ID, its name and its bytes' ref.
type Attachment struct {
	ID   codec.UUID
	Name string
	AttachmentRef
}

// AttachmentSet is one document's attachments, sorted by ID. Values: never modify one in place.
type AttachmentSet []Attachment

// Hash is the set's identity in the table's Merkle tree: SHA-256 of domain byte 0x53 'S' then, for
// each attachment in ID order, the ID's 16 big-endian bytes, the name's length (be32), the name,
// and the ref's hash.
func (s AttachmentSet) Hash() codec.Hash {
	b := make([]byte, 0, 1+len(s)*(16+4+16+32))
	b = append(b, 'S')
	for _, a := range s {
		b = binary.BigEndian.AppendUint64(b, uint64(a.ID.MSB))
		b = binary.BigEndian.AppendUint64(b, uint64(a.ID.LSB))
		b = binary.BigEndian.AppendUint32(b, uint32(len(a.Name)))
		b = append(b, a.Name...)
		rh := a.AttachmentRef.Hash()
		b = append(b, rh.Bytes[:]...)
	}
	h, _ := codec.HashFromBytes(SHA256Digest(b))
	return h
}

func uuidLess(a, b codec.UUID) bool {
	if a.MSB != b.MSB {
		return uint64(a.MSB) < uint64(b.MSB)
	}
	return uint64(a.LSB) < uint64(b.LSB)
}

func (s AttachmentSet) find(id codec.UUID) (int, bool) {
	i := sort.Search(len(s), func(i int) bool { return !uuidLess(s[i].ID, id) })
	return i, i < len(s) && s[i].ID == id
}

// ByName returns the attachment called name.
func (s AttachmentSet) ByName(name string) (Attachment, bool) {
	for _, a := range s {
		if a.Name == name {
			return a, true
		}
	}
	return Attachment{}, false
}

// with returns s with a set, replacing the attachment of a.ID. A name another attachment already
// has is an error: names are unique within a document.
func (s AttachmentSet) with(a Attachment) (AttachmentSet, error) {
	if other, ok := s.ByName(a.Name); ok && other.ID != a.ID {
		return s, fmt.Errorf("attachment name %q is already used by attachment %s", a.Name, other.ID)
	}
	i, found := s.find(a.ID)
	out := make(AttachmentSet, 0, len(s)+1)
	out = append(out, s[:i]...)
	out = append(out, a)
	if found {
		i++
	}
	return append(out, s[i:]...), nil
}

// without returns s without id, and whether it was there.
func (s AttachmentSet) without(id codec.UUID) (AttachmentSet, bool) {
	i, found := s.find(id)
	if !found {
		return s, false
	}
	out := make(AttachmentSet, 0, len(s)-1)
	out = append(out, s[:i]...)
	return append(out, s[i+1:]...), true
}

// AttachmentTable is a namespace's attachments at one commit. The zero value is the empty table.
// Tables are values: Put and Delete return a new table and leave the receiver as it was.
type AttachmentTable struct {
	tree  DocumentTree // docID -> AttachmentSet.Hash
	sets  *setPool
	total int // attachments across all documents
}

// setPool holds every AttachmentSet any version of a table has used, by hash. It only grows.
type setPool struct {
	mu   sync.RWMutex
	sets map[codec.Hash]AttachmentSet
}

func (p *setPool) put(s AttachmentSet) codec.Hash {
	h := s.Hash()
	p.mu.Lock()
	p.sets[h] = s
	p.mu.Unlock()
	return h
}

func (p *setPool) get(h codec.Hash) (AttachmentSet, bool) {
	p.mu.RLock()
	s, ok := p.sets[h]
	p.mu.RUnlock()
	return s, ok
}

// EmptyAttachmentTable returns a table with no attachments.
func EmptyAttachmentTable() AttachmentTable {
	return AttachmentTable{tree: EmptyDocumentTree(), sets: &setPool{sets: map[codec.Hash]AttachmentSet{}}}
}

func (t AttachmentTable) init() AttachmentTable {
	if t.sets == nil {
		return EmptyAttachmentTable()
	}
	return t
}

// Len is the number of attachments, across all documents.
func (t AttachmentTable) Len() int { return t.total }

// Documents is the number of documents with at least one attachment.
func (t AttachmentTable) Documents() int {
	if t.sets == nil {
		return 0
	}
	return t.tree.Size()
}

// Root is the commit's AttachmentTreeHash for this table: nil when it is empty, so a commit with
// no attachments carries no field and hashes as it always did.
func (t AttachmentTable) Root() *codec.Hash {
	if t.Documents() == 0 {
		return nil
	}
	h := t.tree.TreeHash
	return &h
}

// List returns docID's attachments, sorted by name; nil when it has none.
func (t AttachmentTable) List(docID codec.UUID) AttachmentSet {
	if t.Documents() == 0 {
		return nil
	}
	h, ok := t.tree.HashFor(docID)
	if !ok {
		return nil
	}
	s, _ := t.sets.get(h)
	return s
}

// Count is how many attachments docID has.
func (t AttachmentTable) Count(docID codec.UUID) int { return len(t.List(docID)) }

// Get returns docID's attachment id.
func (t AttachmentTable) Get(docID, id codec.UUID) (Attachment, bool) {
	s := t.List(docID)
	if i, ok := s.find(id); ok {
		return s[i], true
	}
	return Attachment{}, false
}

// GetByName returns docID's attachment called name.
func (t AttachmentTable) GetByName(docID codec.UUID, name string) (Attachment, bool) {
	return t.List(docID).ByName(name)
}

func (t AttachmentTable) setDoc(docID codec.UUID, before, after AttachmentSet) (AttachmentTable, error) {
	var tree DocumentTree
	var err error
	if len(after) == 0 {
		tree, err = t.tree.Without(docID)
	} else {
		tree, err = t.tree.With(docID, t.sets.put(after))
	}
	if err != nil {
		return t, err
	}
	return AttachmentTable{tree: tree, sets: t.sets, total: t.total - len(before) + len(after)}, nil
}

// Put returns the table with docID's attachment a.ID set to a - created, or replaced (bytes and
// name) - leaving the document's others.
func (t AttachmentTable) Put(docID codec.UUID, a Attachment) (AttachmentTable, error) {
	if a.ID == (codec.UUID{}) {
		return t, fmt.Errorf("attachment of %s has no id", docID)
	}
	if err := ValidateAttachmentName(a.Name); err != nil {
		return t, err
	}
	if a.Size < 0 {
		return t, fmt.Errorf("attachment %s of %s: negative size %d", a.ID, docID, a.Size)
	}
	if a.Scheme != AttachmentHashSchemeChunked1 {
		return t, fmt.Errorf("attachment %s of %s: unknown content hash scheme %d", a.ID, docID, a.Scheme)
	}
	t = t.init()
	before := t.List(docID)
	after, err := before.with(a)
	if err != nil {
		return t, err
	}
	return t.setDoc(docID, before, after)
}

// Delete returns the table without docID's attachment id. Deleting one that is not there is a
// no-op.
func (t AttachmentTable) Delete(docID, id codec.UUID) (AttachmentTable, error) {
	before := t.List(docID)
	after, found := before.without(id)
	if !found {
		return t, nil
	}
	return t.setDoc(docID, before, after)
}

// DeleteAll returns the table without any of docID's attachments.
func (t AttachmentTable) DeleteAll(docID codec.UUID) (AttachmentTable, error) {
	before := t.List(docID)
	if len(before) == 0 {
		return t, nil
	}
	return t.setDoc(docID, before, nil)
}

// WalkDocuments visits every document with attachments and the hash of its set, until visit
// returns false.
func (t AttachmentTable) WalkDocuments(visit func(docID codec.UUID, setHash codec.Hash) bool) {
	if t.Documents() == 0 {
		return
	}
	t.tree.Walk(visit)
}

// SetHash is the hash of docID's attachment set; ok=false when it has none.
func (t AttachmentTable) SetHash(docID codec.UUID) (codec.Hash, bool) {
	if t.Documents() == 0 {
		return codec.Hash{}, false
	}
	return t.tree.HashFor(docID)
}

// Walk visits every attachment, documents in no particular order and each document's in ID
// order, until visit returns false.
func (t AttachmentTable) Walk(visit func(docID codec.UUID, a Attachment) bool) {
	if t.Documents() == 0 {
		return
	}
	t.tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		s, ok := t.sets.get(h)
		if !ok {
			return true
		}
		for _, a := range s {
			if !visit(id, a) {
				return false
			}
		}
		return true
	})
}

// Apply returns the table after ops: an AttachmentPutOp creates or replaces one attachment, an
// AttachmentDeleteOp removes one, and a DeleteOp removes all of the deleted document's. Other
// operations do not touch it. Order matters as written: a put after a delete of the same document
// leaves the put.
func (t AttachmentTable) Apply(ops []Op) (AttachmentTable, error) {
	var err error
	for _, op := range ops {
		switch o := op.(type) {
		case AttachmentPutOp:
			t, err = t.Put(o.DocID, Attachment{ID: o.AttachmentID, Name: o.Name,
				AttachmentRef: AttachmentRef{Scheme: o.Scheme, ContentHash: o.ContentHash, Size: o.Size, Mime: o.Mime}})
		case AttachmentDeleteOp:
			t, err = t.Delete(o.DocID, o.AttachmentID)
		case DeleteOp:
			t, err = t.DeleteAll(o.DocID)
		}
		if err != nil {
			return t, err
		}
	}
	return t, nil
}

// TouchesAttachments reports whether applying ops could change a table that has entries: an
// attachment op, or a document delete (which removes the document's attachments).
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

// AppendPackedAttachmentTables packs tables for a checkpoint: a uvarint count, then per table a
// uvarint attachment count and its attachments (doc id 16 | attachment id 16 | name length
// uvarint | name | scheme 1 | content hash 32 | size u64 big-endian | mime length uvarint | mime).
// Empty tables are skipped.
func AppendPackedAttachmentTables(dst []byte, tables []AttachmentTable) []byte {
	n := 0
	for _, t := range tables {
		if t.Len() > 0 {
			n++
		}
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	for _, t := range tables {
		if t.Len() == 0 {
			continue
		}
		dst = binary.AppendUvarint(dst, uint64(t.Len()))
		t.Walk(func(id codec.UUID, a Attachment) bool {
			dst = binary.BigEndian.AppendUint64(dst, uint64(id.MSB))
			dst = binary.BigEndian.AppendUint64(dst, uint64(id.LSB))
			dst = binary.BigEndian.AppendUint64(dst, uint64(a.ID.MSB))
			dst = binary.BigEndian.AppendUint64(dst, uint64(a.ID.LSB))
			dst = binary.AppendUvarint(dst, uint64(len(a.Name)))
			dst = append(dst, a.Name...)
			dst = append(dst, a.Scheme)
			dst = append(dst, a.ContentHash.Bytes[:]...)
			dst = binary.BigEndian.AppendUint64(dst, uint64(a.Size))
			dst = binary.AppendUvarint(dst, uint64(len(a.Mime)))
			dst = append(dst, a.Mime...)
			return true
		})
	}
	return dst
}

// ParsePackedAttachmentTables reverses AppendPackedAttachmentTables.
func ParsePackedAttachmentTables(packed []byte) ([]AttachmentTable, error) {
	if len(packed) == 0 {
		return nil, nil
	}
	off := 0
	uv := func() (uint64, error) {
		v, n := binary.Uvarint(packed[off:])
		if n <= 0 {
			return 0, fmt.Errorf("attachment tables: malformed count at %d", off)
		}
		off += n
		return v, nil
	}
	str := func() (string, error) {
		l, err := uv()
		if err != nil {
			return "", err
		}
		if uint64(len(packed)-off) < l {
			return "", fmt.Errorf("attachment tables: string truncated at %d", off)
		}
		s := string(packed[off : off+int(l)])
		off += int(l)
		return s, nil
	}
	count, err := uv()
	if err != nil {
		return nil, err
	}
	var out []AttachmentTable
	for i := uint64(0); i < count; i++ {
		entries, err := uv()
		if err != nil {
			return nil, err
		}
		// Gather each document's attachments first, so a document's set is built once.
		byDoc := map[codec.UUID]AttachmentSet{}
		var order []codec.UUID
		for j := uint64(0); j < entries; j++ {
			if len(packed)-off < 32 {
				return nil, fmt.Errorf("attachment tables: entry truncated at %d", off)
			}
			id := codec.UUID{MSB: int64(binary.BigEndian.Uint64(packed[off:])), LSB: int64(binary.BigEndian.Uint64(packed[off+8:]))}
			attID := codec.UUID{MSB: int64(binary.BigEndian.Uint64(packed[off+16:])), LSB: int64(binary.BigEndian.Uint64(packed[off+24:]))}
			off += 32
			name, err := str()
			if err != nil {
				return nil, err
			}
			if len(packed)-off < 1+32+8 {
				return nil, fmt.Errorf("attachment tables: entry truncated at %d", off)
			}
			r := AttachmentRef{Scheme: packed[off]}
			copy(r.ContentHash.Bytes[:], packed[off+1:off+33])
			r.Size = int64(binary.BigEndian.Uint64(packed[off+33:]))
			off += 41
			if r.Mime, err = str(); err != nil {
				return nil, err
			}
			if err := ValidateAttachmentName(name); err != nil {
				return nil, fmt.Errorf("attachment tables: %w", err)
			}
			if _, seen := byDoc[id]; !seen {
				order = append(order, id)
			}
			set, err := byDoc[id].with(Attachment{ID: attID, Name: name, AttachmentRef: r})
			if err != nil {
				return nil, fmt.Errorf("attachment tables: %w", err)
			}
			byDoc[id] = set
		}
		t := EmptyAttachmentTable()
		for _, id := range order {
			if t, err = t.setDoc(id, nil, byDoc[id]); err != nil {
				return nil, err
			}
		}
		out = append(out, t)
	}
	if off != len(packed) {
		return nil, fmt.Errorf("attachment tables: %d trailing bytes", len(packed)-off)
	}
	return out, nil
}
