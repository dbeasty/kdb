package document

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The attachment set pool (docs/kdb-attachments-plan.md, "Set pool").
//
// A table's trie maps each document to the hash of its attachment set; the sets themselves sit in
// a pool shared by every version of the table. Without a store the pool keeps every set it was
// given - which for a namespace's lifetime is every set any version of its table ever had, so it
// grows with attachment history, not with what is live. With a store it is a cache: it keeps at
// most its budget of sets in memory, writes a set to the store when it evicts it (once), and reads
// a set it does not hold back from the store, checking it against the hash it was asked for. A set
// the store does not have, or has wrong, is an error (ErrAttachmentSetMissing,
// ErrAttachmentSetCorrupt) - never an empty or different set.
//
// The store need not be durable. Every table a process holds was seeded (a checkpoint, a peer's
// packed table) or derived (ops applied) in that process, and both put every set it names into the
// pool first; the store only has to give back what this process evicted into it. A store that also
// holds sets an earlier process wrote is fine: a set is found by its hash and checked against it.

// ErrAttachmentSetMissing reports a set a table names that neither the pool nor its store holds.
var ErrAttachmentSetMissing = errors.New("attachment set missing")

// ErrAttachmentSetCorrupt reports a set the store holds whose bytes do not decode, or do not hash
// to the hash it is stored under.
var ErrAttachmentSetCorrupt = errors.New("attachment set corrupt")

// AttachmentSetStore holds attachment sets by hash for a pool that keeps only some in memory. The
// bytes are AppendAttachmentSet's encoding.
type AttachmentSetStore interface {
	WriteAttachmentSet(h codec.Hash, encoded []byte) error
	// ReadAttachmentSet returns the set stored under h; ok=false when there is none.
	ReadAttachmentSet(h codec.Hash) (encoded []byte, ok bool, err error)
}

// DefaultAttachmentSetBudget is the memory a store-backed pool keeps sets in, by default.
const DefaultAttachmentSetBudget = 8 << 20

// AttachmentSetPool holds attachment sets by hash for the tables that share it. Safe for concurrent
// use.
type AttachmentSetPool struct {
	mu      sync.Mutex
	store   AttachmentSetStore // nil: keep everything
	budget  int64
	entries map[codec.Hash]*list.Element
	lru     list.List // of *poolEntry, most recently used first
	bytes   int64
	// written are the sets evicted into the store, while TrackWritten is on, since TakeWritten
	// last ran, and prevWritten those of the run before: a store that collects garbage must keep
	// them, as a table not yet committed may name them. Off, nothing is recorded, so a store that
	// never collects does not grow this.
	track       bool
	written     map[codec.Hash]struct{}
	prevWritten map[codec.Hash]struct{}
	stats   AttachmentSetPoolStats
}

type poolEntry struct {
	hash   codec.Hash
	set    AttachmentSet
	size   int64
	stored bool // the store has it (it was read from there, or written on an earlier eviction)
}

// AttachmentSetPoolStats describes a pool.
type AttachmentSetPoolStats struct {
	Sets      int   // sets in memory
	Bytes     int64 // their estimated size
	Loads     int64 // sets read back from the store
	Spills    int64 // sets written to the store
	Evictions int64 // sets dropped from memory
}

// NewAttachmentSetPool returns an empty pool. With a store, it keeps about budgetBytes of sets in
// memory (DefaultAttachmentSetBudget when budgetBytes <= 0); without one it keeps every set.
func NewAttachmentSetPool(store AttachmentSetStore, budgetBytes int64) *AttachmentSetPool {
	if budgetBytes <= 0 {
		budgetBytes = DefaultAttachmentSetBudget
	}
	return &AttachmentSetPool{store: store, budget: budgetBytes, entries: map[codec.Hash]*list.Element{}}
}

// Empty is the empty table over this pool: the tables derived from it share the pool.
func (p *AttachmentSetPool) Empty() AttachmentTable {
	return AttachmentTable{tree: EmptyDocumentTree(), sets: p}
}

// Stats returns the pool's current numbers.
func (p *AttachmentSetPool) Stats() AttachmentSetPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Sets, s.Bytes = len(p.entries), p.bytes
	return s
}

// TrackWritten turns recording of the sets written to the store on or off (see TakeWritten).
func (p *AttachmentSetPool) TrackWritten(on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.track = on
	if !on {
		p.written, p.prevWritten = nil, nil
	}
}

// TakeWritten returns the sets written to the store since the run before last of TakeWritten -
// two collections' worth, so a collection that is abandoned after asking forgets nothing - and
// forgets the older half.
func (p *AttachmentSetPool) TakeWritten() map[codec.Hash]struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[codec.Hash]struct{}, len(p.written)+len(p.prevWritten))
	for h := range p.prevWritten {
		out[h] = struct{}{}
	}
	for h := range p.written {
		out[h] = struct{}{}
	}
	p.prevWritten, p.written = p.written, nil
	return out
}

// Contains reports whether the pool holds h in memory, without loading it.
func (p *AttachmentSetPool) Contains(h codec.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.entries[h]
	return ok
}

func setSize(s AttachmentSet) int64 {
	n := int64(160) // the entry, its list element and map slot
	for _, a := range s {
		n += 96 + int64(len(a.Name)+len(a.Mime))
	}
	return n
}

// put adds s to the pool and returns its hash. Adding may evict others into the store, which may
// fail; the pool is unchanged then, apart from s being in it.
func (p *AttachmentSetPool) put(s AttachmentSet) (codec.Hash, error) {
	h := s.Hash()
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[h]; ok {
		p.lru.MoveToFront(e)
		return h, nil
	}
	p.insertLocked(h, s, false)
	return h, p.evictLocked()
}

func (p *AttachmentSetPool) insertLocked(h codec.Hash, s AttachmentSet, stored bool) {
	e := &poolEntry{hash: h, set: s, size: setSize(s), stored: stored}
	p.entries[h] = p.lru.PushFront(e)
	p.bytes += e.size
}

// evictLocked drops the least recently used sets until the pool is within its budget, writing each
// to the store first unless it is already there. It always keeps the most recent one.
func (p *AttachmentSetPool) evictLocked() error {
	if p.store == nil {
		return nil
	}
	for p.bytes > p.budget && p.lru.Len() > 1 {
		el := p.lru.Back()
		e := el.Value.(*poolEntry)
		// While a collecting store may drop what it holds (TrackWritten), stored proves nothing:
		// write the set again, and record it, so the next collections keep it.
		if !e.stored || p.track {
			if err := p.store.WriteAttachmentSet(e.hash, AppendAttachmentSet(nil, e.set)); err != nil {
				return fmt.Errorf("attachment set %s: spilling to the store: %w", e.hash.Hex(), err)
			}
			if p.track {
				if p.written == nil {
					p.written = map[codec.Hash]struct{}{}
				}
				p.written[e.hash] = struct{}{}
			}
			p.stats.Spills++
		}
		p.lru.Remove(el)
		delete(p.entries, e.hash)
		p.bytes -= e.size
		p.stats.Evictions++
	}
	return nil
}

// get returns the set h names, from memory or else from the store.
func (p *AttachmentSetPool) get(h codec.Hash) (AttachmentSet, error) {
	p.mu.Lock()
	if e, ok := p.entries[h]; ok {
		p.lru.MoveToFront(e)
		s := e.Value.(*poolEntry).set
		p.mu.Unlock()
		return s, nil
	}
	store := p.store
	p.mu.Unlock()
	if store == nil {
		return nil, fmt.Errorf("%w: %s is not in the pool", ErrAttachmentSetMissing, h.Hex())
	}
	b, ok, err := store.ReadAttachmentSet(h)
	if err != nil {
		return nil, fmt.Errorf("attachment set %s: reading the store: %w", h.Hex(), err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s is in neither the pool nor its store", ErrAttachmentSetMissing, h.Hex())
	}
	s, err := ParseAttachmentSet(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrAttachmentSetCorrupt, h.Hex(), err)
	}
	if got := s.Hash(); got != h {
		return nil, fmt.Errorf("%w: stored under %s, hashes to %s", ErrAttachmentSetCorrupt, h.Hex(), got.Hex())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.Loads++
	if e, ok := p.entries[h]; ok { // loaded concurrently
		p.lru.MoveToFront(e)
		return s, nil
	}
	p.insertLocked(h, s, true)
	// Only sets already in the store can be evicted without a write, but a failed write of another
	// one does not make this read wrong: report it on the next put instead.
	_ = p.evictLocked()
	return s, nil
}

// AppendAttachmentSet encodes s for an AttachmentSetStore: a version byte (1), a uvarint count,
// then per attachment in ID order its ID (16), name (uvarint length, bytes), scheme (1), content
// hash (32), size (u64 big-endian) and mime (uvarint length, bytes).
func AppendAttachmentSet(dst []byte, s AttachmentSet) []byte {
	dst = append(dst, 1)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	for _, a := range s {
		dst = binary.BigEndian.AppendUint64(dst, uint64(a.ID.MSB))
		dst = binary.BigEndian.AppendUint64(dst, uint64(a.ID.LSB))
		dst = binary.AppendUvarint(dst, uint64(len(a.Name)))
		dst = append(dst, a.Name...)
		dst = append(dst, a.Scheme)
		dst = append(dst, a.ContentHash.Bytes[:]...)
		dst = binary.BigEndian.AppendUint64(dst, uint64(a.Size))
		dst = binary.AppendUvarint(dst, uint64(len(a.Mime)))
		dst = append(dst, a.Mime...)
	}
	return dst
}

// ParseAttachmentSet reverses AppendAttachmentSet, refusing a set out of ID order or with a
// repeated name. It does not check a hash: the caller knows which one the set must have.
func ParseAttachmentSet(b []byte) (AttachmentSet, error) {
	if len(b) == 0 || b[0] != 1 {
		return nil, fmt.Errorf("unknown attachment set encoding")
	}
	off := 1
	uv := func() (uint64, error) {
		v, n := binary.Uvarint(b[off:])
		if n <= 0 {
			return 0, fmt.Errorf("malformed length at %d", off)
		}
		off += n
		return v, nil
	}
	str := func() (string, error) {
		l, err := uv()
		if err != nil {
			return "", err
		}
		if uint64(len(b)-off) < l {
			return "", fmt.Errorf("string truncated at %d", off)
		}
		s := string(b[off : off+int(l)])
		off += int(l)
		return s, nil
	}
	n, err := uv()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(b)) {
		return nil, fmt.Errorf("count %d exceeds the encoding", n)
	}
	out := make(AttachmentSet, 0, n)
	names := make(map[string]struct{}, n)
	for i := uint64(0); i < n; i++ {
		if len(b)-off < 16 {
			return nil, fmt.Errorf("entry truncated at %d", off)
		}
		a := Attachment{ID: codec.UUID{MSB: int64(binary.BigEndian.Uint64(b[off:])), LSB: int64(binary.BigEndian.Uint64(b[off+8:]))}}
		off += 16
		if a.Name, err = str(); err != nil {
			return nil, err
		}
		if len(b)-off < 1+32+8 {
			return nil, fmt.Errorf("entry truncated at %d", off)
		}
		a.Scheme = b[off]
		copy(a.ContentHash.Bytes[:], b[off+1:off+33])
		a.Size = int64(binary.BigEndian.Uint64(b[off+33:]))
		off += 41
		if a.Mime, err = str(); err != nil {
			return nil, err
		}
		if len(out) > 0 && !uuidLess(out[len(out)-1].ID, a.ID) {
			return nil, fmt.Errorf("attachments out of ID order")
		}
		if _, dup := names[a.Name]; dup {
			return nil, fmt.Errorf("attachment name %q repeated", a.Name)
		}
		names[a.Name] = struct{}{}
		out = append(out, a)
	}
	if off != len(b) {
		return nil, fmt.Errorf("%d trailing bytes", len(b)-off)
	}
	return out, nil
}
