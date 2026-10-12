package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// nodeStore holds the DAG's resident commits in a form the garbage collector does not have to
// scan.
//
// The previous representation was map[codec.Hash]commitNode with a document.Commit as the
// value: a slice of parents, three strings, a slice of operations and a pointer to a schema hash
// per commit. Every one of those is a pointer the collector traces on every cycle, and the whole
// thing measured 438-523 bytes per commit (docs/kdb-mmap-plan.md, Phase 0). This keeps the same
// information in two pointer-free structures:
//
//   - index: hash -> slot, a map whose key and value contain no pointers, which Go does not scan
//   - slab:  fixed-width slot records in 4096-slot chunks, likewise not scanned
//
// Anything that genuinely is a pointer lives in a side map holding only the commits that need it:
// messages (most commits have none), operations (bounded by the DAG's operations budget), and the
// parents of the rare commit with more than two.
//
// Not safe for concurrent use; the owning InMemoryCommitDag's mu guards it exactly as it guarded
// the map this replaces.
type nodeStore struct {
	index map[codec.Hash]uint32
	// chunks is the slab, in fixed-size blocks rather than one growing slice: a single slice
	// doubles its capacity as it fills, so up to half of it is empty, and every doubling copies
	// the whole thing at the moment the DAG is busiest admitting commits. The chunk table itself
	// holds a pointer per slotsPerChunk commits, which is negligible next to what it replaces.
	chunks []*[slotsPerChunk]slot
	used   uint32 // slots ever handed out; free holds the ones since returned
	free   []uint32

	// namespaces interns Commit.NamespaceID. A DAG holds one namespace, so this is almost always
	// a single entry; it is a table rather than a field only so a commit that names a different
	// namespace still round-trips exactly.
	namespaces []string

	messages map[codec.Hash]string
	ops      map[codec.Hash][]document.Op
	extra    map[codec.Hash][]codec.Hash // parents beyond the second
}

// slot is one commit's fixed-width record. It must stay free of pointers: that is the whole
// point of it, and TestSlotHasNoPointers pins it.
type slot struct {
	hash       codec.Hash
	parent0    codec.Hash
	parent1    codec.Hash
	schema     codec.Hash
	tree       codec.Hash
	tx         codec.UUID
	author     codec.UUID
	epochMs    int64
	micro      int64
	generation uint32
	nParents   uint32
	ns         uint16
	hasSchema  bool
	live       bool
}

const slotsPerChunk = 4096

func (s *nodeStore) at(i uint32) *slot {
	return &s.chunks[i/slotsPerChunk][i%slotsPerChunk]
}

func newNodeStore() nodeStore {
	return nodeStore{index: make(map[codec.Hash]uint32)}
}

func (s *nodeStore) len() int { return len(s.index) }

func (s *nodeStore) has(h codec.Hash) bool {
	_, ok := s.index[h]
	return ok
}

func (s *nodeStore) generation(h codec.Hash) uint32 {
	if i, ok := s.index[h]; ok {
		return s.at(i).generation
	}
	return 0
}

func (s *nodeStore) setGeneration(h codec.Hash, gen uint32) {
	if i, ok := s.index[h]; ok {
		s.at(i).generation = gen
	}
}

func (s *nodeStore) internNamespace(ns string) uint16 {
	for i, n := range s.namespaces {
		if n == ns {
			return uint16(i)
		}
	}
	s.namespaces = append(s.namespaces, ns)
	return uint16(len(s.namespaces) - 1)
}

// put stores c, replacing any commit already held under its hash and keeping that commit's
// generation unless gen is supplied by the caller (put always writes gen).
func (s *nodeStore) put(c document.Commit, gen uint32) {
	i, ok := s.index[c.Hash]
	if !ok {
		if n := len(s.free); n > 0 {
			i = s.free[n-1]
			s.free = s.free[:n-1]
		} else {
			i = s.used
			s.used++
			if int(i/slotsPerChunk) == len(s.chunks) {
				s.chunks = append(s.chunks, new([slotsPerChunk]slot))
			}
		}
		s.index[c.Hash] = i
	}
	sl := slot{
		hash:       c.Hash,
		tree:       c.DocumentTreeHash,
		tx:         c.TransactionID,
		author:     c.AuthorNodeID,
		epochMs:    c.Timestamp.EpochMillis,
		micro:      int64(c.Timestamp.MicroRemainder),
		generation: gen,
		nParents:   uint32(len(c.ParentHashes)),
		ns:         s.internNamespace(c.NamespaceID),
		live:       true,
	}
	if len(c.ParentHashes) > 0 {
		sl.parent0 = c.ParentHashes[0]
	}
	if len(c.ParentHashes) > 1 {
		sl.parent1 = c.ParentHashes[1]
	}
	if c.SchemaHash != nil {
		sl.hasSchema = true
		sl.schema = *c.SchemaHash
	}
	*s.at(i) = sl

	if len(c.ParentHashes) > 2 {
		if s.extra == nil {
			s.extra = make(map[codec.Hash][]codec.Hash)
		}
		s.extra[c.Hash] = append([]codec.Hash(nil), c.ParentHashes[2:]...)
	} else if s.extra != nil {
		delete(s.extra, c.Hash)
	}
	if c.Message != "" {
		if s.messages == nil {
			s.messages = make(map[codec.Hash]string)
		}
		s.messages[c.Hash] = c.Message
	} else if s.messages != nil {
		delete(s.messages, c.Hash)
	}
	if len(c.Operations) > 0 {
		if s.ops == nil {
			s.ops = make(map[codec.Hash][]document.Op)
		}
		s.ops[c.Hash] = c.Operations
	} else if s.ops != nil {
		delete(s.ops, c.Hash)
	}
}

// replace swaps the commit held under its own hash and keeps the generation already derived for
// it, for the paths that put operations back on (or take them off) a commit whose shape has not
// changed. Reports false when the hash is not resident.
func (s *nodeStore) replace(c document.Commit) bool {
	i, ok := s.index[c.Hash]
	if !ok {
		return false
	}
	s.put(c, s.at(i).generation)
	return true
}

func (s *nodeStore) delete(h codec.Hash) {
	i, ok := s.index[h]
	if !ok {
		return
	}
	delete(s.index, h)
	*s.at(i) = slot{}
	s.free = append(s.free, i)
	delete(s.messages, h)
	delete(s.ops, h)
	delete(s.extra, h)
}

// commit rebuilds the document.Commit for h. It allocates the parent slice, so a path that only
// needs ancestry should use parents or forEachParent instead.
func (s *nodeStore) commit(h codec.Hash) (document.Commit, bool) {
	i, ok := s.index[h]
	if !ok {
		return document.Commit{}, false
	}
	sl := s.at(i)
	c := document.Commit{
		Hash:             sl.hash,
		NamespaceID:      s.namespaces[sl.ns],
		TransactionID:    sl.tx,
		Timestamp:        codec.Timestamp{EpochMillis: sl.epochMs, MicroRemainder: int(sl.micro)},
		AuthorNodeID:     sl.author,
		DocumentTreeHash: sl.tree,
		Message:          s.messages[h],
		Operations:       s.ops[h],
	}
	if sl.nParents > 0 {
		c.ParentHashes = make([]codec.Hash, 0, sl.nParents)
		c.ParentHashes = append(c.ParentHashes, sl.parent0)
		if sl.nParents > 1 {
			c.ParentHashes = append(c.ParentHashes, sl.parent1)
		}
		if sl.nParents > 2 {
			c.ParentHashes = append(c.ParentHashes, s.extra[h]...)
		}
	}
	if sl.hasSchema {
		sc := sl.schema
		c.SchemaHash = &sc
	}
	return c, true
}

// treeHash is a commit's document tree hash without building the commit.
func (s *nodeStore) treeHash(h codec.Hash) (codec.Hash, bool) {
	if i, ok := s.index[h]; ok {
		return s.at(i).tree, true
	}
	return codec.Hash{}, false
}

// forEachParent calls fn for each parent of h, in order, without allocating. Stops early when fn
// returns false. A hash that is not resident has no parents.
func (s *nodeStore) forEachParent(h codec.Hash, fn func(codec.Hash) bool) {
	i, ok := s.index[h]
	if !ok {
		return
	}
	sl := s.at(i)
	if sl.nParents > 0 && !fn(sl.parent0) {
		return
	}
	if sl.nParents > 1 && !fn(sl.parent1) {
		return
	}
	if sl.nParents > 2 {
		for _, p := range s.extra[h] {
			if !fn(p) {
				return
			}
		}
	}
}

// forEach visits every resident commit's hash. fn must not add or remove commits.
func (s *nodeStore) forEach(fn func(codec.Hash)) {
	for h := range s.index {
		fn(h)
	}
}
