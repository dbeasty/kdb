package document

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// DocumentTree is the doc id → content hash map at a commit.
//
// Entries is nil for a tree produced by With/Without - those are backed incrementally by
// their trie alone (see document_tree_trie.go) and deliberately do NOT eagerly materialize a flat
// map, which used to be copied in full on every single call (O(current document count), not
// O(delta)) and was found to be a major contributor to kdb-service getting OOM-killed under
// sustained write load once a namespace grew past a few hundred documents (see
// docs/benchmarks/lightsail-sim/README.md and go/kdb/transaction's
// BenchmarkCommitScalingWithHistorySize). Contains/HashFor/Size all work correctly regardless of
// whether Entries is populated (trie-backed when it isn't); call MaterializedEntries()
// explicitly for the rare cases that genuinely need the full flat map (wire/storage
// serialization, DAG diff, full scans) - see its own doc comment.
//
// Entries is still populated for trees built directly from a flat map (BuildDocumentTree, e.g.
// wire decode) - no regression there, since that map already existed in full before construction.
type DocumentTree struct {
	TreeHash codec.Hash
	Entries  map[codec.UUID]codec.Hash

	// size backs Size() when Entries is nil (trie-backed tree) - tracked incrementally in
	// With/Without so Size() stays O(1) without needing a full trie walk.
	size int

	// trie backs incremental With/Without updates (see document_tree_trie.go): when present,
	// TreeHash for a derived tree is computed in O(delta) instead of O(len(Entries)). It is the
	// tree's handle in its slab (document_tree_slab.go), and holding it is what keeps the tree's
	// records from being reused. nil for the empty tree, and for a tree built from a flat map
	// without one (DocumentTreeFromValue does build one) - With/Without then build one first.
	trie *trieTree
}

func (t DocumentTree) Size() int {
	if t.Entries != nil {
		return len(t.Entries)
	}
	return t.size
}

func (t DocumentTree) Contains(docID codec.UUID) bool {
	if t.Entries != nil {
		_, ok := t.Entries[docID]
		return ok
	}
	_, ok := t.trie.get(docID)
	return ok
}

func (t DocumentTree) HashFor(docID codec.UUID) (codec.Hash, bool) {
	if t.Entries != nil {
		h, ok := t.Entries[docID]
		return h, ok
	}
	return t.trie.get(docID)
}

// SlabBytes is the memory of the slab holding t's trie: every record of t, and of every other
// tree derived from the same build by With/Without that is still held (history, readers), plus
// free slots awaiting reuse. That is what the tree family actually occupies; a per-document
// estimate cannot see the free slots, which run to about half the live records in a namespace
// that is mostly updated. Zero for an empty tree or one without a trie.
func (t DocumentTree) SlabBytes() int64 {
	if t.trie == nil {
		return 0
	}
	return t.trie.slab.chunkBytes()
}

// MaterializedEntries returns the full doc id → content hash map, building it from the trie
// (O(n)) if this tree doesn't already carry one. Only call this where a full map is genuinely
// needed (wire/storage serialization, DAG diff, full namespace scans) - never on a per-write
// hot path, which is exactly the mistake With/Without used to make on every single call.
func (t DocumentTree) MaterializedEntries() map[codec.UUID]codec.Hash {
	if t.Entries != nil {
		return t.Entries
	}
	return t.trie.entries()
}

// Walk streams every (doc id, content hash) entry to visit, stopping early when visit returns
// false. This is the scan-shaped access path: unlike MaterializedEntries it allocates nothing
// proportional to the namespace, so a bounded scan (LIMIT, row budget) over a large namespace
// costs memory proportional to what it keeps, not to what exists. Iteration order is trie order
// (effectively arbitrary but stable for a given tree), matching MaterializedEntries' map
// iteration in being unordered.
func (t DocumentTree) Walk(visit func(codec.UUID, codec.Hash) bool) {
	if t.Entries != nil {
		for id, h := range t.Entries {
			if !visit(id, h) {
				return
			}
		}
		return
	}
	t.trie.walkUntil(visit)
}

func (t DocumentTree) With(docID codec.UUID, contentHash codec.Hash) (DocumentTree, error) {
	h := t.trieRootOrBuild()
	_, existed := h.get(docID)
	nh := trieWith(h, docID, contentHash)
	newSize := t.Size()
	if !existed {
		newSize++
	}
	return DocumentTree{TreeHash: nh.rootHash(), size: newSize, trie: nh}, nil
}

// WithAll is With for each of entries in turn - the same tree, the same hash - built with each
// node it touches hashed once rather than once per entry. For adding many documents at a time:
// a snapshot install's batches.
func (t DocumentTree) WithAll(entries []TreeEntry) (DocumentTree, error) {
	if len(entries) == 0 {
		return t, nil
	}
	items := make([]trieItem, len(entries))
	for i, en := range entries {
		items[i] = trieItem{key: uuidKey(en.DocID), hash: en.ContentHash}
	}
	// Sorted by key, and the last of any key wins - With's order of application.
	sort.SliceStable(items, func(i, j int) bool { return bytes.Compare(items[i].key[:], items[j].key[:]) < 0 })
	w := 0
	for i := range items {
		if w > 0 && items[w-1].key == items[i].key {
			items[w-1] = items[i]
			continue
		}
		items[w] = items[i]
		w++
	}
	items = items[:w]
	h := t.trieRootOrBuild()
	newSize := t.Size()
	for i := range items {
		if _, existed := h.get(keyUUID(&items[i].key)); !existed {
			newSize++
		}
	}
	nh := trieWithAll(h, items)
	return DocumentTree{TreeHash: nh.rootHash(), size: newSize, trie: nh}, nil
}

// TreeEntry is one document's entry in a tree: for DocumentTree.WithAll.
type TreeEntry struct {
	DocID       codec.UUID
	ContentHash codec.Hash
}

func (t DocumentTree) Without(docID codec.UUID) (DocumentTree, error) {
	h := t.trieRootOrBuild()
	_, existed := h.get(docID)
	nh := trieWithout(h, docID)
	newSize := t.Size()
	if existed {
		newSize--
	}
	return DocumentTree{TreeHash: nh.rootHash(), size: newSize, trie: nh}, nil
}

// trieRootOrBuild returns t's trie root, building one from t.Entries (O(n))
// if t doesn't already carry one - e.g. a tree decoded from the wire via
// BuildDocumentTree/DocumentTreeFromValue. One-time cost; every
// subsequent With/Without on the result is O(delta).
func (t DocumentTree) trieRootOrBuild() *trieTree {
	if t.trie != nil || t.Entries == nil || len(t.Entries) == 0 {
		return t.trie
	}
	return trieBuild(t.Entries)
}

var emptyTree DocumentTree

func init() {
	var err error
	emptyTree, err = BuildDocumentTree(nil)
	if err != nil {
		panic(err)
	}
}

// EmptyDocumentTree is the canonical empty tree.
func EmptyDocumentTree() DocumentTree { return emptyTree }

// BuildDocumentTree constructs a tree with content-addressed tree hash,
// building a full trie from entries (O(n) - see trieBuild). Used for
// trees not derived incrementally via With/Without (e.g. wire decode);
// the resulting tree still carries a trie, so any subsequent With/Without
// on it is O(delta).
func BuildDocumentTree(entries map[codec.UUID]codec.Hash) (DocumentTree, error) {
	if entries == nil {
		entries = map[codec.UUID]codec.Hash{}
	}
	h := trieBuild(entries)
	return DocumentTree{TreeHash: h.rootHash(), Entries: entries, trie: h}, nil
}

// BuildTrieDocumentTree is BuildDocumentTree without keeping the flat map: the tree is backed by
// its trie alone, as one produced by With/Without is. For a tree that will live for the life of
// the process - the live tree an open restores from a checkpoint - where holding both the map and
// the trie cost ~120 bytes per document twice over (120 MB at a million documents). entries may
// be dropped by the caller afterwards.
func BuildTrieDocumentTree(entries map[codec.UUID]codec.Hash) (DocumentTree, error) {
	h := trieBuild(entries)
	return DocumentTree{TreeHash: h.rootHash(), trie: h, size: len(entries)}, nil
}

// PackedTreeEntrySize is one entry of a packed tree: the document id's 16 bytes, then its 32-byte
// content hash.
const PackedTreeEntrySize = 16 + 32

// AppendPacked appends the tree's entries to dst in ascending id order, PackedTreeEntrySize bytes
// each. A trie walk already visits ids in that order, so a trie-backed tree packs without sorting.
// For a checkpoint: a million entries as generic codec records cost the better part of a gigabyte
// to encode and again to decode, against 48 MB packed.
func (t DocumentTree) AppendPacked(dst []byte) []byte {
	start := len(dst)
	t.Walk(func(id codec.UUID, h codec.Hash) bool {
		dst = id.AppendBytes(dst)
		dst = append(dst, h.Bytes[:]...)
		return true
	})
	if t.Entries != nil { // a map walk is unordered
		packed := dst[start:]
		n := len(packed) / PackedTreeEntrySize
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool {
			return bytes.Compare(packed[idx[a]*PackedTreeEntrySize:idx[a]*PackedTreeEntrySize+16],
				packed[idx[b]*PackedTreeEntrySize:idx[b]*PackedTreeEntrySize+16]) < 0
		})
		sorted := make([]byte, 0, len(packed))
		for _, i := range idx {
			sorted = append(sorted, packed[i*PackedTreeEntrySize:(i+1)*PackedTreeEntrySize]...)
		}
		dst = append(dst[:start], sorted...)
	}
	return dst
}

// WritePacked writes exactly the bytes AppendPacked appends, through a small buffer, and returns
// how many it wrote: a checkpoint streams its tree section this way instead of holding a packed
// image of the whole tree (480 MB at 10M documents).
func (t DocumentTree) WritePacked(w io.Writer) (int64, error) {
	if t.Entries != nil { // map-backed: unordered, so packed and sorted whole (small trees only)
		b := t.AppendPacked(nil)
		n, err := w.Write(b)
		return int64(n), err
	}
	const flushAt = 64 << 10
	// A whole-tree walk touches every page of a mapped tree's segments: let the ones behind it go
	// as it goes, so a checkpoint's walk of a 10M-document tree does not hold ~900 MB resident at
	// its end. Pages it comes back to fault in again.
	const dropEvery = 64 << 20
	buf := make([]byte, 0, flushAt+PackedTreeEntrySize)
	var written, sinceDrop int64
	var werr error
	t.Walk(func(id codec.UUID, h codec.Hash) bool {
		buf = id.AppendBytes(buf)
		buf = append(buf, h.Bytes[:]...)
		if len(buf) >= flushAt {
			n, err := w.Write(buf)
			written += int64(n)
			sinceDrop += int64(n)
			if err != nil {
				werr = err
				return false
			}
			buf = buf[:0]
			if sinceDrop >= dropEvery {
				t.DropMappedPages()
				sinceDrop = 0
			}
		}
		return true
	})
	if werr != nil {
		return written, werr
	}
	n, err := w.Write(buf)
	return written + int64(n), err
}

// BuildTrieDocumentTreePacked builds a trie-backed tree from AppendPacked's output without an
// intermediate map: entries already in ascending id order go straight to the bulk build. Entries
// that are not strictly ascending (a corrupt or foreign input) are an error, never a silently
// different tree.
func BuildTrieDocumentTreePacked(packed []byte) (DocumentTree, error) {
	if len(packed)%PackedTreeEntrySize != 0 {
		return DocumentTree{}, fmt.Errorf("document: packed tree of %d bytes is not a whole number of entries", len(packed))
	}
	// The entries are read in place: a packed entry has trieItem's layout.
	items := packedItems(packed)
	for i := 1; i < len(items); i++ {
		if bytes.Compare(items[i-1].key[:], items[i].key[:]) >= 0 {
			return DocumentTree{}, fmt.Errorf("document: packed tree entries out of order at %d", i)
		}
	}
	h := trieBuildItems(items)
	return DocumentTree{TreeHash: h.rootHash(), trie: h, size: len(items)}, nil
}

func entriesToArrayValue(entries map[codec.UUID]codec.Hash) codec.Value {
	// Sort keys are computed once per entry (O(n) String() calls) rather
	// than inside the comparator (O(n log n) calls, ~2x per comparison):
	// at 2000 entries this step alone was ~12ms, over 95% of
	// BuildDocumentTree's total cost and two orders of magnitude more
	// than the actual SHA256/wire-encode work - see the Phase 3 note in
	// docs/benchmarks/phase0-baseline.md. Sort order (and therefore the
	// resulting hash) is unchanged: same comparator, same strings.
	type keyed struct {
		id  codec.UUID
		key string
	}
	sorted := make([]keyed, 0, len(entries))
	for id := range entries {
		sorted = append(sorted, keyed{id: id, key: id.String()})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].key < sorted[j].key
	})
	els := make([]codec.Value, len(sorted))
	for i, k := range sorted {
		els[i] = codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(k.id),
			2: hashVal(entries[k.id]),
		}}
	}
	return codec.ArrayValue{Elements: els}
}

// DocumentTreeFromValue decodes a wire document tree array.
func DocumentTreeFromValue(value codec.Value) (DocumentTree, error) {
	arr, ok := value.(codec.ArrayValue)
	if !ok {
		return DocumentTree{}, NewCommitDecodeError("DocumentTree: expected array", nil)
	}
	m := make(map[codec.UUID]codec.Hash, len(arr.Elements))
	for _, e := range arr.Elements {
		rec, ok := e.(codec.RecordValue)
		if !ok {
			return DocumentTree{}, NewCommitDecodeError("DocumentTree: entry record", nil)
		}
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return DocumentTree{}, NewCommitDecodeError("DocumentTree: docId", nil)
		}
		h, err := hashFromVal(rec.Fields[2])
		if err != nil {
			return DocumentTree{}, err
		}
		m[id] = h
	}
	return BuildDocumentTree(m)
}

// Branch is a named branch pointer.
type Branch struct {
	Name        string
	NamespaceID string
	HeadHash    codec.Hash
	CreatedAt   codec.Timestamp
	UpdatedAt   codec.Timestamp
}

// Tag is a named immutable ref.
type Tag struct {
	Name        string
	NamespaceID string
	CommitHash  codec.Hash
	CreatedAt   codec.Timestamp
	Message     string
}

// CommitStub is a DAG placeholder for ice-archived commits.
type CommitStub struct {
	OriginalHash    codec.Hash
	ArchiveLocation string
	StubbedAt       codec.Timestamp
}

func (s CommitStub) ToValue() codec.Value {
	return codec.RecordValue{Fields: map[int]codec.Value{
		1: hashVal(s.OriginalHash),
		2: codec.StringValue{V: s.ArchiveLocation},
		3: timestampVal(s.StubbedAt),
	}}
}

// CommitStubFromValue decodes a wire commit stub record.
func CommitStubFromValue(value codec.Value) (CommitStub, error) {
	rec, ok := value.(codec.RecordValue)
	if !ok {
		return CommitStub{}, NewCommitDecodeError("CommitStub: expected record", nil)
	}
	oh, err := hashFromVal(rec.Fields[1])
	if err != nil {
		return CommitStub{}, NewCommitDecodeError("CommitStub: originalHash", nil)
	}
	loc, ok := rec.Fields[2].(codec.StringValue)
	if !ok {
		return CommitStub{}, NewCommitDecodeError("CommitStub: archiveLocation", nil)
	}
	ts, err := timestampFromVal(rec.Fields[3])
	if err != nil {
		return CommitStub{}, NewCommitDecodeError("CommitStub: stubbedAt", nil)
	}
	return CommitStub{OriginalHash: oh, ArchiveLocation: loc.V, StubbedAt: ts}, nil
}

// WalkAfter streams the entries whose id sorts after after - unsigned big-endian bytes, the order
// of the ids' canonical string forms - in ascending id order, stopping early when visit returns
// false. Unlike Walk, the order is guaranteed, and finding the first entry costs the depth of the
// trie rather than a walk of every entry before it: a caller paging through a tree by "the ids
// after the last one I saw" pays for the page, not for its position (peersync's snapshot pages).
//
// A tree built from a flat map without a trie builds one first (O(n), once per call).
func (t DocumentTree) WalkAfter(after codec.UUID, visit func(codec.UUID, codec.Hash) bool) {
	t.walkAfter(after, visit, nil)
}

// WalkAfterCounting is WalkAfter, also reporting how many trie nodes it entered - for tests and
// benchmarks asserting that a page costs the page.
func (t DocumentTree) WalkAfterCounting(after codec.UUID, visit func(codec.UUID, codec.Hash) bool) (visited int) {
	t.walkAfter(after, visit, &visited)
	return visited
}

// WalkOrdered streams every entry in ascending id order (see WalkAfter).
func (t DocumentTree) WalkOrdered(visit func(codec.UUID, codec.Hash) bool) {
	t.trieRootOrBuild().walkUntil(visit)
}

func (t DocumentTree) walkAfter(after codec.UUID, visit func(codec.UUID, codec.Hash) bool, visited *int) {
	t.trieRootOrBuild().walkAfter(after, visit, visited)
}
