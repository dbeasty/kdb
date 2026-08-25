package document

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// DocumentTree is the doc id → content hash map at a commit.
//
// Entries is nil for a tree produced by With/Without - those are backed incrementally by
// trieRoot alone (see document_tree_trie.go) and deliberately do NOT eagerly materialize a flat
// map, which used to be copied in full on every single call (O(current document count), not
// O(delta)) and was found to be a major contributor to kdb-service getting OOM-killed under
// sustained write load once a namespace grew past a few hundred documents (see
// docs/benchmarks/lightsail-sim/README.md and go/kdb/transaction's
// BenchmarkCommitScalingWithHistorySize). Contains/HashFor/Size all work correctly regardless of
// whether Entries is populated (trieRoot-backed when it isn't); call MaterializedEntries()
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

	// trieRoot backs incremental With/Without updates (see
	// document_tree_trie.go): when present, TreeHash for a derived tree
	// is computed in O(delta) instead of O(len(Entries)). Trees built
	// via BuildDocumentTree from a flat map (e.g. wire decode) don't
	// carry one - With/Without fall back to a one-time O(n) trieBuild
	// in that case, then are incremental from then on. Always correct
	// either way; only the constant differs.
	trieRoot *trieNode
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
	_, ok := trieGet(t.trieRoot, docID)
	return ok
}

func (t DocumentTree) HashFor(docID codec.UUID) (codec.Hash, bool) {
	if t.Entries != nil {
		h, ok := t.Entries[docID]
		return h, ok
	}
	return trieGet(t.trieRoot, docID)
}

// MaterializedEntries returns the full doc id → content hash map, building it from the trie
// (O(n)) if this tree doesn't already carry one. Only call this where a full map is genuinely
// needed (wire/storage serialization, DAG diff, full namespace scans) - never on a per-write
// hot path, which is exactly the mistake With/Without used to make on every single call.
func (t DocumentTree) MaterializedEntries() map[codec.UUID]codec.Hash {
	if t.Entries != nil {
		return t.Entries
	}
	return trieEntries(t.trieRoot)
}

func (t DocumentTree) With(docID codec.UUID, contentHash codec.Hash) (DocumentTree, error) {
	root := t.trieRootOrBuild()
	_, existed := trieGet(root, docID)
	newRoot := trieInsert(root, docID, contentHash)
	newSize := t.Size()
	if !existed {
		newSize++
	}
	return DocumentTree{TreeHash: trieTreeHash(newRoot), size: newSize, trieRoot: newRoot}, nil
}

func (t DocumentTree) Without(docID codec.UUID) (DocumentTree, error) {
	root := t.trieRootOrBuild()
	_, existed := trieGet(root, docID)
	newRoot := trieDelete(root, docID)
	newSize := t.Size()
	if existed {
		newSize--
	}
	return DocumentTree{TreeHash: trieTreeHash(newRoot), size: newSize, trieRoot: newRoot}, nil
}

// trieRootOrBuild returns t's trie root, building one from t.Entries (O(n))
// if t doesn't already carry one - e.g. a tree decoded from the wire via
// BuildDocumentTree/DocumentTreeFromValue. One-time cost; every
// subsequent With/Without on the result is O(delta).
func (t DocumentTree) trieRootOrBuild() *trieNode {
	if t.trieRoot != nil || t.Entries == nil || len(t.Entries) == 0 {
		return t.trieRoot
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
	root := trieBuild(entries)
	return DocumentTree{TreeHash: trieTreeHash(root), Entries: entries, trieRoot: root}, nil
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
