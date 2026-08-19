package document

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// DocumentTree is the materialised doc id → content hash map at a commit.
type DocumentTree struct {
	TreeHash codec.Hash
	Entries  map[codec.UUID]codec.Hash

	// trieRoot backs incremental With/Without updates (see
	// document_tree_trie.go): when present, TreeHash for a derived tree
	// is computed in O(delta) instead of O(len(Entries)). Trees built
	// via BuildDocumentTree from a flat map (e.g. wire decode) don't
	// carry one - With/Without fall back to a one-time O(n) trieBuild
	// in that case, then are incremental from then on. Always correct
	// either way; only the constant differs.
	trieRoot *trieNode
}

func (t DocumentTree) Size() int { return len(t.Entries) }

func (t DocumentTree) Contains(docID codec.UUID) bool {
	_, ok := t.Entries[docID]
	return ok
}

func (t DocumentTree) HashFor(docID codec.UUID) (codec.Hash, bool) {
	h, ok := t.Entries[docID]
	return h, ok
}

func (t DocumentTree) With(docID codec.UUID, contentHash codec.Hash) (DocumentTree, error) {
	entries := make(map[codec.UUID]codec.Hash, len(t.Entries)+1)
	for k, v := range t.Entries {
		entries[k] = v
	}
	entries[docID] = contentHash
	root := t.trieRootOrBuild()
	root = trieInsert(root, docID, contentHash)
	return DocumentTree{TreeHash: trieTreeHash(root), Entries: entries, trieRoot: root}, nil
}

func (t DocumentTree) Without(docID codec.UUID) (DocumentTree, error) {
	entries := make(map[codec.UUID]codec.Hash, len(t.Entries))
	for k, v := range t.Entries {
		if k != docID {
			entries[k] = v
		}
	}
	root := t.trieRootOrBuild()
	root = trieDelete(root, docID)
	return DocumentTree{TreeHash: trieTreeHash(root), Entries: entries, trieRoot: root}, nil
}

// trieRootOrBuild returns t's trie root, building one from t.Entries (O(n))
// if t doesn't already carry one - e.g. a tree decoded from the wire via
// BuildDocumentTree/DocumentTreeFromValue. One-time cost; every
// subsequent With/Without on the result is O(delta).
func (t DocumentTree) trieRootOrBuild() *trieNode {
	if t.trieRoot != nil || len(t.Entries) == 0 {
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
