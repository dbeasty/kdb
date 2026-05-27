package document

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// DocumentTree is the materialised doc id → content hash map at a commit.
type DocumentTree struct {
	TreeHash codec.Hash
	Entries  map[codec.UUID]codec.Hash
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
	return BuildDocumentTree(entries)
}

func (t DocumentTree) Without(docID codec.UUID) (DocumentTree, error) {
	entries := make(map[codec.UUID]codec.Hash, len(t.Entries))
	for k, v := range t.Entries {
		if k != docID {
			entries[k] = v
		}
	}
	return BuildDocumentTree(entries)
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

// BuildDocumentTree constructs a tree with content-addressed tree hash.
func BuildDocumentTree(entries map[codec.UUID]codec.Hash) (DocumentTree, error) {
	treeVal := entriesToArrayValue(entries)
	reg := WireRegistry()
	bytes, err := codec.EncodeBytes(treeVal, DocumentTreeType, reg)
	if err != nil {
		return DocumentTree{}, err
	}
	h, err := codec.HashFromBytes(SHA256Digest(bytes))
	if err != nil {
		return DocumentTree{}, err
	}
	if entries == nil {
		entries = map[codec.UUID]codec.Hash{}
	}
	return DocumentTree{TreeHash: h, Entries: entries}, nil
}

func entriesToArrayValue(entries map[codec.UUID]codec.Hash) codec.Value {
	ids := make([]codec.UUID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	els := make([]codec.Value, len(ids))
	for i, id := range ids {
		els[i] = codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(id),
			2: hashVal(entries[id]),
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
