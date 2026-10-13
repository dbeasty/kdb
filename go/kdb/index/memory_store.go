package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// MemoryStore is the hash/btree index. Hash and btree share one implementation (event_log.go):
// a head state of interned keys and their documents, kept in key order once a range has been
// asked for, plus a bounded window of history for reads at older commits.
type MemoryStore struct {
	descriptor Descriptor
	log        *eventLog
}

// NewMemoryStore creates an in-memory index bound to a DAG, with default options.
func NewMemoryStore(descriptor Descriptor, d *dag.InMemoryCommitDag) *MemoryStore {
	return NewMemoryStoreWithOptions(descriptor, d, MemoryOptions{})
}

// NewMemoryStoreWithOptions creates an in-memory index bound to a DAG.
func NewMemoryStoreWithOptions(descriptor Descriptor, d *dag.InMemoryCommitDag, opts MemoryOptions) *MemoryStore {
	return &MemoryStore{descriptor: descriptor, log: newEventLog(d, opts)}
}

func (s *MemoryStore) Descriptor() Descriptor { return s.descriptor }

func (s *MemoryStore) Put(entry Entry) error {
	s.log.put(entry)
	return nil
}

func (s *MemoryStore) Delete(docID codec.UUID, atCommit codec.Hash) error {
	s.log.delete(docID, atCommit)
	return nil
}

func (s *MemoryStore) BulkLoad(entries []Entry) error {
	s.log.bulkLoad(entries)
	return nil
}

func (s *MemoryStore) Rebuild(entries []Entry) error { return s.BulkLoad(entries) }

func (s *MemoryStore) Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	return s.log.lookup(key, atCommit)
}

// LookupCount is len(Lookup(key)) without building or copying the id list.
func (s *MemoryStore) LookupCount(key Key, atCommit *codec.Hash) (int, error) {
	if err := s.requireKeyedType(); err != nil {
		return 0, err
	}
	return s.log.lookupCount(key, atCommit)
}

func (s *MemoryStore) Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	return s.log.rangeScan(from, to, atCommit, limit, ascending)
}

func (s *MemoryStore) Search(query string, atCommit *codec.Hash, limit int) ([]RankedResult, error) {
	return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeFullText, s.descriptor.Type)
}

func (s *MemoryStore) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]RankedResult, error) {
	return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeVector, s.descriptor.Type)
}

func (s *MemoryStore) Clear() error {
	s.log.clear()
	return nil
}

func (s *MemoryStore) IsValid(atCommit codec.Hash) (bool, error) {
	return s.log.hasCommit(atCommit), nil
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	return s.log.snapshotBytes()
}

// NeedsRebuild brings the head state up to the DAG head and reports whether it could not be:
// the commit graph changed in a way the incremental state cannot follow (a graft, a move of the
// head to a commit that does not descend from it) after history was dropped. The store then
// answers every read with ErrHistoryUnavailable until it is rebuilt.
func (s *MemoryStore) NeedsRebuild() bool { return s.log.needsRebuild() }

// ForgetHistory records that the store's content was just rebuilt from the documents at head,
// so it holds no history: a read at an older commit reports ErrHistoryUnavailable instead of
// replaying the rebuild's events as if they had happened then.
func (s *MemoryStore) ForgetHistory() { s.log.forgetHistory() }

func (s *MemoryStore) RestoreSnapshot(data []byte) error {
	return s.log.restoreSnapshotBytes(data)
}

// PrepareDocument extracts the document's keys for this index. A single-field index files the
// document under every scalar candidate at the path (an array field is multikey, so
// `tags = 'x'` is membership); a multi-field index files it under one CompositeKey built from
// the first candidate of each field. A document with no candidate (or, for a composite, any
// field absent) applies as a delete, so a document that stops carrying the field leaves the
// index.
func (s *MemoryStore) PrepareDocument(docID codec.UUID, jsonText string) (PreparedPut, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	root, err := ParseDocument(jsonText)
	if err != nil {
		return nil, err
	}
	fieldType := s.descriptor.StringOption(OptionFieldType, "")
	paths := s.descriptor.FieldPaths()
	prepared := &memoryPreparedPut{store: s, docID: docID}
	if len(paths) == 1 {
		seen := make(map[string]struct{})
		for _, v := range PathValues(root, paths[0]) {
			k, ok := KeyFromJSONValue(v, fieldType)
			if !ok {
				continue
			}
			ks := KeyString(k)
			if _, dup := seen[ks]; dup {
				continue
			}
			seen[ks] = struct{}{}
			prepared.keys = append(prepared.keys, k)
		}
		return prepared, nil
	}
	parts := make([]Key, 0, len(paths))
	for _, p := range paths {
		vals := PathValues(root, p)
		if len(vals) == 0 {
			return prepared, nil
		}
		k, ok := KeyFromJSONValue(vals[0], fieldType)
		if !ok {
			return prepared, nil
		}
		parts = append(parts, k)
	}
	prepared.keys = []Key{CompositeKey{Parts: parts}}
	return prepared, nil
}

// PutDocument is PrepareDocument followed by Apply.
func (s *MemoryStore) PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error {
	p, err := s.PrepareDocument(docID, jsonText)
	if err != nil {
		return err
	}
	_, err = p.Apply(commitHash)
	return err
}

type memoryPreparedPut struct {
	store *MemoryStore
	docID codec.UUID
	keys  []Key
}

// Apply always records a delete first so a document whose key changed leaves its old bucket,
// then files the new keys. Both events carry commitHash, so an as-of read before that commit
// still sees the old key and a read at or after it sees only the new ones.
func (p *memoryPreparedPut) Apply(commitHash codec.Hash) (Hint, error) {
	s := p.store
	s.log.delete(p.docID, commitHash)
	hint := Hint{
		IndexID:    s.descriptor.IndexID,
		FieldName:  s.descriptor.FirstField(),
		Type:       s.descriptor.Type,
		Action:     HintActionDelete,
		DocID:      p.docID,
		CommitHash: commitHash,
	}
	if len(p.keys) == 0 {
		return hint, nil
	}
	for _, k := range p.keys {
		s.log.put(Entry{DocID: p.docID, Key: k, CommitHash: commitHash})
	}
	hint.Action = HintActionPut
	if len(p.keys) == 1 {
		hint.Key = p.keys[0]
	} else {
		hint.Key = CompositeKey{Parts: p.keys}
	}
	return hint, nil
}

func (s *MemoryStore) requireKeyedType() error {
	if s.descriptor.Type != IndexTypeHash && s.descriptor.Type != IndexTypeBTree {
		return NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeHash, s.descriptor.Type)
	}
	return nil
}
