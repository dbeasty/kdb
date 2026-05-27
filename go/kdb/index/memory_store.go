package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

type putEvent struct {
	seq   int64
	entry Entry
}

type deleteEvent struct {
	seq      int64
	docID    codec.UUID
	atCommit codec.Hash
}

// MemoryStore is a chronological replay index for tests.
type MemoryStore struct {
	descriptor Descriptor
	dag        *dag.InMemoryCommitDag

	mu         sync.Mutex
	puts       []putEvent
	deletes    []deleteEvent
	seqCounter int64
}

// NewMemoryStore creates an in-memory index bound to a DAG.
func NewMemoryStore(descriptor Descriptor, d *dag.InMemoryCommitDag) *MemoryStore {
	return &MemoryStore{descriptor: descriptor, dag: d}
}

func (s *MemoryStore) Descriptor() Descriptor { return s.descriptor }

func (s *MemoryStore) Put(entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqCounter++
	s.puts = append(s.puts, putEvent{seq: s.seqCounter, entry: entry})
	return nil
}

func (s *MemoryStore) Delete(docID codec.UUID, atCommit codec.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqCounter++
	s.deletes = append(s.deletes, deleteEvent{seq: s.seqCounter, docID: docID, atCommit: atCommit})
	return nil
}

func (s *MemoryStore) BulkLoad(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	for _, e := range entries {
		s.seqCounter++
		s.puts = append(s.puts, putEvent{seq: s.seqCounter, entry: e})
	}
	return nil
}

func (s *MemoryStore) Rebuild(entries []Entry) error { return s.BulkLoad(entries) }

func (s *MemoryStore) Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	if s.descriptor.Type != IndexTypeHash && s.descriptor.Type != IndexTypeBTree {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeHash, s.descriptor.Type)
	}
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	buckets := s.replayBuckets(cutoff)
	if ids, ok := buckets[key]; ok {
		out := make([]codec.UUID, 0, len(ids))
		for id := range ids {
			out = append(out, id)
		}
		return out, nil
	}
	return nil, nil
}

func (s *MemoryStore) Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	if s.descriptor.Type != IndexTypeHash && s.descriptor.Type != IndexTypeBTree {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeHash, s.descriptor.Type)
	}
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	buckets := s.replayBuckets(cutoff)
	var keys []Key
	for k := range buckets {
		if from != nil && CompareKeys(k, from) < 0 {
			continue
		}
		if to != nil && CompareKeys(k, to) > 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		c := CompareKeys(keys[i], keys[j])
		if !ascending {
			return c > 0
		}
		return c < 0
	})
	seen := make(map[codec.UUID]struct{})
	var out []codec.UUID
outer:
	for _, k := range keys {
		for id := range buckets[k] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
			if len(out) >= limit {
				break outer
			}
		}
	}
	return out, nil
}

func (s *MemoryStore) Search(query string, atCommit *codec.Hash, limit int) ([]codec.UUID, error) {
	if s.descriptor.Type != IndexTypeFullText {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeFullText, s.descriptor.Type)
	}
	return nil, fmt.Errorf("fulltext search not implemented")
}

func (s *MemoryStore) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]RankedResult, error) {
	if s.descriptor.Type != IndexTypeVector {
		return nil, NewTypeMismatchError("index type mismatch", s.descriptor.FieldName, IndexTypeVector, s.descriptor.Type)
	}
	return nil, fmt.Errorf("vector search not implemented")
}

func (s *MemoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	return nil
}

func (s *MemoryStore) IsValid(atCommit codec.Hash) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dag.HasCommit(atCommit), nil
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := snapshotLines(s.puts, s.deletes)
	return []byte(strings.Join(lines, "\n")), nil
}

func (s *MemoryStore) RestoreSnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLocked()
	lines := strings.Split(string(data), "\n")
	return restoreSnapshotLines(lines, s.ingestPut, s.ingestDelete)
}

func (s *MemoryStore) ingestPut(entry Entry) {
	s.seqCounter++
	s.puts = append(s.puts, putEvent{seq: s.seqCounter, entry: entry})
}

func (s *MemoryStore) ingestDelete(docID codec.UUID, atCommit codec.Hash) {
	s.seqCounter++
	s.deletes = append(s.deletes, deleteEvent{seq: s.seqCounter, docID: docID, atCommit: atCommit})
}

func (s *MemoryStore) clearLocked() {
	s.puts = nil
	s.deletes = nil
	s.seqCounter = 0
}

func (s *MemoryStore) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return s.dag.Head()
}

func (s *MemoryStore) replayBuckets(cutoffHash codec.Hash) map[Key]map[codec.UUID]struct{} {
	buckets := make(map[Key]map[codec.UUID]struct{})
	events := make([]struct {
		seq int64
		put *putEvent
		del *deleteEvent
	}, 0, len(s.puts)+len(s.deletes))
	for i := range s.puts {
		events = append(events, struct {
			seq int64
			put *putEvent
			del *deleteEvent
		}{seq: s.puts[i].seq, put: &s.puts[i]})
	}
	for i := range s.deletes {
		events = append(events, struct {
			seq int64
			put *putEvent
			del *deleteEvent
		}{seq: s.deletes[i].seq, del: &s.deletes[i]})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })
	for _, evt := range events {
		if evt.put != nil {
			if !s.dag.IsAncestor(evt.put.entry.CommitHash, cutoffHash) {
				continue
			}
			if buckets[evt.put.entry.Key] == nil {
				buckets[evt.put.entry.Key] = make(map[codec.UUID]struct{})
			}
			buckets[evt.put.entry.Key][evt.put.entry.DocID] = struct{}{}
		}
		if evt.del != nil {
			if !s.dag.IsAncestor(evt.del.atCommit, cutoffHash) {
				continue
			}
			pruneDoc(buckets, evt.del.docID)
		}
	}
	return buckets
}

func pruneDoc(buckets map[Key]map[codec.UUID]struct{}, docID codec.UUID) {
	var dead []Key
	for k, ids := range buckets {
		delete(ids, docID)
		if len(ids) == 0 {
			dead = append(dead, k)
		}
	}
	for _, k := range dead {
		delete(buckets, k)
	}
}
