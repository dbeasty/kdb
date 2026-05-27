package index

import (
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// VersionedEngine replays index events with commit-ancestry cutoff (Component 12).
type VersionedEngine struct {
	dag *dag.InMemoryCommitDag

	mu         sync.Mutex
	puts       []putEvent
	deletes    []deleteEvent
	seqCounter int64
}

// NewVersionedEngine binds to a commit DAG.
func NewVersionedEngine(d *dag.InMemoryCommitDag) *VersionedEngine {
	return &VersionedEngine{dag: d}
}

func (e *VersionedEngine) Put(entry Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seqCounter++
	e.puts = append(e.puts, putEvent{seq: e.seqCounter, entry: entry})
	return nil
}

func (e *VersionedEngine) Delete(docID codec.UUID, atCommit codec.Hash) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seqCounter++
	e.deletes = append(e.deletes, deleteEvent{seq: e.seqCounter, docID: docID, atCommit: atCommit})
	return nil
}

func (e *VersionedEngine) BulkLoad(entries []Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clearLocked()
	for _, ent := range entries {
		e.seqCounter++
		e.puts = append(e.puts, putEvent{seq: e.seqCounter, entry: ent})
	}
	return nil
}

func (e *VersionedEngine) Clear() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clearLocked()
	return nil
}

func (e *VersionedEngine) Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	cutoff, err := e.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	buckets := e.replayBuckets(cutoff)
	if ids, ok := buckets[key]; ok {
		out := make([]codec.UUID, 0, len(ids))
		for id := range ids {
			out = append(out, id)
		}
		return out, nil
	}
	return nil, nil
}

func (e *VersionedEngine) Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	if limit <= 0 {
		limit = 1<<31 - 1
	}
	cutoff, err := e.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	buckets := e.replayBuckets(cutoff)
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

// HeadBuckets returns the bucket map at DAG head.
func (e *VersionedEngine) HeadBuckets() (map[Key]map[codec.UUID]struct{}, error) {
	cutoff, err := e.cutoff(nil)
	if err != nil {
		return nil, err
	}
	return e.replayBuckets(cutoff), nil
}

func (e *VersionedEngine) IsValid(atCommit codec.Hash) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dag.HasCommit(atCommit), nil
}

func (e *VersionedEngine) SnapshotBytes() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	lines := snapshotLines(e.puts, e.deletes)
	return []byte(strings.Join(lines, "\n")), nil
}

func (e *VersionedEngine) RestoreSnapshotBytes(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clearLocked()
	return restoreSnapshotLines(strings.Split(string(data), "\n"), e.ingestPut, e.ingestDelete)
}

func (e *VersionedEngine) ingestPut(entry Entry) {
	e.seqCounter++
	e.puts = append(e.puts, putEvent{seq: e.seqCounter, entry: entry})
}

func (e *VersionedEngine) ingestDelete(docID codec.UUID, atCommit codec.Hash) {
	e.seqCounter++
	e.deletes = append(e.deletes, deleteEvent{seq: e.seqCounter, docID: docID, atCommit: atCommit})
}

func (e *VersionedEngine) clearLocked() {
	e.puts = nil
	e.deletes = nil
	e.seqCounter = 0
}

func (e *VersionedEngine) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return e.dag.Head()
}

func (e *VersionedEngine) replayBuckets(cutoffHash codec.Hash) map[Key]map[codec.UUID]struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	buckets := make(map[Key]map[codec.UUID]struct{})
	events := make([]struct {
		seq int64
		put *putEvent
		del *deleteEvent
	}, 0, len(e.puts)+len(e.deletes))
	for i := range e.puts {
		events = append(events, struct {
			seq int64
			put *putEvent
			del *deleteEvent
		}{seq: e.puts[i].seq, put: &e.puts[i]})
	}
	for i := range e.deletes {
		events = append(events, struct {
			seq int64
			put *putEvent
			del *deleteEvent
		}{seq: e.deletes[i].seq, del: &e.deletes[i]})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })
	for _, evt := range events {
		if evt.put != nil {
			if !e.dag.IsAncestor(evt.put.entry.CommitHash, cutoffHash) {
				continue
			}
			if buckets[evt.put.entry.Key] == nil {
				buckets[evt.put.entry.Key] = make(map[codec.UUID]struct{})
			}
			buckets[evt.put.entry.Key][evt.put.entry.DocID] = struct{}{}
		}
		if evt.del != nil {
			if !e.dag.IsAncestor(evt.del.atCommit, cutoffHash) {
				continue
			}
			pruneDoc(buckets, evt.del.docID)
		}
	}
	return buckets
}
