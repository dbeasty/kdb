package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// VersionedEngine replays index events with commit-ancestry cutoff (Component 12).
type VersionedEngine struct {
	log *eventLog
}

// NewVersionedEngine binds to a commit DAG.
func NewVersionedEngine(d *dag.InMemoryCommitDag) *VersionedEngine {
	return &VersionedEngine{log: newEventLog(d)}
}

func (e *VersionedEngine) Put(entry Entry) error {
	e.log.put(entry)
	return nil
}

func (e *VersionedEngine) Delete(docID codec.UUID, atCommit codec.Hash) error {
	e.log.delete(docID, atCommit)
	return nil
}

func (e *VersionedEngine) BulkLoad(entries []Entry) error {
	e.log.bulkLoad(entries)
	return nil
}

func (e *VersionedEngine) Clear() error {
	e.log.clear()
	return nil
}

func (e *VersionedEngine) Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	cutoff, err := e.log.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	return e.log.lookup(key, cutoff), nil
}

func (e *VersionedEngine) Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	cutoff, err := e.log.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	return e.log.rangeScan(from, to, cutoff, limit, ascending), nil
}

// HeadBuckets returns the bucket map at DAG head. The caller gets its own copy: the replayed
// map itself is memoized and shared between lookups.
func (e *VersionedEngine) HeadBuckets() (map[Key]map[codec.UUID]struct{}, error) {
	cutoff, err := e.log.cutoff(nil)
	if err != nil {
		return nil, err
	}
	return e.log.bucketsCopy(cutoff), nil
}

func (e *VersionedEngine) IsValid(atCommit codec.Hash) (bool, error) {
	return e.log.hasCommit(atCommit), nil
}

func (e *VersionedEngine) SnapshotBytes() ([]byte, error) {
	return e.log.snapshotBytes(), nil
}

func (e *VersionedEngine) RestoreSnapshotBytes(data []byte) error {
	return e.log.restoreSnapshotBytes(data)
}
