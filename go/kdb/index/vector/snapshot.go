package vector

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// SnapshotFormatVersion is bumped when the snapshot shape changes incompatibly.
const SnapshotFormatVersion = 1

// The snapshot holds every vector version and tombstone in sequence order; the graph is not
// persisted and is rebuilt from the vectors when first needed (§7).
type snapshotFile struct {
	FormatVersion int           `json:"formatVersion"`
	Kind          string        `json:"kind"`
	Field         string        `json:"field"`
	Dimensions    int           `json:"dimensions"`
	Metric        string        `json:"metric"`
	Seq           int64         `json:"seq"`
	Docs          []snapshotDoc `json:"docs"`
}

type snapshotDoc struct {
	DocID  string          `json:"docId"`
	Events []snapshotEvent `json:"events"`
}

type snapshotEvent struct {
	Seq       int64     `json:"seq"`
	Commit    string    `json:"commit"`
	Tombstone bool      `json:"tombstone,omitempty"`
	Vector    []float32 `json:"vector,omitempty"`
}

// Snapshot serialises the vectors and tombstones (§6.5).
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := snapshotFile{
		FormatVersion: SnapshotFormatVersion,
		Kind:          "vector",
		Field:         s.field,
		Dimensions:    s.dims,
		Metric:        s.metric.String(),
		Seq:           s.seq,
		Docs:          []snapshotDoc{},
	}
	for docID, dl := range s.docs {
		sd := snapshotDoc{DocID: docID.String()}
		for _, ev := range dl.events {
			se := snapshotEvent{Seq: ev.seq, Commit: ev.commit.Hex()}
			if ev.node == nil {
				se.Tombstone = true
			} else {
				se.Vector = ev.node.vec
			}
			sd.Events = append(sd.Events, se)
		}
		f.Docs = append(f.Docs, sd)
	}
	sort.Slice(f.Docs, func(i, j int) bool { return f.Docs[i].DocID < f.Docs[j].DocID })
	return json.Marshal(f)
}

// RestoreSnapshot replaces the index content with a Snapshot's, which must have the same
// field, dimensions and metric.
func (s *Store) RestoreSnapshot(data []byte) error {
	var f snapshotFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("vector snapshot: %w", err)
	}
	if f.FormatVersion != SnapshotFormatVersion || f.Kind != "vector" {
		return fmt.Errorf("vector snapshot: unsupported format (version %d, kind %q)", f.FormatVersion, f.Kind)
	}
	if f.Field != s.field || f.Dimensions != s.dims || f.Metric != s.metric.String() {
		return fmt.Errorf("vector snapshot: (%s, %d, %s) does not match index (%s, %d, %s)",
			f.Field, f.Dimensions, f.Metric, s.field, s.dims, s.metric)
	}
	type flatEvent struct {
		docID codec.UUID
		ev    snapshotEvent
	}
	var events []flatEvent
	for _, sd := range f.Docs {
		id, err := codec.UUIDFromString(sd.DocID)
		if err != nil {
			return fmt.Errorf("vector snapshot: %w", err)
		}
		for _, ev := range sd.Events {
			events = append(events, flatEvent{docID: id, ev: ev})
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ev.Seq < events[j].ev.Seq })

	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
	for _, fe := range events {
		commit, err := codec.HashFromHex(fe.ev.Commit)
		if err != nil {
			return fmt.Errorf("vector snapshot: %w", err)
		}
		var vec []float32
		if !fe.ev.Tombstone {
			if len(fe.ev.Vector) != s.dims {
				return fmt.Errorf("vector snapshot: event %d has %d dimensions, index has %d", fe.ev.Seq, len(fe.ev.Vector), s.dims)
			}
			vec = fe.ev.Vector
		}
		s.appendEventLocked(fe.docID, commit, vec)
	}
	if f.Seq > s.seq {
		s.seq = f.Seq
	}
	return nil
}

// SaveToDir writes the snapshot and manifest to dir (see index.SaveStoreSnapshot).
func (s *Store) SaveToDir(dir string, head codec.Hash) error {
	return index.SaveStoreSnapshot(dir, s, head)
}

// LoadFromDir restores the snapshot in dir and returns its manifest.
func (s *Store) LoadFromDir(dir string) (index.Manifest, error) {
	return index.LoadStoreSnapshot(dir, s)
}
