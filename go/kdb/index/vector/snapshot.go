package vector

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// SnapshotFormatVersion is the format of a snapshot carrying the store's complete event history.
const SnapshotFormatVersion = 1

// snapshotFormatHeadOnly is the format written after history was dropped: one put per document
// visible at head, under the head commit, plus the commits of skipped events. A restore holds no
// history before that head.
const snapshotFormatHeadOnly = 2

// The snapshot holds every vector version and tombstone in sequence order while the store still
// has its whole history, and the head's vectors once it does not; the graph is not persisted and
// is rebuilt from the vectors when first needed (§7).
type snapshotFile struct {
	FormatVersion int           `json:"formatVersion"`
	Kind          string        `json:"kind"`
	Field         string        `json:"field"`
	Dimensions    int           `json:"dimensions"`
	Metric        string        `json:"metric"`
	Seq           int64         `json:"seq"`
	Docs          []snapshotDoc `json:"docs"`
	Pending       []string      `json:"pending,omitempty"`
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
	if h, err := s.dag.Head(); err == nil {
		s.advanceLocked(h)
	}
	if s.broken {
		return nil, index.ErrHistoryUnavailable
	}
	f := snapshotFile{
		FormatVersion: SnapshotFormatVersion,
		Kind:          "vector",
		Field:         s.field,
		Dimensions:    s.dims,
		Metric:        s.metric.String(),
		Seq:           s.seq,
		Docs:          []snapshotDoc{},
	}
	byDoc := make(map[codec.UUID]*snapshotDoc)
	add := func(doc codec.UUID, se snapshotEvent) {
		sd := byDoc[doc]
		if sd == nil {
			sd = &snapshotDoc{DocID: doc.String()}
			byDoc[doc] = sd
		}
		sd.Events = append(sd.Events, se)
	}
	if !s.dropped {
		for i := range s.events {
			e := &s.events[i]
			se := snapshotEvent{Seq: e.seq, Commit: e.commit.Hex()}
			if e.node == nil {
				se.Tombstone = true
			} else {
				se.Vector = e.node.vec
			}
			add(e.doc, se)
		}
	} else {
		f.FormatVersion = snapshotFormatHeadOnly
		head := s.lin.Cutoff().Hex()
		for doc, n := range s.head.base {
			add(doc, snapshotEvent{Seq: n.seq, Commit: head, Vector: n.vec})
		}
		for _, c := range s.lin.PendingCommits() {
			f.Pending = append(f.Pending, c.Hex())
		}
		sort.Strings(f.Pending)
	}
	for _, sd := range byDoc {
		f.Docs = append(f.Docs, *sd)
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
	if (f.FormatVersion != SnapshotFormatVersion && f.FormatVersion != snapshotFormatHeadOnly) || f.Kind != "vector" {
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
	if f.FormatVersion == snapshotFormatHeadOnly {
		for _, hex := range f.Pending {
			c, err := codec.HashFromHex(hex)
			if err != nil {
				return fmt.Errorf("vector snapshot: %w", err)
			}
			s.lin.MarkPending(c)
		}
		if h, err := s.dag.Head(); err == nil {
			s.advanceLocked(h)
		}
		if !s.broken && s.started {
			s.dropped = true
		}
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
