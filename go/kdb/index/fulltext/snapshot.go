package fulltext

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// SnapshotFormatVersion is the format of a snapshot carrying the store's complete event history.
const SnapshotFormatVersion = 1

// snapshotFormatHeadOnly is the format of a snapshot written after history was dropped: one put
// per document visible at head, under the head commit, plus the commits of skipped events. A
// restore holds no history before that head, so a search at an older commit reports
// index.ErrHistoryUnavailable.
const snapshotFormatHeadOnly = 2

// The snapshot is a self-describing JSON document: while the store still holds its whole event
// history (every analyzed version and tombstone, in sequence order), all of it, so that atCommit
// reads work after a restart exactly as before it; once history has been dropped, the head.
// Postings are rebuilt on restore.
type snapshotFile struct {
	FormatVersion int           `json:"formatVersion"`
	Kind          string        `json:"kind"`
	Fields        []string      `json:"fields"`
	Weights       []float64     `json:"weights"`
	Seq           int64         `json:"seq"`
	Docs          []snapshotDoc `json:"docs"`
	Pending       []string      `json:"pending,omitempty"`
}

type snapshotDoc struct {
	DocID  string          `json:"docId"`
	Events []snapshotEvent `json:"events"`
}

type snapshotEvent struct {
	Seq       int64           `json:"seq"`
	Commit    string          `json:"commit"`
	Tombstone bool            `json:"tombstone,omitempty"`
	Fields    []snapshotField `json:"fields,omitempty"`
}

type snapshotField struct {
	Length int            `json:"len"`
	Terms  []snapshotTerm `json:"terms"`
}

type snapshotTerm struct {
	Term      string `json:"t"`
	Positions []int  `json:"p"`
}

// Snapshot serialises the index (§6.5).
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
		Kind:          "fulltext",
		Fields:        append([]string(nil), s.fields...),
		Weights:       append([]float64(nil), s.weights...),
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
			add(e.doc, snapshotEventOf(e.seq, e.commit, e.ver))
		}
	} else {
		f.FormatVersion = snapshotFormatHeadOnly
		head := s.lin.Cutoff()
		for doc, ver := range s.head.base {
			add(doc, snapshotEventOf(ver.seq, head, ver))
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

func snapshotEventOf(seq int64, commit codec.Hash, ver *version) snapshotEvent {
	se := snapshotEvent{Seq: seq, Commit: commit.Hex()}
	if ver == nil {
		se.Tombstone = true
		return se
	}
	for _, af := range ver.fields {
		sf := snapshotField{Length: af.length, Terms: []snapshotTerm{}}
		for term, info := range af.terms {
			sf.Terms = append(sf.Terms, snapshotTerm{Term: term, Positions: info.positions})
		}
		sort.Slice(sf.Terms, func(i, j int) bool { return sf.Terms[i].Term < sf.Terms[j].Term })
		se.Fields = append(se.Fields, sf)
	}
	return se
}

// RestoreSnapshot replaces the index content with a Snapshot's. The snapshot must have been
// taken over the same fields.
func (s *Store) RestoreSnapshot(data []byte) error {
	var f snapshotFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("fulltext snapshot: %w", err)
	}
	if (f.FormatVersion != SnapshotFormatVersion && f.FormatVersion != snapshotFormatHeadOnly) || f.Kind != "fulltext" {
		return fmt.Errorf("fulltext snapshot: unsupported format (version %d, kind %q)", f.FormatVersion, f.Kind)
	}
	if !sameStrings(f.Fields, s.fields) {
		return fmt.Errorf("fulltext snapshot: fields %v do not match index fields %v", f.Fields, s.fields)
	}
	type flatEvent struct {
		docID codec.UUID
		ev    snapshotEvent
	}
	var events []flatEvent
	for _, sd := range f.Docs {
		id, err := codec.UUIDFromString(sd.DocID)
		if err != nil {
			return fmt.Errorf("fulltext snapshot: %w", err)
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
			return fmt.Errorf("fulltext snapshot: %w", err)
		}
		var ver *version
		if !fe.ev.Tombstone {
			if len(fe.ev.Fields) != len(s.fields) {
				return fmt.Errorf("fulltext snapshot: event %d has %d fields, index has %d", fe.ev.Seq, len(fe.ev.Fields), len(s.fields))
			}
			ver = &version{fields: make([]analyzedField, len(s.fields))}
			for i, sf := range fe.ev.Fields {
				af := analyzedField{length: sf.Length, terms: make(map[string]*termInfo, len(sf.Terms))}
				for _, st := range sf.Terms {
					af.terms[st.Term] = &termInfo{positions: append([]int(nil), st.Positions...)}
				}
				ver.fields[i] = af
				ver.total += af.length
			}
		}
		s.appendEventLocked(fe.docID, commit, ver)
	}
	if f.Seq > s.seq {
		s.seq = f.Seq
	}
	if f.FormatVersion == snapshotFormatHeadOnly {
		for _, hex := range f.Pending {
			c, err := codec.HashFromHex(hex)
			if err != nil {
				return fmt.Errorf("fulltext snapshot: %w", err)
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

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
