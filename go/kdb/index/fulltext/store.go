// Package fulltext is the Layer 16 (§6) scored full-text index: a multi-field, weighted
// inverted index with positions, BM25F-lite scoring, OR semantics with quoted phrases, and
// commit-ancestry visibility so a Search atCommit sees exactly the documents that commit saw.
package fulltext

import (
	"fmt"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/analyzer"
	"github.com/limidus/kdb/go/kdb/json"
)

// BM25 parameters fixed by the spec (§6.4).
const (
	K1 = 1.2
	B  = 0.75
)

// Options tunes a Store. The spec fixes every scoring parameter; what is left is how much
// history the store keeps.
type Options struct {
	// HistoryEvents is how many of the most recent puts and tombstones are kept for searches at a
	// commit other than the head. 0 means DefaultHistoryEvents; a negative value keeps every
	// event forever (the unbounded behaviour this replaced).
	HistoryEvents int
}

// DefaultHistoryEvents is the default history window. A full-text event holds a whole analyzed
// document version, so the window is much shorter than a keyed index's.
const DefaultHistoryEvents = 4096

func (o Options) maxEvents() int {
	switch {
	case o.HistoryEvents == 0:
		return DefaultHistoryEvents
	case o.HistoryEvents < 0:
		return 0
	default:
		return o.HistoryEvents
	}
}

// viewCacheEntries caps the memoized replayed views while the log is complete.
const viewCacheEntries = 8

type viewKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

// termInfo is one (document version, field, term) posting: tf and positions.
type termInfo struct {
	positions []int
}

// analyzedField is one field of one document version.
type analyzedField struct {
	length int
	terms  map[string]*termInfo
}

// version is one put of a document: its analyzed fields.
type version struct {
	seq    int64
	fields []analyzedField
	total  int
	// refs counts what can still make the version visible somewhere: being the head's version of
	// its document, and every retained event that puts it or records it as the version it
	// replaced. At zero its postings are dead and the next postings compaction drops them.
	refs int32
}

func (v *version) postingCount() int {
	n := 0
	for f := range v.fields {
		n += len(v.fields[f].terms)
	}
	return n
}

// posting points from a term to one field of one document version.
type posting struct {
	doc   codec.UUID
	ver   *version
	field int
	info  *termInfo
}

const (
	evNew uint8 = iota
	evApplied
	evSkipped
)

// event is a put (ver != nil) or a tombstone (ver == nil) at a commit. Once applied to the head
// view, prev is the version the head showed for the document before it (its undo).
type event struct {
	seq    int64
	commit codec.Hash
	doc    codec.UUID
	ver    *version
	prev   *version
	state  uint8
	cexact bool
	step   uint32
	cstep  uint32
}

// view is what is visible at one cutoff: base's version of each document, overridden by over
// (where present; a nil value hides the document), plus N and per-field total lengths.
type view struct {
	base       map[codec.UUID]*version
	over       map[codec.UUID]*version
	n          int
	fieldTotal []int
}

func (v *view) at(doc codec.UUID) *version {
	if v.over != nil {
		if ver, ok := v.over[doc]; ok {
			return ver
		}
	}
	return v.base[doc]
}

// Store is the full-text index for one descriptor.
//
// Like the keyed index (see index.Lineage) it keeps a head view - the version of each document
// at the head, N and the field totals - advanced in place as events arrive, and a bounded window
// of recent events, each recording the version it replaced. A search at an older commit undoes
// the window's events it must not see; one the window cannot answer exactly reports
// index.ErrHistoryUnavailable. Versions nothing can make visible any more are dropped, and their
// postings with them, so memory follows the live corpus plus the window rather than every
// version ever written.
type Store struct {
	desc      index.Descriptor
	dag       *dag.InMemoryCommitDag
	fields    []string
	weights   []float64
	maxEvents int

	mu       sync.Mutex
	seq      int64
	events   []event
	lin      *index.Lineage
	started  bool
	pos      int // events[:pos] are processed into the head view
	head     view
	postings map[string][]*posting
	// totalPostings counts the entries in postings; deadPostings those whose version's refs fell
	// to zero. Postings are compacted once the dead ones are the majority.
	totalPostings int
	deadPostings  int
	dropped       bool
	broken        bool
	cache         map[viewKey]*view
	cacheOrder    []viewKey
}

var _ index.DocumentStore = (*Store)(nil)

// NewFullTextStore builds a store over descriptor's fields (Descriptor.Fields, or FieldName)
// with weights from Options["weights"].
func NewFullTextStore(descriptor index.Descriptor, d *dag.InMemoryCommitDag, opts Options) (*Store, error) {
	if descriptor.Type != index.IndexTypeFullText {
		return nil, index.NewTypeMismatchError("fulltext store requires a FULLTEXT descriptor", descriptor.FieldName, index.IndexTypeFullText, descriptor.Type)
	}
	fields := descriptor.FieldPaths()
	if len(fields) == 0 {
		return nil, fmt.Errorf("fulltext index %s: no fields", descriptor.IndexID)
	}
	weights, err := descriptor.FieldWeights()
	if err != nil {
		return nil, err
	}
	s := &Store{desc: descriptor, dag: d, fields: fields, weights: weights, maxEvents: opts.maxEvents()}
	s.resetLocked()
	return s, nil
}

func (s *Store) resetLocked() {
	s.seq = 0
	s.events = nil
	s.lin = index.NewLineage(s.dag)
	s.started = false
	s.pos = 0
	s.head = view{base: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
	s.postings = make(map[string][]*posting)
	s.totalPostings, s.deadPostings = 0, 0
	s.dropped, s.broken = false, false
	s.cache, s.cacheOrder = nil, nil
}

func (s *Store) Descriptor() index.Descriptor { return s.desc }

// Fields returns the indexed paths in descriptor order.
func (s *Store) Fields() []string { return append([]string(nil), s.fields...) }

// Weights returns the per-field weights in descriptor order.
func (s *Store) Weights() []float64 { return append([]float64(nil), s.weights...) }

// Put indexes an entry whose Key is the document's JSON text as a StringKey (that is what
// the commit path's hints carry for a full-text index).
func (s *Store) Put(entry index.Entry) error {
	sk, ok := entry.Key.(index.StringKey)
	if !ok {
		return fmt.Errorf("fulltext index: Put expects a StringKey holding the document JSON, got %T", entry.Key)
	}
	return s.PutDocument(entry.DocID, entry.CommitHash, sk.Value)
}

func (s *Store) Delete(docID codec.UUID, atCommit codec.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendEventLocked(docID, atCommit, nil)
	return nil
}

func (s *Store) BulkLoad(entries []index.Entry) error {
	if err := s.Clear(); err != nil {
		return err
	}
	for _, e := range entries {
		if err := s.Put(e); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Rebuild(entries []index.Entry) error { return s.BulkLoad(entries) }

func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
	return nil
}

func (s *Store) IsValid(atCommit codec.Hash) (bool, error) { return s.dag.HasCommit(atCommit), nil }

func (s *Store) Lookup(key index.Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeHash, index.IndexTypeFullText)
}

func (s *Store) Range(from, to index.Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeBTree, index.IndexTypeFullText)
}

func (s *Store) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]index.RankedResult, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeVector, index.IndexTypeFullText)
}

// PrepareDocument analyzes the document's indexed fields without touching the index.
func (s *Store) PrepareDocument(docID codec.UUID, jsonText string) (index.PreparedPut, error) {
	root, err := index.ParseDocument(jsonText)
	if err != nil {
		return nil, err
	}
	return s.PrepareParsedDocument(docID, jsonText, root)
}

// PrepareParsedDocument implements index.ParsedDocumentStore.
func (s *Store) PrepareParsedDocument(docID codec.UUID, jsonText string, root kdbjson.Value) (index.PreparedPut, error) {
	return &preparedPut{store: s, docID: docID, jsonText: jsonText, ver: s.analyze(root)}, nil
}

// PutDocument analyzes and indexes a JSON document at commitHash.
func (s *Store) PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error {
	p, err := s.PrepareDocument(docID, jsonText)
	if err != nil {
		return err
	}
	_, err = p.Apply(commitHash)
	return err
}

type preparedPut struct {
	store    *Store
	docID    codec.UUID
	jsonText string
	ver      *version
}

func (p *preparedPut) Apply(commitHash codec.Hash) (index.Hint, error) {
	s := p.store
	s.mu.Lock()
	s.appendEventLocked(p.docID, commitHash, p.ver)
	s.mu.Unlock()
	return index.Hint{
		IndexID:    s.desc.IndexID,
		FieldName:  s.desc.FirstField(),
		Type:       index.IndexTypeFullText,
		Action:     index.HintActionPut,
		DocID:      p.docID,
		Key:        index.StringKey{Value: p.jsonText},
		CommitHash: commitHash,
	}, nil
}

// analyze builds a version from a parsed document. An array field contributes every string
// element, each analyzed on its own, with a position gap of one between elements so a phrase
// never spans two elements: element tokens start at (previous end + 2).
func (s *Store) analyze(root json.Value) *version {
	v := &version{fields: make([]analyzedField, len(s.fields))}
	for i, path := range s.fields {
		af := analyzedField{terms: make(map[string]*termInfo)}
		offset := 0
		for _, text := range index.FieldStrings(root, path) {
			toks := analyzer.Analyze(text)
			for _, t := range toks {
				ti := af.terms[t.Term]
				if ti == nil {
					ti = &termInfo{}
					af.terms[t.Term] = ti
				}
				ti.positions = append(ti.positions, offset+t.Position)
			}
			af.length += len(toks)
			offset += len(toks) + 1
		}
		v.fields[i] = af
		v.total += af.length
	}
	return v
}

func (s *Store) appendEventLocked(docID codec.UUID, commit codec.Hash, ver *version) {
	if s.broken {
		// Nothing can be answered until a rebuild, which clears the store first.
		return
	}
	s.seq++
	if ver != nil {
		ver.seq = s.seq
		s.ref(ver)
		for f := range ver.fields {
			for term, info := range ver.fields[f].terms {
				s.postings[term] = append(s.postings[term], &posting{doc: docID, ver: ver, field: f, info: info})
				s.totalPostings++
			}
		}
	}
	s.events = append(s.events, event{seq: s.seq, commit: commit, doc: docID, ver: ver})
	s.cache, s.cacheOrder = nil, nil
	if s.maxEvents > 0 && len(s.events)-s.pos >= max(1, s.maxEvents/4) {
		if h, err := s.dag.Head(); err == nil {
			s.advanceLocked(h)
		}
	}
}

func (s *Store) ref(v *version) {
	if v != nil {
		v.refs++
	}
}

func (s *Store) unref(v *version) {
	if v == nil {
		return
	}
	v.refs--
	if v.refs == 0 {
		s.deadPostings += v.postingCount()
	}
}

// contribution adds (sign 1) or removes (sign -1) a visible version's share of N and the field
// totals. A document that contributes no tokens is not counted in N.
func contribution(v *view, ver *version, sign int) {
	if ver == nil || ver.total == 0 {
		return
	}
	v.n += sign
	for f := range ver.fields {
		v.fieldTotal[f] += sign * ver.fields[f].length
	}
}

// advanceLocked brings the head view to target and applies every logged event to it.
func (s *Store) advanceLocked(target codec.Hash) bool {
	if s.broken {
		return false
	}
	if !s.started || !s.lin.Advance(target) {
		return s.resetHeadLocked(target)
	}
	if s.pos < len(s.events) {
		step := s.lin.Step()
		for i := s.pos; i < len(s.events); i++ {
			e := &s.events[i]
			in, cstep, exact := s.lin.Classify(e.commit)
			if !in {
				e.state = evSkipped
				continue
			}
			e.state, e.step, e.cstep, e.cexact = evApplied, step, cstep, exact
			s.applyLocked(e)
		}
		s.pos = len(s.events)
		s.lin.DoneClassifying()
	}
	s.truncateLocked()
	return true
}

// applyLocked makes e's version the head's version of its document, recording the one it
// replaces.
func (s *Store) applyLocked(e *event) {
	prev := s.head.base[e.doc]
	e.prev = prev
	s.ref(prev)
	contribution(&s.head, prev, -1)
	if e.ver == nil {
		delete(s.head.base, e.doc)
	} else {
		s.head.base[e.doc] = e.ver
		s.ref(e.ver)
		contribution(&s.head, e.ver, 1)
	}
	s.unref(prev)
}

// resetHeadLocked rebuilds the head view at target by replaying the whole log, which is only
// possible while nothing has been dropped; otherwise the store is broken until a rebuild.
func (s *Store) resetHeadLocked(target codec.Hash) bool {
	if s.dropped {
		s.broken = true
		for i := range s.events {
			s.unref(s.events[i].ver)
			s.unref(s.events[i].prev)
		}
		for _, v := range s.head.base {
			s.unref(v)
		}
		s.events = nil
		s.head = view{base: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
		s.lin.Clear()
		s.compactPostingsLocked(true)
		return false
	}
	for i := range s.events {
		s.unref(s.events[i].prev)
		s.events[i].prev = nil
	}
	for _, v := range s.head.base {
		s.unref(v)
	}
	s.head = view{base: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
	s.lin.Start(target)
	s.lin.ClearPending()
	s.started = true
	anc := s.dag.AncestorSet(target)
	for i := range s.events {
		e := &s.events[i]
		if _, in := anc[e.commit]; !in {
			e.state = evSkipped
			s.lin.MarkPending(e.commit)
			continue
		}
		e.state, e.step, e.cstep, e.cexact = evApplied, 0, 0, true
		s.applyLocked(e)
	}
	s.pos = len(s.events)
	s.truncateLocked()
	return true
}

// truncateLocked drops the oldest processed events beyond the window, releasing the versions
// only they kept alive.
func (s *Store) truncateLocked() {
	if s.maxEvents <= 0 || !s.started {
		return
	}
	keepFrom := uint32(0)
	if len(s.events) > s.maxEvents {
		drop := min(len(s.events)-s.maxEvents*3/4, s.pos)
		if drop > 0 {
			for i := 0; i < drop; i++ {
				e := &s.events[i]
				if e.state == evApplied && e.step > keepFrom {
					keepFrom = e.step
				}
				s.unref(e.ver)
				s.unref(e.prev)
			}
			s.dropped = true
			s.events = s.events[:copy(s.events, s.events[drop:])]
			clear(s.events[len(s.events):cap(s.events)])
			s.pos -= drop
			s.cache, s.cacheOrder = nil, nil
			s.compactPostingsLocked(false)
		}
	}
	s.lin.TrimSteps(keepFrom, s.maxEvents)
}

// compactPostingsLocked drops the postings of dead versions once they are the majority (or
// always, with force).
func (s *Store) compactPostingsLocked(force bool) {
	if !force && (s.deadPostings < 1024 || s.deadPostings*2 < s.totalPostings) {
		return
	}
	total := 0
	for term, list := range s.postings {
		kept := list[:0]
		for _, p := range list {
			if p.ver.refs > 0 {
				kept = append(kept, p)
			}
		}
		clear(list[len(kept):])
		if len(kept) == 0 {
			delete(s.postings, term)
			continue
		}
		if cap(kept) > 2*len(kept)+8 {
			kept = append([]*posting(nil), kept...)
		}
		s.postings[term] = kept
		total += len(kept)
	}
	s.totalPostings = total
	s.deadPostings = 0
}

// viewLocked resolves the view a search at atCommit (nil = head) reads.
func (s *Store) viewLocked(atCommit *codec.Hash) (*view, error) {
	head, err := s.dag.Head()
	if err != nil {
		return nil, err
	}
	s.advanceLocked(head)
	if s.broken {
		return nil, index.ErrHistoryUnavailable
	}
	cutoff := head
	if atCommit != nil {
		cutoff = *atCommit
	}
	if cutoff == s.lin.Cutoff() {
		return &s.head, nil
	}
	if v, ok := s.historicalLocked(cutoff); ok {
		return v, nil
	}
	if !s.dropped {
		return s.replayLocked(cutoff), nil
	}
	return nil, index.ErrHistoryUnavailable
}

// historicalLocked answers cutoff from the head view by undoing, newest first, the applied
// events it must not see - exact when those are a suffix of the applied events (see
// index.Lineage).
func (s *Store) historicalLocked(cutoff codec.Hash) (*view, bool) {
	p, ok := s.lin.Locate(cutoff, !s.dropped)
	if !ok {
		return nil, false
	}
	over := make(map[codec.UUID]*version)
	sawVisible := false
scan:
	for i := len(s.events) - 1; i >= 0; i-- {
		e := &s.events[i]
		if e.state != evApplied {
			continue
		}
		switch p.Visible(e.commit, e.step, e.cstep, e.cexact) {
		case index.VisibleAndOlder:
			break scan
		case index.Visible:
			sawVisible = true
		case index.Invisible:
			if sawVisible {
				return nil, false
			}
			over[e.doc] = e.prev // newest first: the oldest undo wins
		default:
			return nil, false
		}
	}
	v := &view{base: s.head.base, over: over, n: s.head.n, fieldTotal: append([]int(nil), s.head.fieldTotal...)}
	for doc, ver := range over {
		contribution(v, s.head.base[doc], -1)
		contribution(v, ver, 1)
	}
	return v, true
}

// replayLocked builds the view at cutoff from the whole (complete) log: the last put or
// tombstone, in log order, whose commit is an ancestor of the cutoff.
func (s *Store) replayLocked(cutoff codec.Hash) *view {
	key := viewKey{cutoff: cutoff, seq: s.seq, ancestryVersion: s.dag.AncestryVersion()}
	if v, ok := s.cache[key]; ok {
		return v
	}
	anc := s.dag.AncestorSet(cutoff)
	v := &view{base: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
	for i := range s.events {
		e := &s.events[i]
		if _, ok := anc[e.commit]; !ok {
			continue
		}
		contribution(v, v.base[e.doc], -1)
		if e.ver == nil {
			delete(v.base, e.doc)
			continue
		}
		v.base[e.doc] = e.ver
		contribution(v, e.ver, 1)
	}
	if s.cache == nil {
		s.cache = make(map[viewKey]*view, viewCacheEntries)
	}
	s.cache[key] = v
	s.cacheOrder = append(s.cacheOrder, key)
	if len(s.cacheOrder) > viewCacheEntries {
		delete(s.cache, s.cacheOrder[0])
		s.cacheOrder = s.cacheOrder[1:]
	}
	return v
}

// NeedsRebuild brings the head view to the DAG head and reports whether it could not be (see
// index.MemoryStore.NeedsRebuild).
func (s *Store) NeedsRebuild() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, err := s.dag.Head(); err == nil {
		s.advanceLocked(h)
	}
	return s.broken
}

// ForgetHistory records that the content was just rebuilt from the documents at head, so a
// search at an older commit must not replay it as history.
func (s *Store) ForgetHistory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, err := s.dag.Head(); err == nil {
		s.advanceLocked(h)
	}
	if !s.broken && s.started {
		s.dropped = true
		s.cache, s.cacheOrder = nil, nil
	}
}

// Stats are the corpus statistics BM25 scoring reads at one cutoff.
type Stats struct {
	N      int
	AvgLen map[string]float64
}

// Stats returns N and per-field avglen at atCommit (nil = head).
func (s *Store) Stats(atCommit *codec.Hash) (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.viewLocked(atCommit)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{N: v.n, AvgLen: make(map[string]float64, len(s.fields))}
	for f, path := range s.fields {
		st.AvgLen[path] = avgLen(v, f)
	}
	return st, nil
}

// SnapshotStats publishes head statistics into the persisted manifest.
func (s *Store) SnapshotStats() map[string]float64 {
	st, err := s.Stats(nil)
	if err != nil {
		return nil
	}
	out := map[string]float64{"N": float64(st.N)}
	for path, a := range st.AvgLen {
		out["avglen."+path] = a
	}
	return out
}

// avgLen is the field's average length over the N indexed documents; a document that
// contributes no tokens to the field counts as length 0.
func avgLen(v *view, field int) float64 {
	if v.n == 0 {
		return 0
	}
	return float64(v.fieldTotal[field]) / float64(v.n)
}

// storeStats reports the store's size, for tests.
type storeStats struct {
	Events, Postings, DeadPostings, Visible int
	Dropped, Broken                         bool
}

func (s *Store) stats() storeStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storeStats{Events: len(s.events), Postings: s.totalPostings, DeadPostings: s.deadPostings,
		Visible: len(s.head.base), Dropped: s.dropped, Broken: s.broken}
}

// Query is a parsed search string: the distinct analyzed terms (free and phrase terms alike)
// and the phrases that must occur contiguously.
type Query struct {
	Terms   []string
	Phrases [][]string
}

// ParseQuery analyzes a query string. Text inside double quotes is a phrase (an unterminated
// quote runs to the end); everything else contributes free terms. Terms are deduplicated in
// first-seen order. A phrase that analyzes to nothing (stopwords only) imposes no constraint.
func ParseQuery(query string) Query {
	var q Query
	seen := make(map[string]struct{})
	add := func(terms []string) {
		for _, t := range terms {
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			q.Terms = append(q.Terms, t)
		}
	}
	rest := query
	for {
		open := strings.IndexByte(rest, '"')
		if open < 0 {
			add(analyzer.Terms(rest))
			break
		}
		add(analyzer.Terms(rest[:open]))
		rest = rest[open+1:]
		close := strings.IndexByte(rest, '"')
		var phraseText string
		if close < 0 {
			phraseText, rest = rest, ""
		} else {
			phraseText, rest = rest[:close], rest[close+1:]
		}
		phrase := analyzer.Terms(phraseText)
		add(phrase)
		if len(phrase) > 0 {
			q.Phrases = append(q.Phrases, phrase)
		}
	}
	return q
}

// Search ranks the documents visible at atCommit (nil = head) that contain at least one query
// term and every quoted phrase, by BM25F-lite score descending then document id ascending.
// limit <= 0 returns every hit.
func (s *Store) Search(query string, atCommit *codec.Hash, limit int) ([]index.RankedResult, error) {
	q := ParseQuery(query)
	if len(q.Terms) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.viewLocked(atCommit)
	if err != nil {
		return nil, err
	}
	if v.n == 0 {
		return nil, nil
	}
	scores := make(map[codec.UUID]float64)
	var order []codec.UUID
	var live []*posting
	for _, term := range q.Terms {
		live = live[:0]
		docsWithTerm := make(map[codec.UUID]struct{})
		for _, p := range s.postings[term] {
			if v.at(p.doc) != p.ver {
				continue
			}
			live = append(live, p)
			docsWithTerm[p.doc] = struct{}{}
		}
		if len(live) == 0 {
			continue
		}
		nt := float64(len(docsWithTerm))
		idf := math.Log(1 + (float64(v.n)-nt+0.5)/(nt+0.5))
		for _, p := range live {
			tf := float64(len(p.info.positions))
			length := float64(p.ver.fields[p.field].length)
			norm := tf * (K1 + 1) / (tf + K1*(1-B+B*length/avgLen(v, p.field)))
			if _, seen := scores[p.doc]; !seen {
				order = append(order, p.doc)
			}
			scores[p.doc] += s.weights[p.field] * idf * norm
		}
	}

	out := make([]index.RankedResult, 0, len(order))
	for _, docID := range order {
		if !matchesPhrases(v.at(docID), q.Phrases) {
			continue
		}
		out = append(out, index.RankedResult{DocID: docID, Score: float32(scores[docID])})
	}
	index.SortRanked(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// matchesPhrases reports whether every phrase occurs contiguously in some field of ver.
func matchesPhrases(ver *version, phrases [][]string) bool {
	for _, phrase := range phrases {
		found := false
		for f := range ver.fields {
			if fieldHasPhrase(&ver.fields[f], phrase) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func fieldHasPhrase(af *analyzedField, phrase []string) bool {
	first := af.terms[phrase[0]]
	if first == nil {
		return false
	}
	for _, start := range first.positions {
		ok := true
		for i := 1; i < len(phrase); i++ {
			ti := af.terms[phrase[i]]
			if ti == nil || !hasPosition(ti.positions, start+i) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func hasPosition(positions []int, p int) bool {
	i := sort.SearchInts(positions, p)
	return i < len(positions) && positions[i] == p
}
