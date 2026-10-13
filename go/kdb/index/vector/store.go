// Package vector is the Layer 16 (§7) vector index: exact brute-force search as the oracle,
// an HNSW graph above exactThreshold live vectors, tombstones honouring atCommit, and a
// dimension guard that rejects a commit before anything is applied.
package vector

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/index"
)

// Defaults from §7.
const (
	DefaultM              = 16
	DefaultEfConstruction = 200
	DefaultEfSearch       = 64
	DefaultExactThreshold = 1000
)

// Options tunes a Store beyond its descriptor.
type Options struct {
	// ExactThreshold is the number of live vectors above which HNSW replaces exact search.
	// 0 means DefaultExactThreshold; a negative value forces HNSW at every size (tests).
	ExactThreshold int
	// HistoryEvents is how many of the most recent puts and tombstones are kept for searches at a
	// commit other than the head. 0 means DefaultHistoryEvents; a negative value keeps every
	// event forever (the unbounded behaviour this replaced).
	HistoryEvents int
}

// DefaultHistoryEvents is the default history window.
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

const viewCacheEntries = 8

type viewKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

const (
	evNew uint8 = iota
	evApplied
	evSkipped
)

// event is a put (node != nil) or a tombstone at a commit. Once applied to the head view, prev
// is the node the head showed for the document before it (its undo).
type event struct {
	seq    int64
	commit codec.Hash
	doc    codec.UUID
	node   *node
	prev   *node
	state  uint8
	cexact bool
	step   uint32
	cstep  uint32
}

// view is what is visible at one cutoff: base's node for each document, overridden by over
// (where present; nil hides the document).
type view struct {
	base map[codec.UUID]*node
	over map[codec.UUID]*node
	n    int
}

func (v *view) at(doc codec.UUID) *node {
	if v.over != nil {
		if n, ok := v.over[doc]; ok {
			return n
		}
	}
	return v.base[doc]
}

// each calls fn for every visible (document, node).
func (v *view) each(fn func(codec.UUID, *node)) {
	for doc, n := range v.base {
		if _, overridden := v.over[doc]; overridden {
			continue
		}
		fn(doc, n)
	}
	for doc, n := range v.over {
		if n != nil {
			fn(doc, n)
		}
	}
}

// Store is the vector index for one descriptor.
//
// Like the keyed and full-text indexes (see index.Lineage) it keeps a head view advanced in
// place and a bounded window of recent events, each recording the node it replaced; a search at
// an older commit undoes the events it must not see, and one the window cannot answer exactly
// reports index.ErrHistoryUnavailable. Every version is a graph node, and a node nothing can make
// visible any more is dead: once dead nodes are the majority the node list is compacted and the
// graph rebuilt from the live ones, so the graph follows the live vectors plus the window rather
// than every vector ever written.
type Store struct {
	desc           index.Descriptor
	dag            *dag.InMemoryCommitDag
	field          string
	dims           int
	metric         Metric
	m, efC, efS    int
	exactThreshold int
	maxEvents      int

	mu         sync.Mutex
	seq        int64
	events     []event
	lin        *index.Lineage
	started    bool
	pos        int
	head       view
	nodes      []*node
	dead       int
	graph      *hnsw
	dropped    bool
	broken     bool
	cache      map[viewKey]*view
	cacheOrder []viewKey
}

var _ index.DocumentStore = (*Store)(nil)

// NewVectorStore builds a store from the descriptor's options: "dimensions" (required, > 0),
// "metric" (default cosine), "m", "ef_construction", "ef_search".
func NewVectorStore(descriptor index.Descriptor, d *dag.InMemoryCommitDag, opts Options) (*Store, error) {
	if descriptor.Type != index.IndexTypeVector {
		return nil, index.NewTypeMismatchError("vector store requires a VECTOR descriptor", descriptor.FieldName, index.IndexTypeVector, descriptor.Type)
	}
	field := descriptor.FirstField()
	if field == "" {
		return nil, fmt.Errorf("vector index %s: no field", descriptor.IndexID)
	}
	dims, err := descriptor.IntOption(index.OptionDimensions, 0)
	if err != nil {
		return nil, err
	}
	if dims <= 0 {
		return nil, fmt.Errorf("vector index %s: option dimensions must be a positive integer", descriptor.IndexID)
	}
	metric, err := ParseMetric(descriptor.StringOption(index.OptionMetric, DefaultMetric.String()))
	if err != nil {
		return nil, err
	}
	m, err := descriptor.IntOption(index.OptionM, DefaultM)
	if err != nil {
		return nil, err
	}
	if m < 2 {
		return nil, fmt.Errorf("vector index %s: option m must be at least 2", descriptor.IndexID)
	}
	efC, err := descriptor.IntOption(index.OptionEfConstruction, DefaultEfConstruction)
	if err != nil {
		return nil, err
	}
	efS, err := descriptor.IntOption(index.OptionEfSearch, DefaultEfSearch)
	if err != nil {
		return nil, err
	}
	threshold := opts.ExactThreshold
	if threshold == 0 {
		threshold = DefaultExactThreshold
	}
	s := &Store{desc: descriptor, dag: d, field: field, dims: dims, metric: metric, m: m, efC: efC, efS: efS, exactThreshold: threshold, maxEvents: opts.maxEvents()}
	s.resetLocked()
	return s, nil
}

func (s *Store) resetLocked() {
	s.seq = 0
	s.events = nil
	s.lin = index.NewLineage(s.dag)
	s.started = false
	s.pos = 0
	s.head = view{base: make(map[codec.UUID]*node)}
	s.nodes = nil
	s.dead = 0
	s.graph = nil
	s.dropped, s.broken = false, false
	s.cache, s.cacheOrder = nil, nil
}

func (s *Store) Descriptor() index.Descriptor { return s.desc }

// Dimensions returns the configured vector length.
func (s *Store) Dimensions() int { return s.dims }

// Metric returns the configured similarity.
func (s *Store) Metric() Metric { return s.metric }

// Put indexes an entry whose Key is a VectorKey (the commit path's hint form) or a StringKey
// holding the document JSON.
func (s *Store) Put(entry index.Entry) error {
	switch k := entry.Key.(type) {
	case index.VectorKey:
		vec := k.AsFloat32()
		if len(vec) != s.dims {
			return index.NewDimensionMismatchError(s.field, s.dims, len(vec))
		}
		s.mu.Lock()
		s.appendEventLocked(entry.DocID, entry.CommitHash, vec)
		s.mu.Unlock()
		return nil
	case index.StringKey:
		return s.PutDocument(entry.DocID, entry.CommitHash, k.Value)
	default:
		return fmt.Errorf("vector index: Put expects a VectorKey or a StringKey holding the document JSON, got %T", entry.Key)
	}
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
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeHash, index.IndexTypeVector)
}

func (s *Store) Range(from, to index.Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeBTree, index.IndexTypeVector)
}

func (s *Store) Search(query string, atCommit *codec.Hash, limit int) ([]index.RankedResult, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeFullText, index.IndexTypeVector)
}

// PrepareDocument reads the vector at the indexed path. A missing or non-array value applies
// as a delete; a wrong-length array is a *index.DimensionMismatchError; a non-numeric element
// is a schema violation.
func (s *Store) PrepareDocument(docID codec.UUID, jsonText string) (index.PreparedPut, error) {
	root, err := index.ParseDocument(jsonText)
	if err != nil {
		return nil, err
	}
	vec, present, err := index.FieldVector(root, s.field)
	if err != nil {
		return nil, kdberr.NewSchemaViolationError(err.Error(), []kdberr.FieldViolation{{
			FieldName: s.field, ViolationType: kdberr.TypeMismatch, Detail: err.Error(),
		}})
	}
	if present && len(vec) != s.dims {
		return nil, index.NewDimensionMismatchError(s.field, s.dims, len(vec))
	}
	if !present {
		vec = nil
	}
	return &preparedPut{store: s, docID: docID, vec: vec}, nil
}

// PutDocument is PrepareDocument followed by Apply.
func (s *Store) PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error {
	p, err := s.PrepareDocument(docID, jsonText)
	if err != nil {
		return err
	}
	_, err = p.Apply(commitHash)
	return err
}

type preparedPut struct {
	store *Store
	docID codec.UUID
	vec   []float32
}

func (p *preparedPut) Apply(commitHash codec.Hash) (index.Hint, error) {
	s := p.store
	s.mu.Lock()
	s.appendEventLocked(p.docID, commitHash, p.vec)
	s.mu.Unlock()
	h := index.Hint{
		IndexID:    s.desc.IndexID,
		FieldName:  s.field,
		Type:       index.IndexTypeVector,
		Action:     index.HintActionDelete,
		DocID:      p.docID,
		CommitHash: commitHash,
	}
	if p.vec != nil {
		h.Action = index.HintActionPut
		h.Key = index.NewVectorKey(p.vec)
	}
	return h, nil
}

// appendEventLocked records a put (vec != nil) or tombstone. A put becomes a node; when the
// graph already exists the node is inserted incrementally.
func (s *Store) appendEventLocked(docID codec.UUID, commit codec.Hash, vec []float32) {
	if s.broken {
		return
	}
	s.seq++
	var n *node
	if vec != nil {
		n = &node{id: len(s.nodes), doc: docID, seq: s.seq, vec: append([]float32(nil), vec...), refs: 1}
		s.nodes = append(s.nodes, n)
		if s.graph != nil {
			s.graph.nodes = s.nodes
			s.graph.insert(n)
		}
	}
	s.events = append(s.events, event{seq: s.seq, commit: commit, doc: docID, node: n})
	s.cache, s.cacheOrder = nil, nil
	if s.maxEvents > 0 && len(s.events)-s.pos >= max(1, s.maxEvents/4) {
		if h, err := s.dag.Head(); err == nil {
			s.advanceLocked(h)
		}
	}
}

func (s *Store) ref(n *node) {
	if n != nil {
		n.refs++
	}
}

func (s *Store) unref(n *node) {
	if n == nil {
		return
	}
	n.refs--
	if n.refs == 0 {
		s.dead++
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

func (s *Store) applyLocked(e *event) {
	prev := s.head.base[e.doc]
	e.prev = prev
	s.ref(prev)
	if prev != nil {
		s.head.n--
	}
	if e.node == nil {
		delete(s.head.base, e.doc)
	} else {
		s.head.base[e.doc] = e.node
		s.ref(e.node)
		s.head.n++
	}
	s.unref(prev)
}

// resetHeadLocked rebuilds the head view at target by replaying the whole log, which is only
// possible while nothing has been dropped; otherwise the store is broken until a rebuild.
func (s *Store) resetHeadLocked(target codec.Hash) bool {
	if s.dropped {
		s.broken = true
		s.events = nil
		s.head = view{base: make(map[codec.UUID]*node)}
		s.nodes, s.dead, s.graph = nil, 0, nil
		s.lin.Clear()
		return false
	}
	for i := range s.events {
		s.unref(s.events[i].prev)
		s.events[i].prev = nil
	}
	for _, n := range s.head.base {
		s.unref(n)
	}
	s.head = view{base: make(map[codec.UUID]*node)}
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
	s.recountDeadLocked()
	s.truncateLocked()
	return true
}

func (s *Store) recountDeadLocked() {
	s.dead = 0
	for _, n := range s.nodes {
		if n.refs <= 0 {
			s.dead++
		}
	}
}

// truncateLocked drops the oldest processed events beyond the window, releasing the nodes only
// they kept alive, and compacts the graph once dead nodes are the majority.
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
				s.unref(e.node)
				s.unref(e.prev)
			}
			s.dropped = true
			s.events = s.events[:copy(s.events, s.events[drop:])]
			clear(s.events[len(s.events):cap(s.events)])
			s.pos -= drop
			s.cache, s.cacheOrder = nil, nil
			if s.dead > 64 && 2*s.dead > len(s.nodes) {
				s.compactNodesLocked()
			}
		}
	}
	s.lin.TrimSteps(keepFrom, s.maxEvents)
}

// compactNodesLocked drops dead nodes, renumbers the live ones and discards the graph, which the
// next approximate search rebuilds from the live nodes.
func (s *Store) compactNodesLocked() {
	live := s.nodes[:0]
	for _, n := range s.nodes {
		if n.refs > 0 {
			n.id = len(live)
			n.links = nil
			live = append(live, n)
		}
	}
	clear(s.nodes[len(live):])
	s.nodes = append([]*node(nil), live...)
	s.dead = 0
	s.graph = nil
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
// events it must not see (see index.Lineage).
func (s *Store) historicalLocked(cutoff codec.Hash) (*view, bool) {
	p, ok := s.lin.Locate(cutoff, !s.dropped)
	if !ok {
		return nil, false
	}
	over := make(map[codec.UUID]*node)
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
			over[e.doc] = e.prev
		default:
			return nil, false
		}
	}
	v := &view{base: s.head.base, over: over, n: s.head.n}
	for doc, n := range over {
		if s.head.base[doc] != nil {
			v.n--
		}
		if n != nil {
			v.n++
		}
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
	v := &view{base: make(map[codec.UUID]*node)}
	for i := range s.events {
		e := &s.events[i]
		if _, ok := anc[e.commit]; !ok {
			continue
		}
		if e.node == nil {
			delete(v.base, e.doc)
		} else {
			v.base[e.doc] = e.node
		}
	}
	v.n = len(v.base)
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

// LiveCount returns the number of vectors visible at atCommit (nil = head).
func (s *Store) LiveCount(atCommit *codec.Hash) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.viewLocked(atCommit)
	if err != nil {
		return 0, err
	}
	return v.n, nil
}

// NearestNeighbours returns the k best-scoring vectors visible at atCommit (nil = head),
// score descending then document id ascending. Exact search is used up to exactThreshold
// live vectors, HNSW above it.
func (s *Store) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]index.RankedResult, error) {
	if len(queryVector) != s.dims {
		return nil, index.NewDimensionMismatchError(s.field, s.dims, len(queryVector))
	}
	if k <= 0 {
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
	if s.exactThreshold >= 0 && v.n <= s.exactThreshold {
		return s.exact(queryVector, k, v), nil
	}
	return s.approximate(queryVector, k, v), nil
}

// ExactNearestNeighbours is the brute-force oracle regardless of size.
func (s *Store) ExactNearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]index.RankedResult, error) {
	if len(queryVector) != s.dims {
		return nil, index.NewDimensionMismatchError(s.field, s.dims, len(queryVector))
	}
	if k <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.viewLocked(atCommit)
	if err != nil {
		return nil, err
	}
	return s.exact(queryVector, k, v), nil
}

func (s *Store) exact(q []float32, k int, v *view) []index.RankedResult {
	out := make([]index.RankedResult, 0, v.n)
	v.each(func(docID codec.UUID, n *node) {
		out = append(out, index.RankedResult{DocID: docID, Score: Score(s.metric, q, n.vec)})
	})
	index.SortRanked(out)
	if len(out) > k {
		out = out[:k]
	}
	return out
}

// approximate runs the HNSW search; s.mu is held.
func (s *Store) approximate(q []float32, k int, v *view) []index.RankedResult {
	if s.graph == nil {
		g := newHNSW(s.metric, s.m, s.efC)
		g.nodes = s.nodes
		for _, n := range s.nodes {
			g.insert(n)
		}
		s.graph = g
	}
	g := s.graph
	g.nodes = s.nodes
	accept := func(n *node) bool { return v.at(n.doc) == n }
	hits := g.search(q, k, s.efS, accept)
	out := make([]index.RankedResult, 0, len(hits))
	for _, h := range hits {
		n := s.nodes[h.id]
		out = append(out, index.RankedResult{DocID: n.doc, Score: float32(h.score)})
	}
	index.SortRanked(out)
	return out
}

// storeStats reports the store's size, for tests.
type storeStats struct {
	Events, Nodes, Dead, Visible int
	Dropped, Broken              bool
}

func (s *Store) stats() storeStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storeStats{Events: len(s.events), Nodes: len(s.nodes), Dead: s.dead, Visible: len(s.head.base), Dropped: s.dropped, Broken: s.broken}
}
