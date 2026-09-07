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
}

const viewCacheEntries = 8

type viewKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

type docEvent struct {
	seq    int64
	commit codec.Hash
	node   *node // nil = tombstone
}

type docLog struct {
	events []docEvent
}

type view struct {
	visible map[codec.UUID]*node
}

// Store is the vector index for one descriptor.
type Store struct {
	desc           index.Descriptor
	dag            *dag.InMemoryCommitDag
	field          string
	dims           int
	metric         Metric
	m, efC, efS    int
	exactThreshold int

	mu         sync.Mutex
	seq        int64
	docs       map[codec.UUID]*docLog
	nodes      []*node
	graph      *hnsw
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
	s := &Store{desc: descriptor, dag: d, field: field, dims: dims, metric: metric, m: m, efC: efC, efS: efS, exactThreshold: threshold}
	s.resetLocked()
	return s, nil
}

func (s *Store) resetLocked() {
	s.seq = 0
	s.docs = make(map[codec.UUID]*docLog)
	s.nodes = nil
	s.graph = nil
	s.cache = nil
	s.cacheOrder = nil
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
	s.seq++
	dl := s.docs[docID]
	if dl == nil {
		dl = &docLog{}
		s.docs[docID] = dl
	}
	var n *node
	if vec != nil {
		n = &node{id: len(s.nodes), doc: docID, seq: s.seq, vec: append([]float32(nil), vec...)}
		s.nodes = append(s.nodes, n)
		if s.graph != nil {
			s.graph.insert(n)
		}
	}
	dl.events = append(dl.events, docEvent{seq: s.seq, commit: commit, node: n})
	s.cache = nil
	s.cacheOrder = nil
}

func (s *Store) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return s.dag.Head()
}

// viewAt resolves the node visible for each document at cutoff: the last put or tombstone,
// in sequence order, whose commit is an ancestor of the cutoff.
func (s *Store) viewAt(cutoff codec.Hash) *view {
	ancestryVersion := s.dag.AncestryVersion()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := viewKey{cutoff: cutoff, seq: s.seq, ancestryVersion: ancestryVersion}
	if v, ok := s.cache[key]; ok {
		return v
	}
	ancestors := s.dag.AncestorSet(cutoff)
	v := &view{visible: make(map[codec.UUID]*node)}
	for docID, dl := range s.docs {
		var last *docEvent
		for i := range dl.events {
			if _, ok := ancestors[dl.events[i].commit]; ok {
				last = &dl.events[i]
			}
		}
		if last == nil || last.node == nil {
			continue
		}
		v.visible[docID] = last.node
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

// LiveCount returns the number of vectors visible at atCommit (nil = head).
func (s *Store) LiveCount(atCommit *codec.Hash) (int, error) {
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return 0, err
	}
	return len(s.viewAt(cutoff).visible), nil
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
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	v := s.viewAt(cutoff)
	if len(v.visible) == 0 {
		return nil, nil
	}
	if s.exactThreshold >= 0 && len(v.visible) <= s.exactThreshold {
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
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	return s.exact(queryVector, k, s.viewAt(cutoff)), nil
}

func (s *Store) exact(q []float32, k int, v *view) []index.RankedResult {
	out := make([]index.RankedResult, 0, len(v.visible))
	for docID, n := range v.visible {
		out = append(out, index.RankedResult{DocID: docID, Score: Score(s.metric, q, n.vec)})
	}
	index.SortRanked(out)
	if len(out) > k {
		out = out[:k]
	}
	return out
}

func (s *Store) approximate(q []float32, k int, v *view) []index.RankedResult {
	s.mu.Lock()
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
	accept := func(n *node) bool { return v.visible[n.doc] == n }
	hits := g.search(q, k, s.efS, accept)
	s.mu.Unlock()
	out := make([]index.RankedResult, 0, len(hits))
	for _, h := range hits {
		n := s.nodes[h.id]
		out = append(out, index.RankedResult{DocID: n.doc, Score: float32(h.score)})
	}
	index.SortRanked(out)
	return out
}
