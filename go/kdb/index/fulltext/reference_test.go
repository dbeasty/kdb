package fulltext

// The pre-bounding full-text store, kept (renamed) as the reference the bounded one is
// property-tested against.

import (
	"fmt"
	"math"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/analyzer"
	"github.com/limidus/kdb/go/kdb/json"
)

// refViewCacheEntries caps the memoized visibility views (head plus a few historical reads).
const refViewCacheEntries = 8

type refViewKey struct {
	cutoff          codec.Hash
	seq             int64
	ancestryVersion uint64
}

// refDocEvent is a put (ver != nil) or a tombstone (ver == nil) at a commit.
type refDocEvent struct {
	seq    int64
	commit codec.Hash
	ver    *version
}

type refDocLog struct {
	events []refDocEvent
}

// refView is what is visible at one cutoff: the version of each document, N, and per-field
// total lengths.
type refView struct {
	visible    map[codec.UUID]*version
	n          int
	fieldTotal []int
}

// refStore is the full-text index for one descriptor.
type refStore struct {
	desc    index.Descriptor
	dag     *dag.InMemoryCommitDag
	fields  []string
	weights []float64

	mu         sync.Mutex
	seq        int64
	docs       map[codec.UUID]*refDocLog
	postings   map[string][]*posting
	cache      map[refViewKey]*refView
	cacheOrder []refViewKey
}

// newRefStore builds a store over descriptor's fields (Descriptor.Fields, or FieldName)
// with weights from Options["weights"].
func newRefStore(descriptor index.Descriptor, d *dag.InMemoryCommitDag, opts Options) (*refStore, error) {
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
	s := &refStore{desc: descriptor, dag: d, fields: fields, weights: weights}
	s.resetLocked()
	return s, nil
}

func (s *refStore) resetLocked() {
	s.seq = 0
	s.docs = make(map[codec.UUID]*refDocLog)
	s.postings = make(map[string][]*posting)
	s.cache = nil
	s.cacheOrder = nil
}

func (s *refStore) Descriptor() index.Descriptor { return s.desc }

// Fields returns the indexed paths in descriptor order.
func (s *refStore) Fields() []string { return append([]string(nil), s.fields...) }

// Weights returns the per-field weights in descriptor order.
func (s *refStore) Weights() []float64 { return append([]float64(nil), s.weights...) }

// Put indexes an entry whose Key is the document's JSON text as a StringKey (that is what
// the commit path's hints carry for a full-text index).
func (s *refStore) Put(entry index.Entry) error {
	sk, ok := entry.Key.(index.StringKey)
	if !ok {
		return fmt.Errorf("fulltext index: Put expects a StringKey holding the document JSON, got %T", entry.Key)
	}
	return s.PutDocument(entry.DocID, entry.CommitHash, sk.Value)
}

func (s *refStore) Delete(docID codec.UUID, atCommit codec.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendEventLocked(docID, atCommit, nil)
	return nil
}

func (s *refStore) BulkLoad(entries []index.Entry) error {
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

func (s *refStore) Rebuild(entries []index.Entry) error { return s.BulkLoad(entries) }

func (s *refStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
	return nil
}

func (s *refStore) IsValid(atCommit codec.Hash) (bool, error) { return s.dag.HasCommit(atCommit), nil }

func (s *refStore) Lookup(key index.Key, atCommit *codec.Hash) ([]codec.UUID, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeHash, index.IndexTypeFullText)
}

func (s *refStore) Range(from, to index.Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeBTree, index.IndexTypeFullText)
}

func (s *refStore) NearestNeighbours(queryVector []float32, k int, atCommit *codec.Hash) ([]index.RankedResult, error) {
	return nil, index.NewTypeMismatchError("index type mismatch", s.desc.FieldName, index.IndexTypeVector, index.IndexTypeFullText)
}

// PrepareDocument analyzes the document's indexed fields without touching the index.
func (s *refStore) PrepareDocument(docID codec.UUID, jsonText string) (index.PreparedPut, error) {
	root, err := index.ParseDocument(jsonText)
	if err != nil {
		return nil, err
	}
	return &refPreparedPut{store: s, docID: docID, jsonText: jsonText, ver: s.analyze(root)}, nil
}

// PutDocument analyzes and indexes a JSON document at commitHash.
func (s *refStore) PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error {
	p, err := s.PrepareDocument(docID, jsonText)
	if err != nil {
		return err
	}
	_, err = p.Apply(commitHash)
	return err
}

type refPreparedPut struct {
	store    *refStore
	docID    codec.UUID
	jsonText string
	ver      *version
}

func (p *refPreparedPut) Apply(commitHash codec.Hash) (index.Hint, error) {
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
func (s *refStore) analyze(root json.Value) *version {
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

func (s *refStore) appendEventLocked(docID codec.UUID, commit codec.Hash, ver *version) {
	s.seq++
	if ver != nil {
		ver.seq = s.seq
	}
	dl := s.docs[docID]
	if dl == nil {
		dl = &refDocLog{}
		s.docs[docID] = dl
	}
	dl.events = append(dl.events, refDocEvent{seq: s.seq, commit: commit, ver: ver})
	if ver != nil {
		for f := range ver.fields {
			for term, info := range ver.fields[f].terms {
				s.postings[term] = append(s.postings[term], &posting{doc: docID, ver: ver, field: f, info: info})
			}
		}
	}
	s.cache = nil
	s.cacheOrder = nil
}

func (s *refStore) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return s.dag.Head()
}

// viewAt resolves which version of each document is visible at cutoff: the last put or
// tombstone, in sequence order, whose commit is an ancestor of the cutoff.
func (s *refStore) viewAt(cutoff codec.Hash) *refView {
	ancestryVersion := s.dag.AncestryVersion()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := refViewKey{cutoff: cutoff, seq: s.seq, ancestryVersion: ancestryVersion}
	if v, ok := s.cache[key]; ok {
		return v
	}
	ancestors := s.dag.AncestorSet(cutoff)
	v := &refView{visible: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
	for docID, dl := range s.docs {
		var last *refDocEvent
		for i := range dl.events {
			if _, ok := ancestors[dl.events[i].commit]; ok {
				last = &dl.events[i]
			}
		}
		if last == nil || last.ver == nil {
			continue
		}
		v.visible[docID] = last.ver
		if last.ver.total == 0 {
			continue
		}
		v.n++
		for f := range last.ver.fields {
			v.fieldTotal[f] += last.ver.fields[f].length
		}
	}
	if s.cache == nil {
		s.cache = make(map[refViewKey]*refView, refViewCacheEntries)
	}
	s.cache[key] = v
	s.cacheOrder = append(s.cacheOrder, key)
	if len(s.cacheOrder) > refViewCacheEntries {
		delete(s.cache, s.cacheOrder[0])
		s.cacheOrder = s.cacheOrder[1:]
	}
	return v
}

// Stats returns N and per-field avglen at atCommit (nil = head).
func (s *refStore) Stats(atCommit *codec.Hash) (Stats, error) {
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return Stats{}, err
	}
	v := s.viewAt(cutoff)
	st := Stats{N: v.n, AvgLen: make(map[string]float64, len(s.fields))}
	for f, path := range s.fields {
		st.AvgLen[path] = refAvgLen(v, f)
	}
	return st, nil
}

// SnapshotStats publishes head statistics into the persisted manifest.
func (s *refStore) SnapshotStats() map[string]float64 {
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

// refAvgLen is the field's average length over the N indexed documents; a document that
// contributes no tokens to the field counts as length 0.
func refAvgLen(v *refView, field int) float64 {
	if v.n == 0 {
		return 0
	}
	return float64(v.fieldTotal[field]) / float64(v.n)
}

// Search ranks the documents visible at atCommit (nil = head) that contain at least one query
// term and every quoted phrase, by BM25F-lite score descending then document id ascending.
// limit <= 0 returns every hit.
func (s *refStore) Search(query string, atCommit *codec.Hash, limit int) ([]index.RankedResult, error) {
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return nil, err
	}
	q := ParseQuery(query)
	if len(q.Terms) == 0 {
		return nil, nil
	}
	v := s.viewAt(cutoff)
	if v.n == 0 {
		return nil, nil
	}

	s.mu.Lock()
	scores := make(map[codec.UUID]float64)
	var order []codec.UUID
	for _, term := range q.Terms {
		var live []*posting
		docsWithTerm := make(map[codec.UUID]struct{})
		for _, p := range s.postings[term] {
			if v.visible[p.doc] != p.ver {
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
			norm := tf * (K1 + 1) / (tf + K1*(1-B+B*length/refAvgLen(v, p.field)))
			if _, seen := scores[p.doc]; !seen {
				order = append(order, p.doc)
			}
			scores[p.doc] += s.weights[p.field] * idf * norm
		}
	}
	s.mu.Unlock()

	out := make([]index.RankedResult, 0, len(order))
	for _, docID := range order {
		if !matchesPhrases(v.visible[docID], q.Phrases) {
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
