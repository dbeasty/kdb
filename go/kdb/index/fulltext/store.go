// Package fulltext is the Layer 16 (§6) scored full-text index: a multi-field, weighted
// inverted index with positions, BM25F-lite scoring, OR semantics with quoted phrases, and
// commit-ancestry visibility so a Search atCommit sees exactly the documents that commit saw.
package fulltext

import (
	"fmt"
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

// Options tunes a Store. The spec fixes every scoring parameter, so it is currently empty
// and exists so the constructor's shape can grow without breaking callers.
type Options struct{}

// viewCacheEntries caps the memoized visibility views (head plus a few historical reads).
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
}

// docEvent is a put (ver != nil) or a tombstone (ver == nil) at a commit.
type docEvent struct {
	seq    int64
	commit codec.Hash
	ver    *version
}

type docLog struct {
	events []docEvent
}

// posting points from a term to one field of one document version.
type posting struct {
	doc   codec.UUID
	ver   *version
	field int
	info  *termInfo
}

// view is what is visible at one cutoff: the version of each document, N, and per-field
// total lengths.
type view struct {
	visible    map[codec.UUID]*version
	n          int
	fieldTotal []int
}

// Store is the full-text index for one descriptor.
type Store struct {
	desc    index.Descriptor
	dag     *dag.InMemoryCommitDag
	fields  []string
	weights []float64

	mu         sync.Mutex
	seq        int64
	docs       map[codec.UUID]*docLog
	postings   map[string][]*posting
	cache      map[viewKey]*view
	cacheOrder []viewKey
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
	s := &Store{desc: descriptor, dag: d, fields: fields, weights: weights}
	s.resetLocked()
	return s, nil
}

func (s *Store) resetLocked() {
	s.seq = 0
	s.docs = make(map[codec.UUID]*docLog)
	s.postings = make(map[string][]*posting)
	s.cache = nil
	s.cacheOrder = nil
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
	s.seq++
	if ver != nil {
		ver.seq = s.seq
	}
	dl := s.docs[docID]
	if dl == nil {
		dl = &docLog{}
		s.docs[docID] = dl
	}
	dl.events = append(dl.events, docEvent{seq: s.seq, commit: commit, ver: ver})
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

func (s *Store) cutoff(atCommit *codec.Hash) (codec.Hash, error) {
	if atCommit != nil {
		return *atCommit, nil
	}
	return s.dag.Head()
}

// viewAt resolves which version of each document is visible at cutoff: the last put or
// tombstone, in sequence order, whose commit is an ancestor of the cutoff.
func (s *Store) viewAt(cutoff codec.Hash) *view {
	ancestryVersion := s.dag.AncestryVersion()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := viewKey{cutoff: cutoff, seq: s.seq, ancestryVersion: ancestryVersion}
	if v, ok := s.cache[key]; ok {
		return v
	}
	ancestors := s.dag.AncestorSet(cutoff)
	v := &view{visible: make(map[codec.UUID]*version), fieldTotal: make([]int, len(s.fields))}
	for docID, dl := range s.docs {
		var last *docEvent
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

// Stats are the corpus statistics BM25 scoring reads at one cutoff.
type Stats struct {
	N      int
	AvgLen map[string]float64
}

// Stats returns N and per-field avglen at atCommit (nil = head).
func (s *Store) Stats(atCommit *codec.Hash) (Stats, error) {
	cutoff, err := s.cutoff(atCommit)
	if err != nil {
		return Stats{}, err
	}
	v := s.viewAt(cutoff)
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
			norm := tf * (K1 + 1) / (tf + K1*(1-B+B*length/avgLen(v, p.field)))
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
