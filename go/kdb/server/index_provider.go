package server

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fusion"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// RegistryIndexProvider bridges an index.Registry to the three consumers that need it
// (kdb-spec-layer16 Components 66 and 68): sql.IndexProvider for index-backed access paths,
// sql.IndexDDLExecutor for CREATE/DROP INDEX, and SearchProvider for SEARCH frames.
//
// It is deliberately the only place that knows about all three interfaces at once. The index
// package stays free of any server or sql import, and the sql package never learns what a
// registry is.
//
// Every method is safe for concurrent use: the registry is, and this type holds no mutable
// state of its own beyond the pointers set at construction.
type RegistryIndexProvider struct {
	registry *index.Registry
	runtime  *KdbServerRuntime
	// dir is where the catalog and snapshots are persisted; "" for a memory-backed runtime.
	dir string
	// opts are the store options this registry was opened with, kept so a store added by
	// CREATE INDEX is built the same way as one loaded from disk.
	opts stores.Options
	// flushed counts index-touching commits since the last save. Only ever touched from the
	// commit path, which the write gate already serialises.
	flushed int
}

// NewRegistryIndexProvider wires reg to rt. rt supplies the head commit, document bodies for
// IncludeJSON, and the expiry predicate; it may be nil in tests that only exercise lookups.
func NewRegistryIndexProvider(reg *index.Registry, rt *KdbServerRuntime) *RegistryIndexProvider {
	return &RegistryIndexProvider{registry: reg, runtime: rt}
}

// Registry exposes the wrapped registry so the commit path can prepare and apply hints.
func (p *RegistryIndexProvider) Registry() *index.Registry { return p.registry }

var (
	_ sql.IndexProvider    = (*RegistryIndexProvider)(nil)
	_ sql.IndexDDLExecutor = (*RegistryIndexProvider)(nil)
	_ SearchProvider       = (*RegistryIndexProvider)(nil)
)

// ---------------------------------------------------------------------------
// sql.IndexProvider
// ---------------------------------------------------------------------------

// FullTextSearch resolves a FULLTEXT index by name or first field and returns its ranked hits.
func (p *RegistryIndexProvider) FullTextSearch(ctx sql.QueryContext, indexOrField, query string, depth int) ([]index.RankedResult, bool, error) {
	store, ok := p.registry.Resolve(indexOrField, index.IndexTypeFullText)
	if !ok {
		return nil, false, nil
	}
	hits, err := store.Search(query, ctx.AtCommit, depth)
	if err != nil {
		return nil, true, err
	}
	return hits, true, nil
}

// VectorSearch resolves a VECTOR index by name or field and returns its nearest neighbours.
func (p *RegistryIndexProvider) VectorSearch(ctx sql.QueryContext, field string, vector []float32, depth int) ([]index.RankedResult, bool, error) {
	store, ok := p.registry.Resolve(field, index.IndexTypeVector)
	if !ok {
		return nil, false, nil
	}
	hits, err := store.NearestNeighbours(vector, depth, ctx.AtCommit)
	if err != nil {
		return nil, true, err
	}
	return hits, true, nil
}

// ExactLookup answers an equality conjunct from a HASH or BTREE index. A cell that cannot be
// expressed as an index key (JSON, or a type the index was not built for) reports ok=false so
// the executor falls back to a full scan rather than returning a wrong answer.
func (p *RegistryIndexProvider) ExactLookup(ctx sql.QueryContext, field string, value sql.Cell) ([]codec.UUID, bool, error) {
	store, ok := p.keyedStore(field)
	if !ok {
		return nil, false, nil
	}
	key, ok := indexKeyForCell(value)
	if !ok {
		return nil, false, nil
	}
	ids, err := store.Lookup(key, ctx.AtCommit)
	if err != nil {
		return nil, true, err
	}
	return ids, true, nil
}

// RangeLookup answers an ordering or BETWEEN conjunct from a BTREE index. Stores scan the
// closed interval, so an exclusive bound is served as inclusive here and the executor's
// residual predicate removes the endpoint - never the other way round, which would drop rows.
func (p *RegistryIndexProvider) RangeLookup(ctx sql.QueryContext, field string, low, high sql.Cell, lowInclusive, highInclusive bool) ([]codec.UUID, bool, error) {
	store, ok := p.registry.ByField(field, index.IndexTypeBTree)
	if !ok {
		return nil, false, nil
	}
	from, ok := boundKey(low)
	if !ok {
		return nil, false, nil
	}
	to, ok := boundKey(high)
	if !ok {
		return nil, false, nil
	}
	ids, err := store.Range(from, to, ctx.AtCommit, 0, true)
	if err != nil {
		return nil, true, err
	}
	return ids, true, nil
}

// keyedStore prefers a HASH index over a BTREE one for equality, matching the Kotlin reader.
func (p *RegistryIndexProvider) keyedStore(field string) (index.Store, bool) {
	if store, ok := p.registry.ByField(field, index.IndexTypeHash); ok {
		return store, true
	}
	return p.registry.ByField(field, index.IndexTypeBTree)
}

// boundKey maps an open bound (nil or CellNull) to a nil key, which the stores read as
// unbounded on that side.
func boundKey(c sql.Cell) (index.Key, bool) {
	if c == nil {
		return nil, true
	}
	if _, isNull := c.(sql.CellNull); isNull {
		return nil, true
	}
	return indexKeyForCell(c)
}

// indexKeyForCell converts a SQL cell to the index key of the same value. JSON cells have no
// key form; reporting false makes the caller fall back to a scan.
func indexKeyForCell(c sql.Cell) (index.Key, bool) {
	switch v := c.(type) {
	case sql.CellString:
		return index.StringKey{Value: v.Value}, true
	case sql.CellLong:
		return index.Int64Key{Value: v.Value}, true
	case sql.CellDouble:
		return index.Float64Key{Value: v.Value}, true
	case sql.CellBool:
		return index.BoolKey{Value: v.Value}, true
	case sql.CellNull:
		return index.NullKey{}, true
	default:
		return nil, false
	}
}

// ---------------------------------------------------------------------------
// sql.IndexDDLExecutor
// ---------------------------------------------------------------------------

// CreateIndex registers a new index and rebuilds it from the current head, so the index is
// queryable the moment the statement returns rather than only covering later writes.
func (p *RegistryIndexProvider) CreateIndex(stmt sql.StmtCreateIndex, ctx sql.QueryContext) error {
	desc, err := descriptorFor(stmt, ctx.NamespaceID)
	if err != nil {
		return err
	}
	if _, err := p.registry.Add(desc); err != nil {
		return err
	}
	if p.runtime == nil {
		return nil
	}
	if err := p.runtime.rebuildIndex(desc.IndexID); err != nil {
		// A rebuild that fails leaves a registered but empty index, which would answer
		// queries with silently wrong results - the one outcome this layer exists to remove.
		p.registry.Remove(desc.IndexName())
		return err
	}
	return p.runtime.saveIndexCatalog()
}

// DropIndex removes an index by name.
func (p *RegistryIndexProvider) DropIndex(stmt sql.StmtDropIndex, ctx sql.QueryContext) error {
	if _, ok := p.registry.Remove(stmt.Name); !ok {
		return fmt.Errorf("index not found: %s", stmt.Name)
	}
	if p.runtime == nil {
		return nil
	}
	return p.runtime.saveIndexCatalog()
}

// descriptorFor builds the index descriptor a CREATE INDEX statement asks for, validating the
// options the store constructors require (spec §9.2).
func descriptorFor(stmt sql.StmtCreateIndex, namespaceID string) (index.Descriptor, error) {
	if len(stmt.Fields) == 0 {
		return index.Descriptor{}, fmt.Errorf("index %s: no fields", stmt.Name)
	}
	typ := index.IndexTypeHash
	if stmt.Using != "" {
		parsed, err := index.ParseIndexType(stmt.Using)
		if err != nil {
			return index.Descriptor{}, err
		}
		typ = parsed
	}
	paths := make([]string, 0, len(stmt.Fields))
	for _, f := range stmt.Fields {
		paths = append(paths, f.Path)
	}
	if typ != index.IndexTypeFullText && len(paths) > 1 {
		return index.Descriptor{}, fmt.Errorf("index %s: only FULLTEXT indexes may cover several fields", stmt.Name)
	}
	opts := map[string]string{index.OptionIndexName: stmt.Name}
	for k, v := range stmt.With {
		opts[strings.ToLower(k)] = v
	}
	if typ == index.IndexTypeFullText {
		if w := weightsOption(stmt.Fields); w != "" {
			opts[index.OptionWeights] = w
		}
	}
	if typ == index.IndexTypeVector {
		dims, ok := opts[index.OptionDimensions]
		if !ok {
			return index.Descriptor{}, fmt.Errorf("index %s: VECTOR requires WITH (dimensions = n)", stmt.Name)
		}
		if n, err := strconv.Atoi(dims); err != nil || n <= 0 {
			return index.Descriptor{}, fmt.Errorf("index %s: dimensions must be a positive integer", stmt.Name)
		}
	}
	return index.Descriptor{
		IndexID:     index.DerivedIndexID(namespaceID, stmt.Name, typ),
		NamespaceID: namespaceID,
		FieldName:   paths[0],
		Fields:      paths,
		Type:        typ,
		Unique:      stmt.Unique,
		Options:     opts,
	}, nil
}

// weightsOption renders per-field weights as "title=3,description=1", omitted entirely when
// every field is the default weight.
func weightsOption(fields []sql.IndexField) string {
	var parts []string
	nonDefault := false
	for _, f := range fields {
		w := f.Weight
		if w == 0 {
			w = 1
		}
		if w != 1 {
			nonDefault = true
		}
		parts = append(parts, fmt.Sprintf("%s=%s", f.Path, strconv.FormatFloat(w, 'g', -1, 64)))
	}
	if !nonDefault {
		return ""
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------
// SearchProvider
// ---------------------------------------------------------------------------

// Search answers a SEARCH frame: run each requested arm, fuse them when both are present, and
// fill in document bodies when asked. Expired documents are dropped at head, since hits come
// from index state rather than through the runtime's expiry-hiding storage adapter.
func (p *RegistryIndexProvider) Search(ctx context.Context, req wire.SearchMessage) ([]SearchHit, codec.Hash, error) {
	var at *codec.Hash
	resolved, err := p.runtime.Runtime.DAG.Head()
	if err != nil {
		return nil, codec.Hash{}, err
	}
	if req.AtCommitHex != "" {
		h, err := codec.HashFromHex(req.AtCommitHex)
		if err != nil {
			return nil, codec.Hash{}, err
		}
		at = &h
		resolved = h
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultSearchLimit
	}

	var arms []fusion.Arm
	if a := req.Text; a != nil {
		store, ok := p.registry.Resolve(a.Index, index.IndexTypeFullText)
		if !ok {
			return nil, codec.Hash{}, &UnsupportedError{Reason: "no FULLTEXT index for " + a.Index}
		}
		hits, err := store.Search(a.Query, at, armDepth(a.Depth, limit))
		if err != nil {
			return nil, codec.Hash{}, err
		}
		arms = append(arms, armFor(hits, a.Weight, a.Depth, a.MinScore))
	}
	if a := req.Vector; a != nil {
		store, ok := p.registry.Resolve(a.Index, index.IndexTypeVector)
		if !ok {
			return nil, codec.Hash{}, &UnsupportedError{Reason: "no VECTOR index for " + a.Index}
		}
		vec := make([]float32, len(a.Vector))
		for i, v := range a.Vector {
			vec[i] = float32(v)
		}
		hits, err := store.NearestNeighbours(vec, armDepth(a.Depth, limit), at)
		if err != nil {
			return nil, codec.Hash{}, err
		}
		arms = append(arms, armFor(hits, a.Weight, a.Depth, a.MinScore))
	}
	if len(arms) == 0 {
		return nil, codec.Hash{}, fmt.Errorf("search requires at least one arm")
	}

	var ranked []index.RankedResult
	if len(arms) == 1 {
		ranked = prepareSingleArm(arms[0], limit)
	} else {
		mode := fusion.ModeRRF
		if strings.EqualFold(req.Fusion, "weighted") {
			mode = fusion.ModeWeightedSum
		}
		ranked = fusion.Fuse(arms, mode, limit)
	}

	atHead := req.AtCommitHex == ""
	hits := make([]SearchHit, 0, len(ranked))
	for _, r := range ranked {
		hit := SearchHit{DocID: r.DocID, Score: shortestFloat64(r.Score)}
		if req.IncludeJSON || atHead {
			body, _, found, err := p.runtime.getDocumentAt(req.Namespace, r.DocID, resolved)
			if err != nil {
				return nil, codec.Hash{}, err
			}
			// A hit whose document is gone or expired is dropped rather than returned with an
			// empty body: index state can outlive the document it points at.
			if !found {
				continue
			}
			if atHead && p.runtime.isExpiredAtHead(body) {
				continue
			}
			if req.IncludeJSON {
				hit.JSON = body
			}
		}
		hits = append(hits, hit)
	}
	return hits, resolved, nil
}

// defaultSearchLimit bounds a SEARCH that named no limit.
const defaultSearchLimit = 50

// armDepth is the per-arm candidate depth: the request's own depth when set, else the spec's
// §9.1 rule scaled off the result limit.
func armDepth(depth, limit int) int {
	if depth > 0 {
		return depth
	}
	d := 4 * limit
	if d < 50 {
		d = 50
	}
	if d > 1000 {
		d = 1000
	}
	return d
}

func armFor(hits []index.RankedResult, weight *float64, depth int, minScore *float64) fusion.Arm {
	arm := fusion.Arm{Results: hits, Depth: depth}
	if weight != nil {
		arm.Weight = *weight
	}
	if minScore != nil {
		arm.MinScore = float32(*minScore)
		arm.HasMinScore = true
	}
	return arm
}

// prepareSingleArm applies the arm's own floor and depth without fusing, so a one-armed search
// returns that arm's native scores rather than reciprocal ranks.
func prepareSingleArm(arm fusion.Arm, limit int) []index.RankedResult {
	out := arm.Results
	if arm.HasMinScore {
		kept := make([]index.RankedResult, 0, len(out))
		for _, r := range out {
			if r.Score >= arm.MinScore {
				kept = append(kept, r)
			}
		}
		out = kept
	}
	if arm.Depth > 0 && len(out) > arm.Depth {
		out = out[:arm.Depth]
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].DocID.String() < out[j].DocID.String()
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// shortestFloat64 widens a float32 score through its shortest round-trip decimal, so 0.9f
// becomes 0.9 and not 0.8999999761581421 (spec §5) and Go and Kotlin print the same score.
func shortestFloat64(f float32) float64 {
	v, err := strconv.ParseFloat(strconv.FormatFloat(float64(f), 'g', -1, 32), 64)
	if err != nil {
		return float64(f)
	}
	return v
}
