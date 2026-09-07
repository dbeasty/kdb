package sql

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fusion"
)

// Search in SQL - kdb-spec-layer16 §9.1 and §9.3.
//
// The executor never talks to an index store directly; it asks an IndexProvider, which the
// runtime wires to its index catalog. A nil provider means "no indexes": exact/range lookups
// fall back to a full scan and the search functions are planning errors.

// IndexProvider is what the executor consults for index-backed access paths. Every method
// reports ok=false when no suitable index exists for the field or name; the executor then
// falls back (lookups) or fails the query (search functions).
type IndexProvider interface {
	// FullTextSearch returns ranked hits for a FULLTEXT index named by index name or by its
	// first field. depth 0 means every hit.
	FullTextSearch(ctx QueryContext, indexOrField, query string, depth int) (hits []index.RankedResult, ok bool, err error)
	// VectorSearch returns the depth nearest neighbours (depth 0 = every document) from the
	// VECTOR index on field.
	VectorSearch(ctx QueryContext, field string, vector []float32, depth int) (hits []index.RankedResult, ok bool, err error)
	// ExactLookup returns the ids whose indexed value equals value (HASH or BTREE index).
	ExactLookup(ctx QueryContext, field string, value Cell) (ids []codec.UUID, ok bool, err error)
	// RangeLookup returns the ids whose indexed value lies in [low, high] with the given
	// strictness (BTREE index). A nil low or high is unbounded on that side.
	RangeLookup(ctx QueryContext, field string, low, high Cell, lowInclusive, highInclusive bool) (ids []codec.UUID, ok bool, err error)
}

// searchHits is one resolved search expression: the ranked hit list and a score lookup.
type searchHits struct {
	label  string
	ranked []index.RankedResult
	byID   map[codec.UUID]float32
}

func newSearchHits(label string, ranked []index.RankedResult) *searchHits {
	h := &searchHits{label: label, ranked: ranked, byID: make(map[codec.UUID]float32, len(ranked))}
	for _, r := range ranked {
		if _, dup := h.byID[r.DocID]; !dup {
			h.byID[r.DocID] = r.Score
		}
	}
	return h
}

// searchKey canonicalises a search expression so the same MATCH in WHERE, projection and
// ORDER BY resolves once. Parameters are keyed by index, literals by value.
func searchKey(e Expr) string {
	var b strings.Builder
	writeSearchKey(&b, e)
	return b.String()
}

func writeSearchKey(b *strings.Builder, e Expr) {
	switch ex := e.(type) {
	case ExprMatch:
		b.WriteString("match(")
		b.WriteString(ex.IndexOrField)
		b.WriteByte(',')
		writeSearchKey(b, ex.Query)
		b.WriteByte(')')
	case ExprSimilarity:
		b.WriteString("similarity(")
		b.WriteString(ex.Field)
		b.WriteByte(',')
		writeSearchKey(b, ex.Vector)
		b.WriteByte(')')
	case ExprFuse:
		b.WriteString("fuse[")
		b.WriteString(ex.Mode)
		b.WriteString("](")
		for i, arm := range ex.Arms {
			if i > 0 {
				b.WriteByte(',')
			}
			writeSearchKey(b, arm)
		}
		b.WriteByte(')')
	case ExprParameter:
		fmt.Fprintf(b, "?%d", ex.Index)
	case ExprLiteral:
		fmt.Fprintf(b, "%s", cellKey(ex.Cell))
	default:
		b.WriteString("?")
	}
}

func isSearchExpr(e Expr) bool {
	switch e.(type) {
	case ExprMatch, ExprSimilarity, ExprFuse:
		return true
	}
	return false
}

// collectSearchExprs gathers every search expression in a SELECT, in first-seen order and
// deduplicated by key.
func collectSearchExprs(q SelectQuery) []Expr {
	seen := map[string]bool{}
	var out []Expr
	var walk func(e Expr)
	walk = func(e Expr) {
		if e == nil {
			return
		}
		if isSearchExpr(e) {
			k := searchKey(e)
			if !seen[k] {
				seen[k] = true
				out = append(out, e)
			}
			if f, ok := e.(ExprFuse); ok {
				for _, arm := range f.Arms {
					walk(arm)
				}
			}
			return
		}
		forEachChild(e, walk)
	}
	for _, p := range q.Projections {
		if pe, ok := p.(ProjExpression); ok {
			walk(pe.Expr)
		}
	}
	walk(q.Where)
	for _, g := range q.GroupBy {
		walk(g)
	}
	for _, o := range q.OrderBy {
		walk(o.Expr)
	}
	return out
}

// forEachChild visits the direct sub-expressions of e.
func forEachChild(e Expr, visit func(Expr)) {
	switch ex := e.(type) {
	case ExprBinary:
		visit(ex.Left)
		visit(ex.Right)
	case ExprUnary:
		visit(ex.Expr)
	case ExprFunctionCall:
		for _, a := range ex.Args {
			visit(a)
		}
	case ExprIn:
		visit(ex.Expr)
		for _, v := range ex.Values {
			visit(v)
		}
	case ExprBetween:
		visit(ex.Expr)
		visit(ex.Low)
		visit(ex.High)
	case ExprMatch:
		visit(ex.Query)
	case ExprSimilarity:
		visit(ex.Vector)
	case ExprFuse:
		for _, a := range ex.Arms {
			visit(a)
		}
	}
}

// searchDepth is the §9.1 candidate depth: when ORDER BY leads with a score expression and the
// query has a LIMIT, depth = min(1000, max(50, 4·(n+m))); otherwise 0 (every hit).
func searchDepth(q SelectQuery, env *evalEnv) int {
	if q.Limit == nil || len(q.OrderBy) == 0 {
		return 0
	}
	if !isSearchExpr(env.resolveAlias(q.OrderBy[0].Expr)) {
		return 0
	}
	d := 4 * (*q.Limit + q.Offset)
	if d < 50 {
		d = 50
	}
	if d > 1000 {
		d = 1000
	}
	return d
}

// resolveSearches runs every search expression of the query through the provider and stores
// the hits on the environment. Missing indexes are planning errors per §9.1.
func (e *Executor) resolveSearches(q SelectQuery, env *evalEnv, ctx QueryContext) error {
	exprs := collectSearchExprs(q)
	if len(exprs) == 0 {
		return nil
	}
	env.scores = map[string]*searchHits{}
	depth := searchDepth(q, env)
	// Arms first so FUSE can reuse them; collectSearchExprs lists a FUSE before its arms, so
	// resolve in two passes.
	for _, ex := range exprs {
		if _, isFuse := ex.(ExprFuse); !isFuse {
			if err := e.resolveOneSearch(ex, env, ctx, depth); err != nil {
				return err
			}
		}
	}
	for _, ex := range exprs {
		if _, isFuse := ex.(ExprFuse); isFuse {
			if err := e.resolveOneSearch(ex, env, ctx, depth); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Executor) resolveOneSearch(ex Expr, env *evalEnv, ctx QueryContext, depth int) error {
	key := searchKey(ex)
	if _, done := env.scores[key]; done {
		return nil
	}
	switch s := ex.(type) {
	case ExprMatch:
		if e.IndexProvider == nil {
			return NewPlanningError("no FULLTEXT index for "+s.IndexOrField, "")
		}
		qc, ok := env.cell(s.Query, newEvalDoc(emptyDoc)).(CellString)
		if !ok {
			return NewPlanningError("MATCH query must be a string", "")
		}
		hits, ok, err := e.IndexProvider.FullTextSearch(ctx, s.IndexOrField, qc.Value, depth)
		if err != nil {
			return err
		}
		if !ok {
			return NewPlanningError("no FULLTEXT index for "+s.IndexOrField, "")
		}
		env.scores[key] = newSearchHits("fulltext("+s.IndexOrField+")", hits)
	case ExprSimilarity:
		if e.IndexProvider == nil {
			return NewPlanningError("no VECTOR index for "+s.Field, "")
		}
		vec, err := DecodeVector(env.cell(s.Vector, newEvalDoc(emptyDoc)))
		if err != nil {
			return err
		}
		hits, ok, err := e.IndexProvider.VectorSearch(ctx, s.Field, vec, depth)
		if err != nil {
			return err
		}
		if !ok {
			return NewPlanningError("no VECTOR index for "+s.Field, "")
		}
		env.scores[key] = newSearchHits("vector("+s.Field+")", hits)
	case ExprFuse:
		arms := make([]fusion.Arm, 0, len(s.Arms))
		labels := make([]string, 0, len(s.Arms))
		for _, arm := range s.Arms {
			if err := e.resolveOneSearch(arm, env, ctx, depth); err != nil {
				return err
			}
			h := env.scores[searchKey(arm)]
			arms = append(arms, fusion.Arm{Results: h.ranked, Weight: 1, Depth: depth})
			labels = append(labels, h.label)
		}
		mode := fusion.ModeRRF
		if s.Mode == "weighted" {
			mode = fusion.ModeWeightedSum
		}
		fused := fusion.Fuse(arms, mode, 0)
		env.scores[key] = newSearchHits("fuse:"+s.Mode+"("+strings.Join(labels, ",")+")", fused)
	}
	return nil
}
