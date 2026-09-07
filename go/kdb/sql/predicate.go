package sql

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Expression evaluation over documents - kdb-spec-layer16 §2 (column resolution, candidate
// lists, comparison rules) and §4 (predicate coverage).
//
// evalEnv is one query's evaluation context: schema, parameters, the FROM table (for alias
// stripping), projection aliases (ORDER BY score), a LIKE regex cache, and the search results
// MATCH / SIMILARITY / FUSE read their scores from. evalDoc wraps one document with a lazily
// parsed JSON tree so a row is parsed at most once however many column references touch it.

type evalEnv struct {
	schema  schema.KdbSchema
	params  []Parameter
	from    TableRef
	aliases map[string]Expr
	regexes map[string]*regexp.Regexp
	scores  map[string]*searchHits
}

func newEvalEnv(sch schema.KdbSchema, params []Parameter, from TableRef) *evalEnv {
	return &evalEnv{schema: sch, params: params, from: from}
}

type evalDoc struct {
	doc    document.Document
	root   kdbjson.Value
	parsed bool
}

func newEvalDoc(doc document.Document) *evalDoc { return &evalDoc{doc: doc} }

func (d *evalDoc) tree() kdbjson.Value {
	if !d.parsed {
		d.parsed = true
		v, err := kdbjson.ParseValue(d.doc.JSON)
		if err == nil {
			d.root = v
		}
	}
	return d.root
}

// Reserved column names resolving outside the document body.
const (
	colKdbID = "kdb_id"
	colDoc   = "_doc"
)

func isReservedColumn(name string) bool { return name == colKdbID || name == colDoc || name == "*" }

// stripTableAlias removes a leading "<table>." or "<alias>." from a column path (§2).
func stripTableAlias(name string, from TableRef) string {
	i := strings.IndexByte(name, '.')
	if i <= 0 {
		return name
	}
	head := name[:i]
	if (from.Name != "" && strings.EqualFold(head, from.Name)) || (from.Alias != "" && strings.EqualFold(head, from.Alias)) {
		return name[i+1:]
	}
	return name
}

// rootSegment is the first path segment of a column reference - the part Rule 1 checks against
// the declared schema.
func rootSegment(name string) string {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i]
	}
	return name
}

func (env *evalEnv) columnName(name string) string { return stripTableAlias(name, env.from) }

// columnValues returns the JSON candidates of a column reference: with expand, a terminal array
// contributes its elements (predicate semantics); without, it is one candidate (projection /
// ORDER BY semantics). Reserved names yield exactly one candidate.
func (env *evalEnv) columnValues(name string, d *evalDoc, expand bool) []kdbjson.Value {
	name = env.columnName(name)
	switch name {
	case colKdbID:
		return []kdbjson.Value{kdbjson.StringValue{V: d.doc.ID.String()}}
	case colDoc:
		if root := d.tree(); root != nil {
			return []kdbjson.Value{root}
		}
		return nil
	}
	root := d.tree()
	if root == nil {
		return nil
	}
	return kdbjson.CandidatesOf(root, kdbjson.SplitDotted(name), expand)
}

// candidates evaluates an operand to its candidate cells (§2): a column reference fans out over
// implicit array traversal, everything else is a single value.
func (env *evalEnv) candidates(expr Expr, d *evalDoc) []Cell {
	if col, ok := expr.(ExprColumnRef); ok {
		if env.columnName(col.Name) == colDoc {
			return []Cell{CellJSON{JSON: d.doc.JSON}}
		}
		vals := env.columnValues(col.Name, d, true)
		out := make([]Cell, 0, len(vals))
		for _, v := range vals {
			out = append(out, jsonValueToCell(v, nil))
		}
		return out
	}
	return []Cell{env.cell(expr, d)}
}

// cell evaluates an expression to one cell: the first candidate of a column reference (NULL when
// there is none), a literal, a parameter, a function result, or a search score.
func (env *evalEnv) cell(expr Expr, d *evalDoc) Cell {
	switch e := expr.(type) {
	case ExprLiteral:
		if e.Cell == nil {
			return CellNull{}
		}
		return e.Cell
	case ExprColumnRef:
		if env.columnName(e.Name) == colDoc {
			return CellJSON{JSON: d.doc.JSON}
		}
		vals := env.columnValues(e.Name, d, false)
		if len(vals) == 0 {
			return CellNull{}
		}
		return jsonValueToCell(vals[0], nil)
	case ExprParameter:
		return parameterToCell(env.params, e.Index)
	case ExprFunctionCall:
		return env.functionCell(e, d)
	case ExprMatch, ExprSimilarity, ExprFuse:
		return env.scoreCell(e, d)
	case ExprBinary, ExprUnary, ExprIn, ExprBetween:
		return CellBool{Value: env.predicate(expr, d)}
	default:
		return CellNull{}
	}
}

// predicate evaluates a boolean expression against one document (§4).
func (env *evalEnv) predicate(expr Expr, d *evalDoc) bool {
	switch e := expr.(type) {
	case ExprBinary:
		switch e.Op {
		case BinaryOpAnd:
			return env.predicate(e.Left, d) && env.predicate(e.Right, d)
		case BinaryOpOr:
			return env.predicate(e.Left, d) || env.predicate(e.Right, d)
		case BinaryOpLike, BinaryOpILike:
			pattern, ok := env.cell(e.Right, d).(CellString)
			if !ok {
				return false
			}
			re := env.likeRegexp(pattern.Value, e.Op == BinaryOpILike)
			for _, c := range env.candidates(e.Left, d) {
				if s, ok := c.(CellString); ok && re.MatchString(s.Value) {
					return true
				}
			}
			return false
		default:
			rights := env.candidates(e.Right, d)
			for _, l := range env.candidates(e.Left, d) {
				for _, r := range rights {
					if compareOp(e.Op, l, r) {
						return true
					}
				}
			}
			return false
		}
	case ExprUnary:
		switch e.Op {
		case UnaryOpNot:
			return !env.predicate(e.Expr, d)
		case UnaryOpIsNull:
			return isNullCell(env.cell(e.Expr, d))
		}
		return false
	case ExprIn:
		lefts := env.candidates(e.Expr, d)
		for _, v := range e.Values {
			for _, r := range env.candidates(v, d) {
				for _, l := range lefts {
					if compareOp(BinaryOpEQ, l, r) {
						return true
					}
				}
			}
		}
		return false
	case ExprBetween:
		low := env.cell(e.Low, d)
		high := env.cell(e.High, d)
		for _, l := range env.candidates(e.Expr, d) {
			if compareOp(BinaryOpGE, l, low) && compareOp(BinaryOpLE, l, high) {
				return true
			}
		}
		return false
	case ExprColumnRef:
		// A bare column predicate is true iff some candidate is boolean true.
		for _, v := range env.columnValues(e.Name, d, true) {
			if b, ok := v.(kdbjson.BoolValue); ok && b.V {
				return true
			}
		}
		return false
	case ExprParameter, ExprLiteral:
		return isTrueCell(env.cell(expr, d))
	case ExprFunctionCall:
		return isTrueCell(env.functionCell(e, d))
	case ExprMatch, ExprSimilarity, ExprFuse:
		hits := env.scores[searchKey(expr)]
		if hits == nil {
			return false
		}
		_, hit := hits.byID[d.doc.ID]
		return hit
	}
	return false
}

func isNullCell(c Cell) bool {
	if c == nil {
		return true
	}
	_, isNull := c.(CellNull)
	return isNull
}

func isTrueCell(c Cell) bool {
	switch v := c.(type) {
	case CellBool:
		return v.Value
	case CellLong:
		return v.Value != 0
	default:
		return false
	}
}

// likeRegexp compiles a SQL LIKE pattern: % matches any run, _ one character, everything else
// is literal (regex metacharacters escaped). Compiled patterns are cached per query.
func (env *evalEnv) likeRegexp(pattern string, caseInsensitive bool) *regexp.Regexp {
	key := pattern
	if caseInsensitive {
		key = "i:" + pattern
	} else {
		key = "c:" + pattern
	}
	if env.regexes == nil {
		env.regexes = map[string]*regexp.Regexp{}
	}
	if re, ok := env.regexes[key]; ok {
		return re
	}
	var sb strings.Builder
	if caseInsensitive {
		sb.WriteString("(?is)^")
	} else {
		sb.WriteString("(?s)^")
	}
	for _, r := range pattern {
		switch r {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	sb.WriteString("$")
	re, err := regexp.Compile(sb.String())
	if err != nil {
		// QuoteMeta output always compiles; fall back to a never-matching pattern regardless.
		re = regexp.MustCompile(`\A\z.`)
	}
	env.regexes[key] = re
	return re
}

// functionCell evaluates a scalar function in row context. Aggregate names evaluate over the
// single row (the planner keeps them out of WHERE).
func (env *evalEnv) functionCell(call ExprFunctionCall, d *evalDoc) Cell {
	name := strings.ToLower(call.Name)
	switch name {
	case "array_contains", "array_contains_any":
		if len(call.Args) < 2 {
			return CellBool{Value: false}
		}
		arr := env.arrayAt(call.Args[0], d)
		if arr == nil {
			return CellBool{Value: false}
		}
		wantAll := name == "array_contains"
		for _, arg := range call.Args[1:] {
			needle, err := cellToJSONValue(env.cell(arg, d))
			if err != nil {
				return CellBool{Value: false}
			}
			found := false
			for _, el := range arr.Elements {
				if kdbjson.DeepEqual(el, needle) {
					found = true
					break
				}
			}
			if found && !wantAll {
				return CellBool{Value: true}
			}
			if !found && wantAll {
				return CellBool{Value: false}
			}
		}
		return CellBool{Value: wantAll}
	case "array_length":
		if len(call.Args) != 1 {
			return CellNull{}
		}
		arr := env.arrayAt(call.Args[0], d)
		if arr == nil {
			return CellNull{}
		}
		return CellLong{Value: int64(len(arr.Elements))}
	case "count", "sum", "avg", "min", "max":
		return env.aggregate(call, []*evalDoc{d})
	default:
		return CellNull{}
	}
}

// arrayAt returns the array a column path (or JSON-valued expression) designates, or nil.
func (env *evalEnv) arrayAt(expr Expr, d *evalDoc) *kdbjson.ArrayValue {
	if col, ok := expr.(ExprColumnRef); ok {
		vals := env.columnValues(col.Name, d, false)
		if len(vals) == 0 {
			return nil
		}
		if arr, ok := vals[0].(kdbjson.ArrayValue); ok {
			return &arr
		}
		return nil
	}
	if js, ok := env.cell(expr, d).(CellJSON); ok {
		if v, err := kdbjson.ParseValue(js.JSON); err == nil {
			if arr, ok := v.(kdbjson.ArrayValue); ok {
				return &arr
			}
		}
	}
	return nil
}

// scoreCell is the projection value of a search expression: the hit's score, 0 for a MATCH
// non-hit, NULL for a document outside a SIMILARITY / FUSE candidate set.
func (env *evalEnv) scoreCell(expr Expr, d *evalDoc) Cell {
	hits := env.scores[searchKey(expr)]
	if hits == nil {
		return CellNull{}
	}
	if s, ok := hits.byID[d.doc.ID]; ok {
		return CellDouble{Value: float32ToDouble(s)}
	}
	if _, isMatch := expr.(ExprMatch); isMatch {
		return CellDouble{Value: 0}
	}
	return CellNull{}
}

// --- public wrappers (kept for callers outside the executor) -----------------------------

// EvalPredicate evaluates a WHERE expression against one document.
func EvalPredicate(expr Expr, doc document.Document, sch schema.KdbSchema, params []Parameter) bool {
	return newEvalEnv(sch, params, TableRef{}).predicate(expr, newEvalDoc(doc))
}

// EvalCell evaluates an expression to a cell value (first-candidate semantics for columns).
func EvalCell(expr Expr, doc document.Document, sch schema.KdbSchema, params []Parameter) Cell {
	return newEvalEnv(sch, params, TableRef{}).cell(expr, newEvalDoc(doc))
}

// CellForColumn reads a column (dotted path or reserved name) from document JSON.
func CellForColumn(name string, doc document.Document, sch schema.KdbSchema) Cell {
	return newEvalEnv(sch, nil, TableRef{}).cell(ExprColumnRef{Name: name}, newEvalDoc(doc))
}

// --- cells and comparison ----------------------------------------------------------------

func parameterToCell(params []Parameter, index int) Cell {
	if index < 0 || index >= len(params) {
		return CellNull{}
	}
	switch p := params[index].(type) {
	case ParamString:
		return CellString{Value: p.Value}
	case ParamInt:
		return CellLong{Value: p.Value}
	case ParamDouble:
		return CellDouble{Value: p.Value}
	case ParamBool:
		return CellBool{Value: p.Value}
	case ParamVector:
		return CellJSON{JSON: vectorJSON(p.Value)}
	case ParamNull:
		return CellNull{}
	default:
		return CellNull{}
	}
}

// float32ToDouble widens an index score to a double through its shortest 32-bit decimal form,
// so a score of 0.9f reaches the client as 0.9 rather than 0.8999999761581421. Kotlin's
// Float.toString().toDouble() produces the same value, keeping the trees in parity.
func float32ToDouble(f float32) float64 {
	v, err := strconv.ParseFloat(strconv.FormatFloat(float64(f), 'g', -1, 32), 64)
	if err != nil {
		return float64(f)
	}
	return v
}

func vectorJSON(v []float32) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}

// DecodeVector reads a vector operand (a [..] literal parsed to CellJSON, or a ParamVector) as
// []float32. Any other cell, or a non-numeric array, is an error.
func DecodeVector(c Cell) ([]float32, error) {
	js, ok := c.(CellJSON)
	if !ok {
		return nil, NewPlanningError("vector operand must be a [..] literal or a vector parameter", "")
	}
	v, err := kdbjson.ParseValue(js.JSON)
	if err != nil {
		return nil, NewPlanningError("vector operand is not valid JSON", "")
	}
	arr, ok := v.(kdbjson.ArrayValue)
	if !ok {
		return nil, NewPlanningError("vector operand must be a JSON array of numbers", "")
	}
	out := make([]float32, len(arr.Elements))
	for i, el := range arr.Elements {
		switch n := el.(type) {
		case kdbjson.IntValue:
			out[i] = float32(n.V)
		case kdbjson.NumberValue:
			out[i] = float32(n.V)
		default:
			return nil, NewPlanningError("vector operand must be a JSON array of numbers", "")
		}
	}
	return out, nil
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// cellTypeRank orders cell types for the total comparator: NULL < numeric (long, double, bool)
// < string < JSON.
func cellTypeRank(c Cell) int {
	switch c.(type) {
	case nil, CellNull:
		return 0
	case CellLong, CellDouble, CellBool:
		return 1
	case CellString:
		return 2
	case CellJSON:
		return 3
	default:
		return 4
	}
}

func numericValue(c Cell) (i int64, f float64, isInt bool) {
	switch v := c.(type) {
	case CellLong:
		return v.Value, float64(v.Value), true
	case CellDouble:
		return 0, v.Value, false
	case CellBool:
		n := boolToInt64(v.Value)
		return n, float64(n), true
	}
	return 0, 0, false
}

// compareSameKind compares two cells of the same type rank; it never panics.
func compareSameKind(left, right Cell) int {
	switch cellTypeRank(left) {
	case 1:
		li, lf, lInt := numericValue(left)
		ri, rf, rInt := numericValue(right)
		if lInt && rInt {
			switch {
			case li < ri:
				return -1
			case li > ri:
				return 1
			}
			return 0
		}
		switch {
		case lf < rf:
			return -1
		case lf > rf:
			return 1
		case math.IsNaN(lf) && !math.IsNaN(rf):
			return -1
		case !math.IsNaN(lf) && math.IsNaN(rf):
			return 1
		}
		return 0
	case 2:
		return strings.Compare(left.(CellString).Value, right.(CellString).Value)
	case 3:
		return strings.Compare(left.(CellJSON).JSON, right.(CellJSON).JSON)
	}
	return 0
}

// CompareCells is the total ordering used by ORDER BY and GROUP BY (§2): NULL sorts before
// every value (NULL vs NULL is 0), numbers compare numerically across long/double/bool, and
// values of different kinds order by kind (numeric < string < JSON) so mismatched types never
// panic and always order deterministically.
func CompareCells(left, right Cell) int {
	lr, rr := cellTypeRank(left), cellTypeRank(right)
	if lr != rr {
		if lr < rr {
			return -1
		}
		return 1
	}
	return compareSameKind(left, right)
}

// compareComparable is the predicate comparison (§2): NULL is never comparable, mismatched
// types are incomparable, otherwise the natural order.
func compareComparable(left, right Cell) (int, bool) {
	lr, rr := cellTypeRank(left), cellTypeRank(right)
	if lr == 0 || rr == 0 || lr != rr {
		return 0, false
	}
	return compareSameKind(left, right), true
}

// compareOp applies a comparison operator with the §2 rules: NULL → false; incomparable → `=`
// false, `<>` true, ordering false.
func compareOp(op BinaryOp, left, right Cell) bool {
	if isNullCell(left) || isNullCell(right) {
		return false
	}
	cmp, ok := compareComparable(left, right)
	if !ok {
		return op == BinaryOpNE
	}
	switch op {
	case BinaryOpEQ:
		return cmp == 0
	case BinaryOpNE:
		return cmp != 0
	case BinaryOpLT:
		return cmp < 0
	case BinaryOpLE:
		return cmp <= 0
	case BinaryOpGT:
		return cmp > 0
	case BinaryOpGE:
		return cmp >= 0
	default:
		return false
	}
}

// jsonValueToCell maps a JSON value to a cell. Booleans become CellLong 0/1 (the wire encoding
// clients already rely on); objects and arrays become CellJSON.
func jsonValueToCell(v kdbjson.Value, ft schema.FieldType) Cell {
	switch val := v.(type) {
	case nil, kdbjson.NullValue:
		return CellNull{}
	case kdbjson.StringValue:
		return CellString{Value: val.V}
	case kdbjson.IntValue:
		return CellLong{Value: val.V}
	case kdbjson.NumberValue:
		return CellDouble{Value: val.V}
	case kdbjson.BoolValue:
		return CellLong{Value: boolToInt64(val.V)}
	case kdbjson.ObjectValue, kdbjson.ArrayValue:
		return CellJSON{JSON: kdbjson.ToJSONString(val)}
	default:
		_ = ft
		return CellNull{}
	}
}

// cellKey renders a cell as a type-tagged string for DISTINCT and GROUP BY hashing.
func cellKey(c Cell) string {
	switch v := c.(type) {
	case nil, CellNull:
		return "n"
	case CellString:
		return "s" + v.Value
	case CellLong:
		return "l" + strconv.FormatInt(v.Value, 10)
	case CellDouble:
		if v.Value == math.Trunc(v.Value) && math.Abs(v.Value) < 1e15 {
			// 1 and 1.0 group together, matching the numeric comparison rule.
			return "l" + strconv.FormatInt(int64(v.Value), 10)
		}
		return "d" + strconv.FormatFloat(v.Value, 'g', -1, 64)
	case CellBool:
		return "l" + strconv.FormatInt(boolToInt64(v.Value), 10)
	case CellJSON:
		return "j" + v.JSON
	default:
		return "?"
	}
}
