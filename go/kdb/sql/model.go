package sql

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Statement is a parsed SQL statement (subset).
type Statement interface {
	isStatement()
}

type StmtSelect struct{ Query SelectQuery }

func (StmtSelect) isStatement() {}

type StmtInsert struct{ Insert InsertStatement }

func (StmtInsert) isStatement() {}

type StmtCreateTable struct{ DDL CreateTableStatement }

func (StmtCreateTable) isStatement() {}

// StmtUpdate is UPDATE t SET ... [WHERE ...] (kdb-spec-layer16 §5).
type StmtUpdate struct{ Update UpdateStatement }

func (StmtUpdate) isStatement() {}

// StmtDelete is DELETE FROM t [WHERE ...] (kdb-spec-layer16 §5).
type StmtDelete struct{ Delete DeleteStatement }

func (StmtDelete) isStatement() {}

// StmtCreateIndex is CREATE [UNIQUE] INDEX name ON t (fields) [USING kind] [WITH (...)]
// (kdb-spec-layer16 §9.2). Using is the upper-cased kind ("HASH", "BTREE", "FULLTEXT",
// "VECTOR") or "" when absent; With holds the option list with lower-cased keys.
type StmtCreateIndex struct {
	Name   string
	Table  string
	Fields []IndexField
	Using  string
	Unique bool
	With   map[string]string
}

func (StmtCreateIndex) isStatement() {}

// IndexField is one indexed path with its FULLTEXT weight (1 when not given).
type IndexField struct {
	Path   string
	Weight float64
}

// StmtDropIndex is DROP INDEX name ON t.
type StmtDropIndex struct {
	Name  string
	Table string
}

func (StmtDropIndex) isStatement() {}

// UpdateStatement is the body of an UPDATE. Assignment targets are dotted JSON paths (the
// reserved target "_doc" replaces the whole document); values are evaluated against the
// pre-update document.
type UpdateStatement struct {
	Table       TableRef
	Assignments []Assignment
	Where       Expr
}

// Assignment is one SET target = value pair.
type Assignment struct {
	Path  string
	Value Expr
}

// DeleteStatement is the body of a DELETE.
type DeleteStatement struct {
	Table TableRef
	Where Expr
}

// InsertStatement is INSERT INTO ... VALUES ...
type InsertStatement struct {
	Table   TableRef
	Columns []string
	Rows    [][]Expr
}

// CreateTableStatement is CREATE TABLE ...
type CreateTableStatement struct {
	Table   TableRef
	Columns []ColumnDefinition
	// UniqueConstraints are the table-level UNIQUE (a, b, ...) tuples (kdb-spec-layer16 §9.6).
	UniqueConstraints [][]string
}

// ColumnDefinition is one CREATE TABLE column.
type ColumnDefinition struct {
	Name     string
	Type     schema.FieldType
	Required bool
	Indexed  bool
	Unique   bool
}

// TableRef names a table (catalog binding is external).
type TableRef struct {
	Name  string
	Alias string
}

// SelectQuery is a SELECT statement body.
type SelectQuery struct {
	Distinct    bool
	Projections []Projection
	// From is the zero TableRef for a table-less SELECT (`SELECT 1`), which evaluates its
	// projections once against a single synthetic row. Use HasFrom rather than comparing Name
	// directly.
	From    TableRef
	Where   Expr
	GroupBy []Expr
	OrderBy []OrderItem
	Limit   *int
	Offset  int
}

// HasFrom reports whether the query names a table.
//
// An empty name is unambiguous: readIdentifier never yields one, so a blank From can only come
// from a query that had no FROM clause at all.
func (q SelectQuery) HasFrom() bool { return q.From.Name != "" }

// Projection is SELECT list element.
type Projection interface {
	isProjection()
}

type ProjStar struct{}

func (ProjStar) isProjection() {}

type ProjColumn struct {
	Name  string
	Alias string
}

func (ProjColumn) isProjection() {}

type ProjExpression struct {
	Expr  Expr
	Alias string
}

func (ProjExpression) isProjection() {}

// OrderItem is ORDER BY entry.
type OrderItem struct {
	Expr      Expr
	Ascending bool
}

// Expr is a SQL expression AST node.
type Expr interface {
	isExpr()
}

type ExprLiteral struct{ Cell Cell }

func (ExprLiteral) isExpr() {}

type ExprColumnRef struct{ Name string }

func (ExprColumnRef) isExpr() {}

type ExprParameter struct{ Index int }

func (ExprParameter) isExpr() {}

type ExprBinary struct {
	Op          BinaryOp
	Left, Right Expr
}

func (ExprBinary) isExpr() {}

type ExprUnary struct {
	Op   UnaryOp
	Expr Expr
}

func (ExprUnary) isExpr() {}

type ExprFunctionCall struct {
	Name string
	Args []Expr
}

func (ExprFunctionCall) isExpr() {}

// ExprIn is `expr [NOT] IN (v1, v2, ...)`; the NOT form is ExprUnary{UnaryOpNot, ExprIn}.
type ExprIn struct {
	Expr   Expr
	Values []Expr
}

func (ExprIn) isExpr() {}

// ExprBetween is `expr BETWEEN low AND high` (inclusive); NOT BETWEEN wraps it in UnaryOpNot.
type ExprBetween struct {
	Expr      Expr
	Low, High Expr
}

func (ExprBetween) isExpr() {}

// ExprMatch is MATCH(index_or_field, query) - a full-text predicate (true for hits) or a score
// projection (BM25, 0 for non-hits). Requires a FULLTEXT index (kdb-spec-layer16 §9.1).
type ExprMatch struct {
	IndexOrField string
	Query        Expr
}

func (ExprMatch) isExpr() {}

// ExprSimilarity is SIMILARITY(field, vector) - the vector-index metric score for a document.
// Vector is a vector literal (ExprLiteral{CellJSON}) or a ParamVector parameter.
type ExprSimilarity struct {
	Field  string
	Vector Expr
}

func (ExprSimilarity) isExpr() {}

// ExprFuse is FUSE(arm1, arm2[, 'rrf' | 'weighted']): rank fusion over MATCH/SIMILARITY arms.
// Mode is "rrf" (default) or "weighted".
type ExprFuse struct {
	Arms []Expr
	Mode string
}

func (ExprFuse) isExpr() {}

// BinaryOp is a binary operator.
type BinaryOp int

const (
	BinaryOpEQ BinaryOp = iota
	BinaryOpNE
	BinaryOpLT
	BinaryOpLE
	BinaryOpGT
	BinaryOpGE
	BinaryOpAnd
	BinaryOpOr
	BinaryOpLike
	BinaryOpILike
)

// UnaryOp is a unary operator.
type UnaryOp int

const (
	UnaryOpNot UnaryOp = iota
	UnaryOpIsNull
)

// Cell is a runtime SQL value.
type Cell interface {
	isCell()
}

type CellNull struct{}

func (CellNull) isCell() {}

type CellString struct{ Value string }

func (CellString) isCell() {}

type CellLong struct{ Value int64 }

func (CellLong) isCell() {}

type CellDouble struct{ Value float64 }

func (CellDouble) isCell() {}

type CellJSON struct{ JSON string }

func (CellJSON) isCell() {}

// CellBool is a boolean value. It is produced by TRUE/FALSE literals and ParamBool parameters
// (so DML writes JSON booleans, not 0/1); a document's boolean field still projects as CellLong
// 0/1 for wire compatibility. Comparisons treat CellBool and CellLong 0/1 as the same type.
type CellBool struct{ Value bool }

func (CellBool) isCell() {}

// Parameter is a bound query parameter.
type Parameter interface {
	isParameter()
}

type ParamString struct{ Value string }

func (ParamString) isParameter() {}

type ParamInt struct{ Value int64 }

func (ParamInt) isParameter() {}

type ParamDouble struct{ Value float64 }

func (ParamDouble) isParameter() {}

type ParamBool struct{ Value bool }

func (ParamBool) isParameter() {}

type ParamNull struct{}

func (ParamNull) isParameter() {}

// ParamVector is the vector parameter type (kdb-spec-layer16 §9.1): a JSON array of numbers on
// the wire, bound as the query vector of SIMILARITY(field, ?).
type ParamVector struct{ Value []float32 }

func (ParamVector) isParameter() {}

// QueryContext carries execution state.
type QueryContext struct {
	NamespaceID string
	Schema      schema.KdbSchema
	AtCommit    *codec.Hash
	Parameters  []Parameter
	MaxRows     int
	// RowBudget caps how many rows a scan may *examine*, as opposed to MaxRows which caps how
	// many it returns - kdb-spec-layer13 Component 48 §5.2, closing §2.8's "no bound on scan
	// work" gap. 0 means unlimited.
	//
	// The distinction is the whole point: a `SELECT ... WHERE <selective predicate>` over a
	// namespace of ten million documents returns almost nothing and so respects any MaxRows
	// comfortably, while still reading every one of those ten million documents into memory to
	// decide that. Bounding the result bounds what the client sees; only bounding the work
	// bounds what the server spends.
	RowBudget int
	// Stats, when non-nil, is filled by the executor with what the query actually cost - rows
	// examined and bytes materialized. This is the measured "actual" that admission control's
	// cost model learns from (kdb-spec-layer13 Component 48 §5.2, P2 "cost is estimated, then
	// measured"): unlike a process-wide allocation counter, it is exact and attributable to
	// this query alone, so it stays meaningful under concurrency.
	Stats *ExecStats
}

// ExecStats records what a query actually cost while it ran. All counters are additive across
// the executor's phases (id resolution, predicate evaluation, materialization, projection).
type ExecStats struct {
	// RowsExamined is how many rows the query looked at - the quantity RowBudget bounds -
	// including rows a predicate then discarded.
	RowsExamined int
	// DocsRead is how many document fetches DocBytesRead spans - DocBytesRead/DocsRead is the
	// query's mean observed document size.
	DocsRead int
	// DocBytesRead is the total document JSON bytes fetched from storage, including transient
	// reads made only to evaluate a predicate.
	DocBytesRead int64
	// RetainedBytes is the executor's peak simultaneously-held materialization: result
	// documents plus projected row cells. This is the number an admission grant should have
	// reserved (before response encoding, which the server accounts for separately).
	RetainedBytes int64
}

// addExamined counts one examined row. Nil-safe so call sites need no guard.
func (s *ExecStats) addExamined(n int) {
	if s != nil {
		s.RowsExamined += n
	}
}

// addDocRead counts one document fetch of n bytes. Nil-safe.
func (s *ExecStats) addDocRead(n int) {
	if s != nil {
		s.DocsRead++
		s.DocBytesRead += int64(n)
	}
}

// addRetained counts n bytes materialized into the result being built. Nil-safe.
func (s *ExecStats) addRetained(n int64) {
	if s != nil {
		s.RetainedBytes += n
	}
}

// QueryResult is a SELECT or DDL result set.
type QueryResult struct {
	Columns       []ResultColumn
	Rows          []QueryRow
	RowsAffected  int
	GeneratedIDs  []string
	AppliedSchema *schema.KdbSchema
	// Plan names the access path the executor chose (kdb-spec-layer16 §9.3): "fullscan",
	// "index:eq(field)", "index:range(field)", "index:in(field)", "fulltext(name)",
	// "vector(field)", "fuse(...)". Tests assert on it to prove an index was used.
	Plan string
}

// DMLResult is the outcome of INSERT (operations for commit).
type DMLResult struct {
	Operations   []document.Op
	RowsAffected int
	GeneratedIDs []string
}

// QueryRow is one result row.
type QueryRow struct {
	Values []Cell
}

// ResultColumn describes a column in the result.
type ResultColumn struct {
	Name    string
	SQLType string
	Source  ColumnSource
}

// ColumnSource classifies result column origin.
type ColumnSource int

const (
	ColumnSourceSchemaField ColumnSource = iota
	ColumnSourceKdbID
	ColumnSourceDocJSON
	ColumnSourceExpression
)

// PhysicalPlan is a minimal scan plan.
type PhysicalPlan interface {
	isPhysicalPlan()
}

type PlanFullScan struct{ Label string }

func (PlanFullScan) isPhysicalPlan() {}

// PlanSingleRow produces exactly one synthetic row and reads no storage - the plan for a
// table-less SELECT such as `SELECT 1`.
type PlanSingleRow struct{}

func (PlanSingleRow) isPhysicalPlan() {}

type PlanFilter struct {
	Predicate Expr
	Input     PhysicalPlan
}

func (PlanFilter) isPhysicalPlan() {}

type PlanLimit struct {
	Limit, Offset int
	Input         PhysicalPlan
}

func (PlanLimit) isPhysicalPlan() {}

// PlanIndexLookup resolves ids through the executor's IndexProvider (kdb-spec-layer16 §9.3).
// Kind is "eq", "range", or "in"; the executor falls back to a full scan when the provider
// reports no usable index. Every WHERE conjunct, including the indexed one, is re-checked by the
// residual filter, so a provider that returns a superset is still correct.
type PlanIndexLookup struct {
	Kind          string
	Field         string
	Value         Expr   // eq
	Values        []Expr // in
	Low, High     Expr   // range (nil = unbounded)
	LowInclusive  bool
	HighInclusive bool
	Input         PhysicalPlan // the fallback when the index is unavailable
}

func (PlanIndexLookup) isPhysicalPlan() {}

// PlanSearch resolves ids from a MATCH / SIMILARITY / FUSE expression, in rank order.
type PlanSearch struct {
	Expr  Expr
	Label string
}

func (PlanSearch) isPhysicalPlan() {}
