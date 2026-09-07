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
}

// ColumnDefinition is one CREATE TABLE column.
type ColumnDefinition struct {
	Name     string
	Type     schema.FieldType
	Required bool
	Indexed  bool
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
