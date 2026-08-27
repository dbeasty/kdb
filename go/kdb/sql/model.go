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
	From        TableRef
	Where       Expr
	OrderBy     []OrderItem
	Limit       *int
	Offset      int
}

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
