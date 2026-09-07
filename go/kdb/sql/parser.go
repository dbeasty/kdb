package sql

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/limidus/kdb/go/kdb/schema"
)

// Parser parses SQL text into statements.
type Parser interface {
	Parse(sql string) (Statement, error)
}

// DefaultParser is a recursive-descent parser for the KDB SQL subset: SELECT (with WHERE,
// GROUP BY, ORDER BY, LIMIT/OFFSET, DISTINCT, aggregates, MATCH/SIMILARITY/FUSE), INSERT,
// UPDATE, DELETE, CREATE TABLE, CREATE INDEX, DROP INDEX (kdb-spec-layer16 §4, §5, §9).
//
// Every malformed input is reported as a *ParseError. Nothing in here panics: Parse runs in the
// connection goroutine of every client, where an unrecovered panic would end far more than the
// one bad statement.
type DefaultParser struct{}

func (DefaultParser) Parse(sql string) (stmt Statement, err error) {
	p := &rdParser{input: strings.TrimSpace(sql)}
	defer func() {
		// Belt and braces: the parser is written not to panic, but a panic here must still
		// surface as an error, never unwind into the caller's goroutine.
		if r := recover(); r != nil {
			stmt = nil
			err = NewParseError("internal parser error", p.input, p.pos)
		}
	}()
	stmt, err = p.parseStatement()
	if err != nil {
		return nil, err
	}
	p.matchChar(';')
	p.skipWS()
	if p.pos < len(p.input) {
		return nil, p.parseError("unexpected input after statement")
	}
	return stmt, nil
}

type rdParser struct {
	input          string
	pos            int
	nextParamIndex int
	lastKeyword    string
}

func (p *rdParser) parseStatement() (Statement, error) {
	p.skipWS()
	switch {
	case p.matchKeyword("SELECT"):
		q, err := p.parseSelectQuery()
		if err != nil {
			return nil, err
		}
		return StmtSelect{Query: q}, nil
	case p.matchKeyword("INSERT"):
		ins, err := p.parseInsert()
		if err != nil {
			return nil, err
		}
		return StmtInsert{Insert: ins}, nil
	case p.matchKeyword("UPDATE"):
		upd, err := p.parseUpdate()
		if err != nil {
			return nil, err
		}
		return StmtUpdate{Update: upd}, nil
	case p.matchKeyword("DELETE"):
		del, err := p.parseDelete()
		if err != nil {
			return nil, err
		}
		return StmtDelete{Delete: del}, nil
	case p.matchKeyword("CREATE"):
		if p.matchKeyword("TABLE") {
			ddl, err := p.parseCreateTableBody()
			if err != nil {
				return nil, err
			}
			return StmtCreateTable{DDL: ddl}, nil
		}
		unique := p.matchKeyword("UNIQUE")
		if p.matchKeyword("INDEX") {
			return p.parseCreateIndexBody(unique)
		}
		return nil, p.parseError("expected TABLE or INDEX")
	case p.matchKeyword("DROP"):
		if !p.matchKeyword("INDEX") {
			return nil, p.parseError("expected INDEX")
		}
		return p.parseDropIndexBody()
	default:
		return nil, p.parseError("expected SELECT, INSERT, UPDATE, DELETE, CREATE, or DROP")
	}
}

// --- DDL ---------------------------------------------------------------------------------

func (p *rdParser) parseCreateTableBody() (CreateTableStatement, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return CreateTableStatement{}, err
	}
	table := TableRef{Name: name}
	if err := p.expectChar('('); err != nil {
		return CreateTableStatement{}, err
	}
	var columns []ColumnDefinition
	var constraints [][]string
	for {
		if p.matchKeyword("UNIQUE") {
			fields, err := p.parseIdentifierList()
			if err != nil {
				return CreateTableStatement{}, err
			}
			constraints = append(constraints, fields)
		} else {
			col, err := p.parseColumnDefinition()
			if err != nil {
				return CreateTableStatement{}, err
			}
			columns = append(columns, col)
		}
		if !p.matchChar(',') {
			break
		}
	}
	if err := p.expectChar(')'); err != nil {
		return CreateTableStatement{}, err
	}
	if len(columns) == 0 {
		return CreateTableStatement{}, p.parseError("CREATE TABLE needs at least one column")
	}
	return CreateTableStatement{Table: table, Columns: columns, UniqueConstraints: constraints}, nil
}

// parseIdentifierList reads "( a, b, c )".
func (p *rdParser) parseIdentifierList() ([]string, error) {
	if err := p.expectChar('('); err != nil {
		return nil, err
	}
	var out []string
	for {
		id, err := p.readIdentifier()
		if err != nil {
			return nil, err
		}
		out = append(out, id)
		if !p.matchChar(',') {
			break
		}
	}
	if err := p.expectChar(')'); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *rdParser) parseColumnDefinition() (ColumnDefinition, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return ColumnDefinition{}, err
	}
	typ, err := p.parseColumnType()
	if err != nil {
		return ColumnDefinition{}, err
	}
	col := ColumnDefinition{Name: name, Type: typ, Indexed: true}
	for {
		switch {
		case p.matchKeyword("NOT"):
			if err := p.expectKeyword("NULL"); err != nil {
				return ColumnDefinition{}, err
			}
			col.Required = true
		case p.matchKeyword("UNIQUE"):
			col.Unique = true
		default:
			return col, nil
		}
	}
}

func (p *rdParser) parseColumnType() (schema.FieldType, error) {
	id, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	name := strings.ToUpper(id)
	if p.peek() == '(' {
		p.consume()
		p.readNumber()
		if err := p.expectChar(')'); err != nil {
			return nil, err
		}
	}
	switch name {
	case "VARCHAR", "TEXT", "STRING", "CHAR":
		return schema.StringType{}, nil
	case "INT", "INTEGER":
		return schema.Int32Type{}, nil
	case "BIGINT", "LONG":
		return schema.Int64Type{}, nil
	case "DOUBLE", "FLOAT", "REAL":
		return schema.Float64Type{}, nil
	case "BOOLEAN", "BOOL":
		return schema.BoolType{}, nil
	case "TIMESTAMP", "DATETIME":
		return schema.TimestampType{}, nil
	case "UUID":
		return schema.UUIDType{}, nil
	default:
		return nil, p.parseError("unsupported column type: " + name)
	}
}

func (p *rdParser) parseCreateIndexBody(unique bool) (Statement, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.expectChar('('); err != nil {
		return nil, err
	}
	var fields []IndexField
	for {
		path, err := p.readIdentifier()
		if err != nil {
			return nil, err
		}
		f := IndexField{Path: path, Weight: 1}
		if p.matchKeyword("WEIGHT") {
			w, err := p.readFloat()
			if err != nil {
				return nil, err
			}
			f.Weight = w
		}
		fields = append(fields, f)
		if !p.matchChar(',') {
			break
		}
	}
	if err := p.expectChar(')'); err != nil {
		return nil, err
	}
	stmt := StmtCreateIndex{Name: name, Table: table, Fields: fields, Unique: unique}
	if p.matchKeyword("USING") {
		kind, err := p.readIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.Using = strings.ToUpper(kind)
		switch stmt.Using {
		case "HASH", "BTREE", "FULLTEXT", "VECTOR":
		default:
			return nil, p.parseError("unsupported index kind: " + stmt.Using)
		}
	}
	if p.matchKeyword("WITH") {
		if err := p.expectChar('('); err != nil {
			return nil, err
		}
		stmt.With = map[string]string{}
		for {
			key, err := p.readIdentifier()
			if err != nil {
				return nil, err
			}
			if !p.matchOp("=") {
				return nil, p.parseError("expected '='")
			}
			val, err := p.readOptionValue()
			if err != nil {
				return nil, err
			}
			stmt.With[strings.ToLower(key)] = val
			if !p.matchChar(',') {
				break
			}
		}
		if err := p.expectChar(')'); err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

// readOptionValue reads a WITH option value: a string literal, a number, or a bare word.
func (p *rdParser) readOptionValue() (string, error) {
	p.skipWS()
	switch {
	case p.peek() == '\'':
		return p.readStringLiteral()
	case p.peek() == '-' || unicode.IsDigit(rune(p.peek())):
		neg := p.matchOp("-")
		n := p.readNumber()
		if n == "" {
			return "", p.parseError("expected number")
		}
		if neg {
			return "-" + n, nil
		}
		return n, nil
	default:
		return p.readIdentifier()
	}
}

func (p *rdParser) parseDropIndexBody() (Statement, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	return StmtDropIndex{Name: name, Table: table}, nil
}

// --- DML ---------------------------------------------------------------------------------

func (p *rdParser) parseInsert() (InsertStatement, error) {
	if err := p.expectKeyword("INTO"); err != nil {
		return InsertStatement{}, err
	}
	name, err := p.readIdentifier()
	if err != nil {
		return InsertStatement{}, err
	}
	table := TableRef{Name: name}
	columns, err := p.parseIdentifierList()
	if err != nil {
		return InsertStatement{}, err
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return InsertStatement{}, err
	}
	var rows [][]Expr
	for {
		if err := p.expectChar('('); err != nil {
			return InsertStatement{}, err
		}
		var values []Expr
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return InsertStatement{}, err
			}
			values = append(values, expr)
			if !p.matchChar(',') {
				break
			}
		}
		if err := p.expectChar(')'); err != nil {
			return InsertStatement{}, err
		}
		rows = append(rows, values)
		if !p.matchChar(',') {
			break
		}
	}
	return InsertStatement{Table: table, Columns: columns, Rows: rows}, nil
}

func (p *rdParser) parseUpdate() (UpdateStatement, error) {
	table, err := p.parseTableRef()
	if err != nil {
		return UpdateStatement{}, err
	}
	if err := p.expectKeyword("SET"); err != nil {
		return UpdateStatement{}, err
	}
	var assignments []Assignment
	for {
		path, err := p.readIdentifier()
		if err != nil {
			return UpdateStatement{}, err
		}
		if !p.matchOp("=") {
			return UpdateStatement{}, p.parseError("expected '='")
		}
		value, err := p.parseExpr()
		if err != nil {
			return UpdateStatement{}, err
		}
		assignments = append(assignments, Assignment{Path: path, Value: value})
		if !p.matchChar(',') {
			break
		}
	}
	var where Expr
	if p.matchKeyword("WHERE") {
		where, err = p.parseExpr()
		if err != nil {
			return UpdateStatement{}, err
		}
	}
	return UpdateStatement{Table: table, Assignments: assignments, Where: where}, nil
}

func (p *rdParser) parseDelete() (DeleteStatement, error) {
	if err := p.expectKeyword("FROM"); err != nil {
		return DeleteStatement{}, err
	}
	table, err := p.parseTableRef()
	if err != nil {
		return DeleteStatement{}, err
	}
	var where Expr
	if p.matchKeyword("WHERE") {
		where, err = p.parseExpr()
		if err != nil {
			return DeleteStatement{}, err
		}
	}
	return DeleteStatement{Table: table, Where: where}, nil
}

// --- SELECT ------------------------------------------------------------------------------

func (p *rdParser) parseSelectQuery() (SelectQuery, error) {
	distinct := p.matchKeyword("DISTINCT")
	projections, err := p.parseProjections()
	if err != nil {
		return SelectQuery{}, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return SelectQuery{}, err
	}
	table, err := p.parseTableRef()
	if err != nil {
		return SelectQuery{}, err
	}
	var where Expr
	if p.matchKeyword("WHERE") {
		where, err = p.parseExpr()
		if err != nil {
			return SelectQuery{}, err
		}
	}
	var groupBy []Expr
	if p.matchKeyword("GROUP") {
		if err := p.expectKeyword("BY"); err != nil {
			return SelectQuery{}, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return SelectQuery{}, err
			}
			groupBy = append(groupBy, expr)
			if !p.matchChar(',') {
				break
			}
		}
	}
	var orderBy []OrderItem
	if p.matchKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return SelectQuery{}, err
		}
		for {
			expr, err := p.parseExpr()
			if err != nil {
				return SelectQuery{}, err
			}
			asc := true
			if p.matchKeyword("DESC") {
				asc = false
			} else {
				p.matchKeyword("ASC")
			}
			orderBy = append(orderBy, OrderItem{Expr: expr, Ascending: asc})
			if !p.matchChar(',') {
				break
			}
		}
	}
	var limit *int
	offset := 0
	if p.matchKeyword("LIMIT") {
		n, err := p.readInt()
		if err != nil {
			return SelectQuery{}, err
		}
		if n < 0 {
			return SelectQuery{}, p.parseError("LIMIT must not be negative")
		}
		limit = &n
	}
	if p.matchKeyword("OFFSET") {
		n, err := p.readInt()
		if err != nil {
			return SelectQuery{}, err
		}
		if n < 0 {
			return SelectQuery{}, p.parseError("OFFSET must not be negative")
		}
		offset = n
	}
	return SelectQuery{
		Distinct: distinct, Projections: projections, From: table,
		Where: where, GroupBy: groupBy, OrderBy: orderBy, Limit: limit, Offset: offset,
	}, nil
}

// parseTableRef reads "name [AS] [alias]". A bare word after the table name is an alias unless
// it is a clause keyword (WHERE, SET, ORDER, ...).
func (p *rdParser) parseTableRef() (TableRef, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return TableRef{}, err
	}
	ref := TableRef{Name: name}
	if p.matchKeyword("AS") {
		alias, err := p.readIdentifier()
		if err != nil {
			return TableRef{}, err
		}
		ref.Alias = alias
		return ref, nil
	}
	if p.peekIdentifierIsAlias() {
		alias, err := p.readIdentifier()
		if err != nil {
			return TableRef{}, err
		}
		ref.Alias = alias
	}
	return ref, nil
}

// clauseKeywords are words that can follow a table reference or a projection and so must not
// be swallowed as an alias.
var clauseKeywords = map[string]bool{
	"WHERE": true, "SET": true, "ORDER": true, "GROUP": true, "LIMIT": true, "OFFSET": true,
	"FROM": true, "HAVING": true, "ON": true, "USING": true, "WITH": true, "AND": true,
	"OR": true, "NOT": true, "AS": true, "VALUES": true, "INTO": true, "BY": true,
	"ASC": true, "DESC": true, "JOIN": true, "INNER": true, "LEFT": true, "UNION": true,
	"IS": true, "IN": true, "LIKE": true, "ILIKE": true, "BETWEEN": true,
}

func (p *rdParser) peekIdentifierIsAlias() bool {
	p.skipWS()
	save := p.pos
	id, err := p.readIdentifier()
	p.pos = save
	if err != nil {
		return false
	}
	return !clauseKeywords[strings.ToUpper(id)] && !strings.Contains(id, ".")
}

func (p *rdParser) parseProjections() ([]Projection, error) {
	var out []Projection
	for {
		p.skipWS()
		if p.peek() == '*' {
			p.consume()
			out = append(out, ProjStar{})
		} else {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			alias, err := p.parseOptionalAlias()
			if err != nil {
				return nil, err
			}
			if col, ok := expr.(ExprColumnRef); ok {
				out = append(out, ProjColumn{Name: col.Name, Alias: alias})
			} else {
				out = append(out, ProjExpression{Expr: expr, Alias: alias})
			}
		}
		if !p.matchChar(',') {
			break
		}
	}
	return out, nil
}

func (p *rdParser) parseOptionalAlias() (string, error) {
	p.skipWS()
	if p.matchKeyword("AS") {
		return p.readIdentifier()
	}
	if p.peekIdentifierIsAlias() {
		return p.readIdentifier()
	}
	return "", nil
}

// --- expressions -------------------------------------------------------------------------

func (p *rdParser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *rdParser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = ExprBinary{Op: BinaryOpOr, Left: left, Right: right}
	}
	return left, nil
}

func (p *rdParser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("AND") {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = ExprBinary{Op: BinaryOpAnd, Left: left, Right: right}
	}
	return left, nil
}

func (p *rdParser) parseNot() (Expr, error) {
	if p.matchKeyword("NOT") {
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return ExprUnary{Op: UnaryOpNot, Expr: inner}, nil
	}
	return p.parseComparison()
}

func (p *rdParser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.matchKeyword("IS") {
		negated := p.matchKeyword("NOT")
		if err := p.expectKeyword("NULL"); err != nil {
			return nil, err
		}
		var e Expr = ExprUnary{Op: UnaryOpIsNull, Expr: left}
		if negated {
			e = ExprUnary{Op: UnaryOpNot, Expr: e}
		}
		return e, nil
	}
	negated := p.matchKeyword("NOT")
	switch {
	case p.matchKeyword("LIKE"), p.matchKeyword("ILIKE"):
		op := BinaryOpLike
		if strings.EqualFold(p.lastKeyword, "ILIKE") {
			op = BinaryOpILike
		}
		pattern, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return negate(negated, ExprBinary{Op: op, Left: left, Right: pattern}), nil
	case p.matchKeyword("IN"):
		if err := p.expectChar('('); err != nil {
			return nil, err
		}
		var values []Expr
		for {
			v, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			values = append(values, v)
			if !p.matchChar(',') {
				break
			}
		}
		if err := p.expectChar(')'); err != nil {
			return nil, err
		}
		return negate(negated, ExprIn{Expr: left, Values: values}), nil
	case p.matchKeyword("BETWEEN"):
		low, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		high, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return negate(negated, ExprBetween{Expr: left, Low: low, High: high}), nil
	}
	if negated {
		return nil, p.parseError("expected LIKE, ILIKE, IN, or BETWEEN after NOT")
	}
	var op BinaryOp
	switch {
	case p.matchOp("="):
		op = BinaryOpEQ
	case p.matchOp("<>"), p.matchOp("!="):
		op = BinaryOpNE
	case p.matchOp("<="):
		op = BinaryOpLE
	case p.matchOp(">="):
		op = BinaryOpGE
	case p.matchOp("<"):
		op = BinaryOpLT
	case p.matchOp(">"):
		op = BinaryOpGT
	default:
		return left, nil
	}
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return ExprBinary{Op: op, Left: left, Right: right}, nil
}

func negate(negated bool, e Expr) Expr {
	if negated {
		return ExprUnary{Op: UnaryOpNot, Expr: e}
	}
	return e
}

func (p *rdParser) parsePrimary() (Expr, error) {
	p.skipWS()
	c := p.peek()
	switch {
	case c == '\'':
		s, err := p.readStringLiteral()
		if err != nil {
			return nil, err
		}
		return ExprLiteral{Cell: CellString{Value: s}}, nil
	case c == '?':
		p.consume()
		idx := p.nextParamIndex
		p.nextParamIndex++
		return ExprParameter{Index: idx}, nil
	case c == '-' || c == '+' || unicode.IsDigit(rune(c)):
		return p.parseNumberLiteral()
	case c == '[':
		return p.parseVectorLiteral()
	case c == '(':
		p.consume()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, err
		}
		return e, nil
	case c == '_' || unicode.IsLetter(rune(c)):
		return p.parseIdentifierExpr()
	default:
		return nil, p.parseError("expected expression")
	}
}

func (p *rdParser) parseNumberLiteral() (Expr, error) {
	neg := false
	if p.peek() == '-' {
		neg = true
		p.consume()
	} else if p.peek() == '+' {
		p.consume()
	}
	num := p.readNumber()
	if num == "" {
		return nil, p.parseError("expected number")
	}
	if strings.ContainsAny(num, ".eE") {
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return nil, p.parseError("invalid number: " + num)
		}
		if neg {
			f = -f
		}
		return ExprLiteral{Cell: CellDouble{Value: f}}, nil
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return nil, p.parseError("invalid integer: " + num)
	}
	if neg {
		n = -n
	}
	return ExprLiteral{Cell: CellLong{Value: n}}, nil
}

// parseVectorLiteral reads "[0.1, 0.2, ...]" into a CellJSON holding the compact array text;
// DecodeVector turns it back into []float32 at execution time.
func (p *rdParser) parseVectorLiteral() (Expr, error) {
	if err := p.expectChar('['); err != nil {
		return nil, err
	}
	var sb strings.Builder
	sb.WriteByte('[')
	p.skipWS()
	if p.peek() != ']' {
		for {
			e, err := p.parseNumberLiteral()
			if err != nil {
				return nil, err
			}
			if sb.Len() > 1 {
				sb.WriteByte(',')
			}
			switch c := e.(ExprLiteral).Cell.(type) {
			case CellLong:
				sb.WriteString(strconv.FormatInt(c.Value, 10))
			case CellDouble:
				sb.WriteString(strconv.FormatFloat(c.Value, 'g', -1, 64))
			}
			if !p.matchChar(',') {
				break
			}
			p.skipWS()
		}
	}
	if err := p.expectChar(']'); err != nil {
		return nil, err
	}
	sb.WriteByte(']')
	return ExprLiteral{Cell: CellJSON{JSON: sb.String()}}, nil
}

func (p *rdParser) parseIdentifierExpr() (Expr, error) {
	name, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(name) {
	case "NULL":
		return ExprLiteral{Cell: CellNull{}}, nil
	case "TRUE":
		return ExprLiteral{Cell: CellBool{Value: true}}, nil
	case "FALSE":
		return ExprLiteral{Cell: CellBool{Value: false}}, nil
	}
	p.skipWS()
	if p.peek() != '(' {
		return ExprColumnRef{Name: name}, nil
	}
	p.consume()
	lower := strings.ToLower(name)
	var args []Expr
	p.skipWS()
	if p.peek() == '*' && lower == "count" {
		p.consume()
		args = []Expr{ExprColumnRef{Name: "*"}}
	} else if p.peek() != ')' {
		for {
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if !p.matchChar(',') {
				break
			}
		}
	}
	if err := p.expectChar(')'); err != nil {
		return nil, err
	}
	return p.buildFunctionCall(lower, args)
}

// buildFunctionCall turns the search functions into their dedicated AST nodes and leaves every
// other call as ExprFunctionCall (aggregates, ARRAY_* helpers, and anything the planner will
// reject later).
func (p *rdParser) buildFunctionCall(name string, args []Expr) (Expr, error) {
	switch name {
	case "match":
		if len(args) != 2 {
			return nil, p.parseError("MATCH takes (index_or_field, query)")
		}
		col, ok := args[0].(ExprColumnRef)
		if !ok {
			return nil, p.parseError("MATCH: first argument must be an index or field name")
		}
		return ExprMatch{IndexOrField: col.Name, Query: args[1]}, nil
	case "similarity":
		if len(args) != 2 {
			return nil, p.parseError("SIMILARITY takes (field, vector)")
		}
		col, ok := args[0].(ExprColumnRef)
		if !ok {
			return nil, p.parseError("SIMILARITY: first argument must be a field name")
		}
		switch v := args[1].(type) {
		case ExprParameter:
		case ExprLiteral:
			if _, ok := v.Cell.(CellJSON); !ok {
				return nil, p.parseError("SIMILARITY: vector must be a [..] literal or a parameter")
			}
		default:
			return nil, p.parseError("SIMILARITY: vector must be a [..] literal or a parameter")
		}
		return ExprSimilarity{Field: col.Name, Vector: args[1]}, nil
	case "fuse":
		if len(args) < 2 || len(args) > 3 {
			return nil, p.parseError("FUSE takes (arm1, arm2[, 'rrf' | 'weighted'])")
		}
		mode := "rrf"
		if len(args) == 3 {
			lit, ok := args[2].(ExprLiteral)
			s, okS := lit.Cell.(CellString)
			if !ok || !okS {
				return nil, p.parseError("FUSE: mode must be 'rrf' or 'weighted'")
			}
			mode = strings.ToLower(s.Value)
			if mode != "rrf" && mode != "weighted" {
				return nil, p.parseError("FUSE: mode must be 'rrf' or 'weighted'")
			}
		}
		for _, arm := range args[:2] {
			switch arm.(type) {
			case ExprMatch, ExprSimilarity:
			default:
				return nil, p.parseError("FUSE: arms must be MATCH or SIMILARITY calls")
			}
		}
		return ExprFuse{Arms: args[:2], Mode: mode}, nil
	default:
		return ExprFunctionCall{Name: name, Args: args}, nil
	}
}

// --- lexing ------------------------------------------------------------------------------

func (p *rdParser) readStringLiteral() (string, error) {
	if err := p.expectChar('\''); err != nil {
		return "", err
	}
	var sb strings.Builder
	closed := false
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		p.pos++
		if c == '\'' {
			if p.pos < len(p.input) && p.input[p.pos] == '\'' {
				sb.WriteByte('\'')
				p.pos++
			} else {
				closed = true
				break
			}
		} else {
			sb.WriteByte(c)
		}
	}
	if !closed {
		return "", p.parseError("unterminated string literal")
	}
	return sb.String(), nil
}

// readNumber reads digits with an optional fraction and exponent. It returns "" when no digit
// is present.
func (p *rdParser) readNumber() string {
	p.skipWS()
	start := p.pos
	for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
		p.pos++
	}
	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
			p.pos++
		}
	}
	if p.pos < len(p.input) && (p.input[p.pos] == 'e' || p.input[p.pos] == 'E') {
		save := p.pos
		p.pos++
		if p.pos < len(p.input) && (p.input[p.pos] == '-' || p.input[p.pos] == '+') {
			p.pos++
		}
		ds := p.pos
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
			p.pos++
		}
		if p.pos == ds {
			p.pos = save
		}
	}
	return p.input[start:p.pos]
}

func (p *rdParser) readInt() (int, error) {
	n := p.readNumber()
	if n == "" {
		return 0, p.parseError("expected integer")
	}
	v, err := strconv.Atoi(n)
	if err != nil {
		return 0, p.parseError("invalid integer")
	}
	return v, nil
}

func (p *rdParser) readFloat() (float64, error) {
	n := p.readNumber()
	if n == "" {
		return 0, p.parseError("expected number")
	}
	v, err := strconv.ParseFloat(n, 64)
	if err != nil {
		return 0, p.parseError("invalid number")
	}
	return v, nil
}

// readIdentifier reads a (possibly dotted) identifier: segments of letters, digits, and
// underscores joined by single dots, e.g. "status", "t.status", "collaborators.userId".
func (p *rdParser) readIdentifier() (string, error) {
	p.skipWS()
	start := p.pos
	for {
		segStart := p.pos
		if p.pos < len(p.input) && (p.input[p.pos] == '_' || unicode.IsLetter(rune(p.input[p.pos]))) {
			p.pos++
			for p.pos < len(p.input) && (unicode.IsLetter(rune(p.input[p.pos])) || unicode.IsDigit(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
				p.pos++
			}
		}
		if p.pos == segStart {
			p.pos = start
			return "", p.parseError("expected identifier")
		}
		if p.pos < len(p.input) && p.input[p.pos] == '.' {
			p.pos++
			continue
		}
		break
	}
	return p.input[start:p.pos], nil
}

func (p *rdParser) expectKeyword(kw string) error {
	if !p.matchKeyword(kw) {
		return p.parseError("expected " + kw)
	}
	return nil
}

func (p *rdParser) matchKeyword(kw string) bool {
	p.skipWS()
	if p.pos+len(kw) > len(p.input) {
		return false
	}
	if !strings.EqualFold(p.input[p.pos:p.pos+len(kw)], kw) {
		return false
	}
	if p.pos+len(kw) < len(p.input) {
		n := p.input[p.pos+len(kw)]
		if unicode.IsLetter(rune(n)) || unicode.IsDigit(rune(n)) || n == '_' || n == '.' {
			return false
		}
	}
	p.pos += len(kw)
	p.lastKeyword = kw
	return true
}

func (p *rdParser) matchOp(op string) bool {
	p.skipWS()
	if !strings.HasPrefix(p.input[p.pos:], op) {
		return false
	}
	p.pos += len(op)
	return true
}

func (p *rdParser) matchChar(c byte) bool {
	p.skipWS()
	if p.peek() != c {
		return false
	}
	p.pos++
	return true
}

func (p *rdParser) expectChar(c byte) error {
	if !p.matchChar(c) {
		return p.parseError("expected '" + string(c) + "'")
	}
	return nil
}

func (p *rdParser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *rdParser) consume() {
	if p.pos < len(p.input) {
		p.pos++
	}
}

func (p *rdParser) skipWS() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *rdParser) parseError(msg string) error {
	return NewParseError(msg, p.input, p.pos)
}
