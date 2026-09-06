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

// DefaultParser is a recursive-descent parser for SELECT, INSERT, CREATE TABLE.
type DefaultParser struct{}

func (DefaultParser) Parse(sql string) (Statement, error) {
	p := &rdParser{input: strings.TrimSpace(sql)}
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
	case p.matchKeyword("CREATE"):
		if !p.matchKeyword("TABLE") {
			return nil, p.parseError("expected TABLE")
		}
		ddl, err := p.parseCreateTableBody()
		if err != nil {
			return nil, err
		}
		return StmtCreateTable{DDL: ddl}, nil
	default:
		return nil, p.parseError("expected SELECT, INSERT, or CREATE TABLE")
	}
}

type rdParser struct {
	input          string
	pos            int
	nextParamIndex int
}

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
	for {
		col, err := p.parseColumnDefinition()
		if err != nil {
			return CreateTableStatement{}, err
		}
		columns = append(columns, col)
		if !p.matchChar(',') {
			break
		}
	}
	if err := p.expectChar(')'); err != nil {
		return CreateTableStatement{}, err
	}
	return CreateTableStatement{Table: table, Columns: columns}, nil
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
	required := false
	if p.matchKeyword("NOT") {
		if err := p.expectKeyword("NULL"); err != nil {
			return ColumnDefinition{}, err
		}
		required = true
	}
	return ColumnDefinition{Name: name, Type: typ, Required: required, Indexed: true}, nil
}

func (p *rdParser) parseColumnType() (schema.FieldType, error) {
	ident, err := p.readIdentifier()
	if err != nil {
		return nil, err
	}
	name := strings.ToUpper(ident)
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

func (p *rdParser) parseInsert() (InsertStatement, error) {
	if err := p.expectKeyword("INTO"); err != nil {
		return InsertStatement{}, err
	}
	name, err := p.readIdentifier()
	if err != nil {
		return InsertStatement{}, err
	}
	table := TableRef{Name: name}
	if err := p.expectChar('('); err != nil {
		return InsertStatement{}, err
	}
	var columns []string
	for {
		column, err := p.readIdentifier()
		if err != nil {
			return InsertStatement{}, err
		}
		columns = append(columns, column)
		if !p.matchChar(',') {
			break
		}
	}
	if err := p.expectChar(')'); err != nil {
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

func (p *rdParser) parseSelectQuery() (SelectQuery, error) {
	distinct := p.matchKeyword("DISTINCT")
	projections, err := p.parseProjections()
	if err != nil {
		return SelectQuery{}, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return SelectQuery{}, err
	}
	tableName, err := p.readIdentifier()
	if err != nil {
		return SelectQuery{}, err
	}
	table := TableRef{Name: tableName}
	var where Expr
	if p.matchKeyword("WHERE") {
		where, err = p.parseExpr()
		if err != nil {
			return SelectQuery{}, err
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
		limit = &n
	}
	if p.matchKeyword("OFFSET") {
		n, err := p.readInt()
		if err != nil {
			return SelectQuery{}, err
		}
		offset = n
	}
	return SelectQuery{
		Distinct: distinct, Projections: projections, From: table,
		Where: where, OrderBy: orderBy, Limit: limit, Offset: offset,
	}, nil
}

func (p *rdParser) parseProjections() ([]Projection, error) {
	var out []Projection
	for {
		p.skipWS()
		if p.peek() == '*' {
			p.consume()
			out = append(out, ProjStar{})
		} else if p.matchKeyword("COUNT") {
			if err := p.expectChar('('); err != nil {
				return nil, err
			}
			p.skipWS()
			var arg Expr
			if p.peek() == '*' {
				p.consume()
				arg = ExprColumnRef{Name: "*"}
			} else {
				var err error
				arg, err = p.parseExpr()
				if err != nil {
					return nil, err
				}
			}
			if err := p.expectChar(')'); err != nil {
				return nil, err
			}
			alias, err := p.parseOptionalAlias()
			if err != nil {
				return nil, err
			}
			out = append(out, ProjExpression{Expr: ExprFunctionCall{Name: "count", Args: []Expr{arg}}, Alias: alias})
		} else {
			name, err := p.readIdentifier()
			if err != nil {
				return nil, err
			}
			alias, err := p.parseOptionalAlias()
			if err != nil {
				return nil, err
			}
			out = append(out, ProjColumn{Name: name, Alias: alias})
		}
		if !p.matchChar(',') {
			break
		}
	}
	return out, nil
}

// parseOptionalAlias reads an `AS <name>` clause if one is present. The alias itself is not
// optional once AS has been consumed - `SELECT a AS` with nothing after it is an error, not an
// empty alias, and used to be one of the panicking paths.
func (p *rdParser) parseOptionalAlias() (string, error) {
	p.skipWS()
	if p.matchKeyword("AS") {
		return p.readIdentifier()
	}
	return "", nil
}

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
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("AND") {
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = ExprBinary{Op: BinaryOpAnd, Left: left, Right: right}
	}
	return left, nil
}

func (p *rdParser) parseComparison() (Expr, error) {
	if p.matchKeyword("NOT") {
		inner, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		return ExprUnary{Op: UnaryOpNot, Expr: inner}, nil
	}
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.matchKeyword("IS") {
		if p.matchKeyword("NOT") {
			if err := p.expectKeyword("NULL"); err != nil {
				return nil, err
			}
			return ExprUnary{Op: UnaryOpNot, Expr: ExprUnary{Op: UnaryOpIsNull, Expr: left}}, nil
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return nil, err
		}
		return ExprUnary{Op: UnaryOpIsNull, Expr: left}, nil
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

func (p *rdParser) parsePrimary() (Expr, error) {
	p.skipWS()
	switch {
	case p.peek() == '\'':
		s, err := p.readStringLiteral()
		if err != nil {
			return nil, err
		}
		return ExprLiteral{Cell: CellString{Value: s}}, nil
	case p.peek() == '?':
		p.consume()
		idx := p.nextParamIndex
		p.nextParamIndex++
		return ExprParameter{Index: idx}, nil
	case unicode.IsDigit(rune(p.peek())):
		num := p.readNumber()
		// Both conversions used to discard their error, which silently turned anything
		// readNumber accepted but strconv did not - "1.2.3", or an integer past 2^63 - into 0.
		// A literal quietly becoming a different literal is worse than a rejected statement:
		// the query runs and returns the wrong rows.
		if strings.Contains(num, ".") {
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return nil, p.parseError("invalid numeric literal " + strconv.Quote(num))
			}
			return ExprLiteral{Cell: CellDouble{Value: f}}, nil
		}
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, p.parseError("invalid integer literal " + strconv.Quote(num))
		}
		return ExprLiteral{Cell: CellLong{Value: n}}, nil
	case p.matchKeyword("NULL"):
		return ExprLiteral{Cell: CellNull{}}, nil
	case unicode.IsLetter(rune(p.peek())) || p.peek() == '_':
		name, err := p.readIdentifier()
		if err != nil {
			return nil, err
		}
		if p.peek() == '(' {
			p.consume()
			var args []Expr
			if p.peek() != ')' {
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
			return ExprFunctionCall{Name: strings.ToLower(name), Args: args}, nil
		}
		return ExprColumnRef{Name: name}, nil
	case p.peek() == '(':
		p.consume()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, err
		}
		return e, nil
	default:
		return nil, p.parseError("expected expression")
	}
}

func (p *rdParser) readStringLiteral() (string, error) {
	if err := p.expectChar('\''); err != nil {
		return "", err
	}
	var sb strings.Builder
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		p.pos++
		if c == '\'' {
			if p.pos < len(p.input) && p.input[p.pos] == '\'' {
				sb.WriteByte('\'')
				p.pos++
			} else {
				break
			}
		} else {
			sb.WriteByte(c)
		}
	}
	return sb.String(), nil
}

func (p *rdParser) readNumber() string {
	p.skipWS()
	start := p.pos
	for p.pos < len(p.input) && (unicode.IsDigit(rune(p.input[p.pos])) || p.input[p.pos] == '.') {
		p.pos++
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

// readIdentifier reads one identifier, or reports where one was required and what was found
// instead.
//
// This used to panic rather than return an error, which was not a local wart: nothing on the
// server's frame-handling path recovered, so `SELECT 1` - a projection that is a literal rather
// than an identifier, and a standard connectivity probe - unwound out of the connection
// goroutine and killed the whole process, taking every other connection and namespace with it.
// Every other function in this parser already returns errors, and handleSqlExec is written to
// turn a parse error into a clean SqlResult; only this one signature made panicking look
// necessary.
func (p *rdParser) readIdentifier() (string, error) {
	p.skipWS()
	start := p.pos
	if p.pos < len(p.input) && (p.input[p.pos] == '_' || unicode.IsLetter(rune(p.input[p.pos]))) {
		p.pos++
		for p.pos < len(p.input) && (unicode.IsLetter(rune(p.input[p.pos])) || unicode.IsDigit(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
			p.pos++
		}
	}
	if start == p.pos {
		return "", p.parseError("expected identifier" + p.foundSuffix())
	}
	return p.input[start:p.pos], nil
}

// foundSuffix describes what is actually at the cursor, for an error message that says what went
// wrong rather than only what was wanted. "expected identifier" alone leaves a caller staring at
// a statement with no idea which token offended; ParseError already carries the position, but the
// offending text is what a human reads first.
func (p *rdParser) foundSuffix() string {
	p.skipWS()
	if p.pos >= len(p.input) {
		return ", found end of statement"
	}
	end := p.pos
	for end < len(p.input) && !unicode.IsSpace(rune(p.input[end])) && end-p.pos < 16 {
		end++
	}
	if end == p.pos {
		end = p.pos + 1
	}
	return ", found " + strconv.Quote(p.input[p.pos:end])
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
		if unicode.IsLetter(rune(n)) || unicode.IsDigit(rune(n)) {
			return false
		}
	}
	p.pos += len(kw)
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

func (p *rdParser) consume() { p.pos++ }

func (p *rdParser) skipWS() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *rdParser) parseError(msg string) error {
	return NewParseError(msg, p.input, p.pos)
}
