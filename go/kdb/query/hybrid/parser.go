package hybrid

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/sql"
)

// SQLParser parses hybrid SQL with optional AT VERSION/COMMIT/TIME clauses.
type SQLParser interface {
	ParseWithVersion(sql string) (ParsedStatement, error)
	Parse(sql string) (sql.Statement, error)
}

// DefaultSQLParser strips version clauses then delegates to the base SQL parser.
type DefaultSQLParser struct {
	Base sql.Parser
}

// NewDefaultSQLParser returns a hybrid parser.
func NewDefaultSQLParser(base sql.Parser) *DefaultSQLParser {
	if base == nil {
		base = sql.DefaultParser{}
	}
	return &DefaultSQLParser{Base: base}
}

func (p *DefaultSQLParser) ParseWithVersion(sqlStr string) (ParsedStatement, error) {
	stripped, version := StripVersionClause(strings.TrimSpace(sqlStr))
	if _, err := p.Base.Parse(stripped); err != nil {
		return ParsedStatement{}, err
	}
	return ParsedStatement{SQL: stripped, Version: version}, nil
}

func (p *DefaultSQLParser) Parse(sqlStr string) (sql.Statement, error) {
	return p.Base.Parse(sqlStr)
}

// StripVersionClause removes AT VERSION/COMMIT/TIME from the tail of SQL.
func StripVersionClause(sqlStr string) (string, VersionClause) {
	type kw struct {
		phrase  string
		factory func(string) VersionClause
	}
	keywords := []kw{
		{"AT VERSION", func(lit string) VersionClause { return AtTag{Tag: lit} }},
		{"AT COMMIT", func(lit string) VersionClause { return AtCommit{Hex: lit} }},
		{"AT TIME", func(lit string) VersionClause { return AtTime{ISO8601: lit} }},
	}
	upper := strings.ToUpper(sqlStr)
	for _, k := range keywords {
		idx := strings.LastIndex(upper, k.phrase)
		if idx < 0 {
			continue
		}
		tail := strings.TrimSpace(sqlStr[idx+len(k.phrase):])
		lit, ok := readQuotedLiteral(tail)
		if !ok {
			continue
		}
		stripped := strings.TrimSpace(sqlStr[:idx])
		return stripped, k.factory(lit)
	}
	return sqlStr, nil
}

func readQuotedLiteral(tail string) (string, bool) {
	if tail == "" || tail[0] != '\'' {
		return "", false
	}
	end := strings.IndexByte(tail[1:], '\'')
	if end < 0 {
		return "", false
	}
	return tail[1 : 1+end], true
}
