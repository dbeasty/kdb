package sql

import kdberr "github.com/limidus/kdb/go/kdb/error"

// ParseError is returned when SQL text cannot be parsed.
type ParseError struct {
	*kdberr.DecodeError
	SQL string
	Pos int
}

func NewParseError(msg, sql string, pos int) *ParseError {
	return &ParseError{
		DecodeError: kdberr.NewDecodeError(msg, pos, nil),
		SQL:         sql,
		Pos:         pos,
	}
}

// PlanningError is returned when a statement cannot be planned or executed.
type PlanningError struct {
	*kdberr.SchemaError
	SQL string
}

func NewPlanningError(msg, sql string) *PlanningError {
	return &PlanningError{SchemaError: kdberr.NewSchemaError(msg, nil), SQL: sql}
}
