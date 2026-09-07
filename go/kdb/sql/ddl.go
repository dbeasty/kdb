package sql

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
)

// DDLExecutor applies CREATE TABLE DDL to namespace schema.
type DDLExecutor struct{}

// ExecuteCreateTable builds a new schema from column definitions, including single-column
// UNIQUE flags and table-level UNIQUE (a, b) constraints (kdb-spec-layer16 §9.6).
func (DDLExecutor) ExecuteCreateTable(ddl CreateTableStatement, ctx QueryContext) (schema.KdbSchema, error) {
	if !ctx.Schema.IsNone() {
		return schema.KdbSchema{}, NewPlanningError("CREATE TABLE: schema already exists for namespace", ddl.Table.Name)
	}
	fields := make([]schema.Field, len(ddl.Columns))
	for i, col := range ddl.Columns {
		fields[i] = schema.Field{
			Name: col.Name, Type: col.Type, Required: col.Required, Indexed: col.Indexed || col.Unique, Unique: col.Unique,
		}
	}
	constraints := make([]schema.UniqueConstraint, 0, len(ddl.UniqueConstraints))
	for _, c := range ddl.UniqueConstraints {
		constraints = append(constraints, schema.UniqueConstraint{Fields: append([]string(nil), c...)})
	}
	sch, err := schema.BuildWithConstraints(fields, constraints, 1, codec.TimestampNow(), "")
	if err != nil {
		return schema.KdbSchema{}, NewPlanningError("CREATE TABLE: "+err.Error(), ddl.Table.Name)
	}
	return sch, nil
}
