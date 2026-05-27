package sql

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
)

// DDLExecutor applies CREATE TABLE DDL to namespace schema.
type DDLExecutor struct{}

// ExecuteCreateTable builds a new schema from column definitions.
func (DDLExecutor) ExecuteCreateTable(ddl CreateTableStatement, ctx QueryContext) (schema.KdbSchema, error) {
	if !ctx.Schema.IsNone() {
		return schema.KdbSchema{}, NewPlanningError("CREATE TABLE: schema already exists for namespace", ddl.Table.Name)
	}
	fields := make([]schema.Field, len(ddl.Columns))
	for i, col := range ddl.Columns {
		fields[i] = schema.Field{
			Name: col.Name, Type: col.Type, Required: col.Required, Indexed: col.Indexed,
		}
	}
	return schema.Build(fields, 1, codec.TimestampNow(), "")
}
