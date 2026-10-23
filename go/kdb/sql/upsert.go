package sql

import (
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// An upsert - INSERT ... ON CONFLICT (columns) DO UPDATE SET ... | DO NOTHING [RETURNING ...] -
// is decided by the server under its write gate, where the holder of a unique value is known
// exactly (KdbServerRuntime.ExecImmediateInsert). This file is its SQL half: building the row,
// checking the statement, and the UPDATE that runs against the holder.
//
// Cassandra's INSERT and UPDATE are both upserts by primary key, and its INSERT ... IF NOT EXISTS
// makes "insert unless present" atomic. KDB documents are keyed by kdb_id, while an application's
// natural key is a declared unique constraint, so the conflict target names that constraint, as
// PostgreSQL's ON CONFLICT names a unique index; the write gate is what makes it atomic.

// ImmediateInserter is implemented by every engine NewEngine builds: the SQL half of an
// immediate INSERT (InsertStatement.Immediate).
type ImmediateInserter interface {
	// PrepareImmediateInsert: see DMLExecutor.PrepareImmediateInsert.
	PrepareImmediateInsert(ins InsertStatement, ctx QueryContext) (InsertRow, error)
	// InsertReturning: see DMLExecutor.InsertReturning.
	InsertReturning(ins InsertStatement, ctx QueryContext, row InsertRow) DMLResult
}

// InsertRow is an immediate INSERT's one row, built before the gate: the document it would
// write, the operations that write it, and the conflict target's field tuple as declared (nil
// without ON CONFLICT).
type InsertRow struct {
	ID         codec.UUID
	JSON       string
	Operations []document.Op
	Conflict   []string
}

// PrepareImmediateInsert checks an immediate INSERT and builds its row. It takes exactly one
// VALUES row and no attachments, and its ON CONFLICT columns must be those of a unique constraint
// the schema declares, in any order.
func (d *DMLExecutor) PrepareImmediateInsert(ins InsertStatement, ctx QueryContext) (InsertRow, error) {
	if len(ins.Rows) != 1 {
		return InsertRow{}, NewPlanningError("INSERT with ON CONFLICT or RETURNING takes exactly one VALUES row", "")
	}
	if len(ins.Attachments) > 0 {
		return InsertRow{}, NewPlanningError("INSERT with ON CONFLICT or RETURNING cannot add attachments", "")
	}
	var row InsertRow
	if oc := ins.OnConflict; oc != nil {
		tuple, ok := conflictTuple(oc.Columns, ins.Table, ctx.Schema)
		if !ok {
			return InsertRow{}, NewPlanningError("ON CONFLICT columns must be those of a unique constraint", "")
		}
		row.Conflict = tuple
		if oc.Update != nil {
			upd := ConflictUpdate(ins)
			if err := checkUpdate(upd, ctx.Schema); err != nil {
				return InsertRow{}, err
			}
		}
	}
	if len(ins.Returning) > 0 {
		q := SelectQuery{Projections: ins.Returning, From: ins.Table}
		if isGroupedQuery(q) {
			return InsertRow{}, NewPlanningError("RETURNING of an INSERT cannot use aggregates", "")
		}
		if err := validateSelect(q, ctx.Schema); err != nil {
			return InsertRow{}, err
		}
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, ins.Table)
	env.attach = ctx.attach
	id, jsonText, err := insertRow(ins, ins.Rows[0], env, ctx.Schema, true)
	if err != nil {
		return InsertRow{}, err
	}
	row.ID, row.JSON = id, jsonText
	row.Operations = []document.Op{document.WriteOp{DocID: id, Patch: jsonText, Replace: true}}
	return row, nil
}

// conflictTuple returns the declared unique constraint whose fields are exactly columns.
func conflictTuple(columns []string, table TableRef, sch schema.KdbSchema) ([]string, bool) {
	want := make([]string, len(columns))
	for i, c := range columns {
		want[i] = stripTableAlias(c, table)
	}
	slices.Sort(want)
	for _, tuple := range sch.UniqueTuples() {
		have := slices.Clone(tuple)
		slices.Sort(have)
		if slices.Equal(have, want) {
			return tuple, true
		}
	}
	return nil, false
}

// ConflictUpdate is the UPDATE an INSERT's ON CONFLICT DO UPDATE runs against the document
// holding the conflicting value: its SET, REMOVE and WHERE, the INSERT's RETURNING, and at most
// that one document.
func ConflictUpdate(ins InsertStatement) UpdateStatement {
	upd := *ins.OnConflict.Update
	upd.Table = ins.Table
	upd.Returning = ins.Returning
	one := 1
	upd.Limit = &one
	return upd
}

// InsertReturning is the result of an immediate INSERT that inserted its row: one affected row,
// its id generated, and the RETURNING projection of the document as written.
func (d *DMLExecutor) InsertReturning(ins InsertStatement, ctx QueryContext, row InsertRow) DMLResult {
	res := DMLResult{Operations: row.Operations, RowsAffected: 1, GeneratedIDs: []string{row.ID.String()}}
	if len(ins.Returning) > 0 {
		env := newEvalEnv(ctx.Schema, ctx.Parameters, ins.Table)
		env.attach = ctx.attach
		res.Columns = columnsFor(SelectQuery{Projections: ins.Returning, From: ins.Table}, ctx.Schema)
		doc := newEvalDoc(document.Document{ID: row.ID, JSON: row.JSON})
		res.Rows = []QueryRow{{Values: projectRow(ins.Returning, env, doc)}}
	}
	return res
}

func (e *defaultEngine) PrepareImmediateInsert(ins InsertStatement, ctx QueryContext) (InsertRow, error) {
	if ctx.attach == nil {
		ctx.attach = e.executor.newAttachEnv(ctx)
	}
	row, err := e.dml.PrepareImmediateInsert(ins, ctx)
	if err == nil {
		err = ctx.attach.err
	}
	return row, err
}

func (e *defaultEngine) InsertReturning(ins InsertStatement, ctx QueryContext, row InsertRow) DMLResult {
	if ctx.attach == nil {
		ctx.attach = e.executor.newAttachEnv(ctx)
	}
	return e.dml.InsertReturning(ins, ctx, row)
}
