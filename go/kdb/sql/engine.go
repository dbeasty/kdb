package sql

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Engine executes SQL against a namespace DAG and storage.
type Engine interface {
	Execute(sql string, ctx QueryContext) (QueryResult, error)
	ExecuteDML(sql string, ctx QueryContext) (DMLResult, error)
}

// IndexDDLExecutor applies CREATE INDEX / DROP INDEX. The runtime's index catalog implements
// it; an IndexProvider passed to NewEngineWithIndexes that also implements this interface is
// used for index DDL automatically.
type IndexDDLExecutor interface {
	CreateIndex(stmt StmtCreateIndex, ctx QueryContext) error
	DropIndex(stmt StmtDropIndex, ctx QueryContext) error
}

// ImmediateUpdater is implemented by every engine NewEngine builds. It is the two halves of an
// immediate UPDATE (UpdateStatement.Immediate) that the server runs around its write gate: find
// candidates at a read head before the gate, then decide and build the writes among them at the
// live head inside it.
type ImmediateUpdater interface {
	// UpdateCandidates: see DMLExecutor.UpdateCandidates.
	UpdateCandidates(upd UpdateStatement, ctx QueryContext, k int) (ids []codec.UUID, complete bool, err error)
	// ExecuteUpdateAmong: see DMLExecutor.ExecuteUpdateAmong.
	ExecuteUpdateAmong(upd UpdateStatement, ctx QueryContext, among []codec.UUID) (DMLResult, error)
}

func (e *defaultEngine) UpdateCandidates(upd UpdateStatement, ctx QueryContext, k int) ([]codec.UUID, bool, error) {
	if ctx.attach == nil {
		ctx.attach = e.executor.newAttachEnv(ctx)
	}
	ids, complete, err := e.dml.UpdateCandidates(upd, ctx, k)
	if err == nil {
		err = ctx.attach.err
	}
	return ids, complete, err
}

func (e *defaultEngine) ExecuteUpdateAmong(upd UpdateStatement, ctx QueryContext, among []codec.UUID) (DMLResult, error) {
	if ctx.attach == nil {
		ctx.attach = e.executor.newAttachEnv(ctx)
	}
	res, err := e.dml.ExecuteUpdateAmong(upd, ctx, among)
	if err == nil {
		err = ctx.attach.err
	}
	if err != nil {
		return DMLResult{}, err
	}
	return res, nil
}

type defaultEngine struct {
	parser   Parser
	planner  Planner
	executor *Executor
	dml      *DMLExecutor
	ddl      DDLExecutor
	indexDDL IndexDDLExecutor
}

// NewEngine wires parser, planner, and executors without any index support.
func NewEngine(store storage.Adapter, d *dag.InMemoryCommitDag) Engine {
	return NewEngineWithIndexes(store, d, nil)
}

// NewEngineWithIndexes is NewEngine with an IndexProvider serving index-backed access paths
// and the search functions (kdb-spec-layer16 §9). A nil provider behaves like NewEngine.
func NewEngineWithIndexes(store storage.Adapter, d *dag.InMemoryCommitDag, provider IndexProvider) Engine {
	exec := &Executor{Storage: store, DAG: d, IndexProvider: provider}
	eng := &defaultEngine{
		parser:   DefaultParser{},
		planner:  DefaultPlanner{},
		executor: exec,
		dml:      &DMLExecutor{Executor: exec},
	}
	if ddl, ok := provider.(IndexDDLExecutor); ok && provider != nil {
		eng.indexDDL = ddl
	}
	return eng
}

func (e *defaultEngine) Execute(sqlText string, ctx QueryContext) (QueryResult, error) {
	stmt, err := e.parser.Parse(sqlText)
	if err != nil {
		return QueryResult{}, err
	}
	switch s := stmt.(type) {
	case StmtSelect:
		plan, residual, err := e.planner.PlanSelect(s.Query, ctx.Schema)
		if err != nil {
			return QueryResult{}, err
		}
		if !s.Explain {
			return e.executor.ExecuteSelect(s.Query, plan, residual, ctx)
		}
		if ctx.Stats == nil {
			ctx.Stats = &ExecStats{}
		}
		res, err := e.executor.ExecuteSelect(s.Query, plan, residual, ctx)
		if err != nil {
			return QueryResult{}, err
		}
		return explainResult(res, ctx.Stats), nil
	case StmtCreateTable:
		sch, err := e.ddl.ExecuteCreateTable(s.DDL, ctx)
		if err != nil {
			return QueryResult{}, err
		}
		return QueryResult{AppliedSchema: &sch}, nil
	case StmtCreateIndex:
		if e.indexDDL == nil {
			return QueryResult{}, NewPlanningError("CREATE INDEX requires an index catalog on this engine", sqlText)
		}
		if err := e.indexDDL.CreateIndex(s, ctx); err != nil {
			return QueryResult{}, err
		}
		return QueryResult{}, nil
	case StmtDropIndex:
		if e.indexDDL == nil {
			return QueryResult{}, NewPlanningError("DROP INDEX requires an index catalog on this engine", sqlText)
		}
		if err := e.indexDDL.DropIndex(s, ctx); err != nil {
			return QueryResult{}, err
		}
		return QueryResult{}, nil
	case StmtInsert, StmtUpdate, StmtDelete:
		return QueryResult{}, NewPlanningError("DML must be executed via ExecuteDML", sqlText)
	default:
		return QueryResult{}, NewPlanningError("unsupported statement", sqlText)
	}
}

func (e *defaultEngine) ExecuteDML(sqlText string, ctx QueryContext) (DMLResult, error) {
	stmt, err := e.parser.Parse(sqlText)
	if err != nil {
		return DMLResult{}, err
	}
	if ctx.attach == nil {
		ctx.attach = e.executor.newAttachEnv(ctx)
	}
	var ops []document.Op
	switch s := stmt.(type) {
	case StmtInsert:
		ops, err = e.dml.ExecuteInsert(s.Insert, ctx)
		if err != nil {
			return DMLResult{}, err
		}
		var ids []string
		for _, op := range ops {
			if w, ok := op.(document.WriteOp); ok {
				ids = append(ids, w.DocID.String())
			}
		}
		if ctx.attach.err != nil {
			return DMLResult{}, ctx.attach.err
		}
		return DMLResult{Operations: ops, RowsAffected: len(ids), GeneratedIDs: ids}, nil
	case StmtUpdate:
		res, err := e.dml.ExecuteUpdateAmong(s.Update, ctx, nil)
		if err == nil {
			err = ctx.attach.err
		}
		if err != nil {
			return DMLResult{}, err
		}
		return res, nil
	case StmtDelete:
		ops, err = e.dml.ExecuteDelete(s.Delete, ctx)
	default:
		return DMLResult{}, NewPlanningError("not a DML statement", sqlText)
	}
	if err == nil {
		err = ctx.attach.err
	}
	if err != nil {
		return DMLResult{}, err
	}
	return DMLResult{Operations: ops, RowsAffected: rowsAffected(ops)}, nil
}

// explainResult is EXPLAIN's answer: one row naming the access path the query took (the Plan
// label - "fullscan", "index:eq(status)", "index:eq(status)+order(createdAt)" for an ordered BTREE
// walk, "...+topk(order(updatedAt) abandoned)" for one given up for the bounded top-k) and what
// it cost: rows returned, rows examined, documents read.
func explainResult(res QueryResult, st *ExecStats) QueryResult {
	plan := res.Plan
	if plan == "" {
		plan = "unknown"
	}
	return QueryResult{
		Columns: []ResultColumn{
			{Name: "plan", SQLType: "VARCHAR", Source: ColumnSourceExpression},
			{Name: "rows", SQLType: "BIGINT", Source: ColumnSourceExpression},
			{Name: "rows_examined", SQLType: "BIGINT", Source: ColumnSourceExpression},
			{Name: "docs_read", SQLType: "BIGINT", Source: ColumnSourceExpression},
		},
		Rows: []QueryRow{{Values: []Cell{
			CellString{Value: plan},
			CellLong{Value: int64(len(res.Rows))},
			CellLong{Value: int64(st.RowsExamined)},
			CellLong{Value: int64(st.DocsRead)},
		}}},
		Plan: res.Plan,
	}
}
