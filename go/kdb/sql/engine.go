package sql

import (
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Engine executes SQL against a namespace DAG and storage.
type Engine interface {
	Execute(sql string, ctx QueryContext) (QueryResult, error)
	ExecuteDML(sql string, ctx QueryContext) (DMLResult, error)
}

type defaultEngine struct {
	parser    Parser
	planner   Planner
	executor  *Executor
	dml       *DMLExecutor
	ddl       DDLExecutor
}

// NewEngine wires parser, planner, and executors.
func NewEngine(store storage.Adapter, d *dag.InMemoryCommitDag) Engine {
	exec := &Executor{Storage: store, DAG: d}
	return &defaultEngine{
		parser:   DefaultParser{},
		planner:  DefaultPlanner{},
		executor: exec,
		dml:      &DMLExecutor{Executor: exec},
	}
}

func (e *defaultEngine) Execute(sqlText string, ctx QueryContext) (QueryResult, error) {
	stmt, err := e.parser.Parse(sqlText)
	if err != nil {
		return QueryResult{}, err
	}
	switch s := stmt.(type) {
	case StmtSelect:
		plan, residual := e.planner.PlanSelect(s.Query, ctx.Schema)
		return e.executor.ExecuteSelect(s.Query, plan, residual, ctx)
	case StmtCreateTable:
		sch, err := e.ddl.ExecuteCreateTable(s.DDL, ctx)
		if err != nil {
			return QueryResult{}, err
		}
		return QueryResult{AppliedSchema: &sch}, nil
	case StmtInsert:
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
	ins, ok := stmt.(StmtInsert)
	if !ok {
		return DMLResult{}, NewPlanningError("not an INSERT statement", sqlText)
	}
	ops, err := e.dml.ExecuteInsert(ins.Insert, ctx)
	if err != nil {
		return DMLResult{}, err
	}
	ids := make([]string, len(ops))
	for i, op := range ops {
		if w, ok := op.(document.WriteOp); ok {
			ids[i] = w.DocID.String()
		}
	}
	return DMLResult{Operations: ops, RowsAffected: len(ops), GeneratedIDs: ids}, nil
}
