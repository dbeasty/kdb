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

// IndexDDLExecutor applies CREATE INDEX / DROP INDEX. The runtime's index catalog implements
// it; an IndexProvider passed to NewEngineWithIndexes that also implements this interface is
// used for index DDL automatically.
type IndexDDLExecutor interface {
	CreateIndex(stmt StmtCreateIndex, ctx QueryContext) error
	DropIndex(stmt StmtDropIndex, ctx QueryContext) error
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
		return e.executor.ExecuteSelect(s.Query, plan, residual, ctx)
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
	var ops []document.Op
	switch s := stmt.(type) {
	case StmtInsert:
		ops, err = e.dml.ExecuteInsert(s.Insert, ctx)
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
	case StmtUpdate:
		ops, err = e.dml.ExecuteUpdate(s.Update, ctx)
	case StmtDelete:
		ops, err = e.dml.ExecuteDelete(s.Delete, ctx)
	default:
		return DMLResult{}, NewPlanningError("not a DML statement", sqlText)
	}
	if err != nil {
		return DMLResult{}, err
	}
	return DMLResult{Operations: ops, RowsAffected: len(ops)}, nil
}
