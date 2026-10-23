package server

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// An immediate INSERT (sql.InsertStatement.Immediate: one with ON CONFLICT or RETURNING) is an
// upsert - MongoDB's updateOne(filter, update, {upsert: true}) on a unique key:
//
//	INSERT INTO docs (kdb_id, _c, jobId, status, attempts) VALUES (?, 'jobs', ?, 'pending', 0)
//	  ON CONFLICT (_c, jobId) DO UPDATE SET status = 'pending' REMOVE lastError
//	  RETURNING kdb_id, _doc
//
// Like an immediate UPDATE it is not buffered on the session: it is decided and committed in one
// step at the front of the write gate. There the unique-key registry - which every commit updates
// before the gate moves on - names exactly the document holding the row's values of the conflict
// constraint, if any. None: the row is inserted. One: the row is dropped and that document is
// updated (DO UPDATE, the same code an immediate UPDATE runs among its candidates) or left alone
// (DO NOTHING). Two writers upserting the same key therefore never both insert, and neither fails
// with a unique violation: the second finds the first's document and updates it.
//
// The commit's own unique check still runs: a row that would collide on some other constraint
// than the conflict target is refused, as in PostgreSQL.

// ImmediateInsertResult is what an immediate INSERT did.
type ImmediateInsertResult struct {
	// DML holds the RETURNING columns and rows, the number of documents written (0 when DO
	// NOTHING found a holder, or DO UPDATE's WHERE did not match it), and on an insert the
	// inserted document's id in GeneratedIDs.
	DML sql.DMLResult
	// Inserted reports that the row was inserted rather than a holder updated.
	Inserted bool
	// Commit is the commit made; Committed is false when nothing was written.
	Commit    document.Commit
	Committed bool
	// Head is Commit's hash when it committed, else the live head the statement was decided at.
	Head codec.Hash
}

// ExecImmediateInsert runs an immediate INSERT on this runtime's namespace as one auto-committed
// statement (see the comment above). qctx and check are as for ExecImmediateUpdate.
func (s *KdbServerRuntime) ExecImmediateInsert(ins sql.InsertStatement, qctx sql.QueryContext, principal auth.Principal, sessionID string, check func(sql.DMLResult) error) (ImmediateInsertResult, error) {
	ii, ok := s.SQLEngine().(sql.ImmediateInserter)
	iu, ok2 := s.SQLEngine().(sql.ImmediateUpdater)
	if !ok || !ok2 {
		return ImmediateInsertResult{}, sql.NewPlanningError("INSERT with ON CONFLICT or RETURNING is not supported by this engine", "")
	}
	// The row is built before the gate: it reads nothing.
	row, err := ii.PrepareImmediateInsert(ins, qctx)
	if err != nil {
		return ImmediateInsertResult{}, err
	}
	if row.Conflict != nil {
		if s.UniqueKeys == nil {
			return ImmediateInsertResult{}, sql.NewPlanningError("ON CONFLICT needs unique constraint enforcement, which this runtime does not have", "")
		}
		if err := s.UniqueKeyRebuildError; err != nil {
			// The registry may not know every holder, so "no holder" would not be exact.
			return ImmediateInsertResult{}, fmt.Errorf("ON CONFLICT is unavailable until the namespace's unique constraints are consistent: %w", err)
		}
	}
	at := func(h codec.Hash) sql.QueryContext {
		c := qctx
		c.AtCommit = &h
		return c
	}
	var out ImmediateInsertResult
	commit, committed, err := s.CommitImmediate(principal, sessionID, func(live codec.Hash) ([]document.Op, error) {
		out.Head = live
		exists, err := s.documentExistsAt(live, row.ID)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, sql.NewPlanningError("kdb_id "+row.ID.String()+" already exists", "")
		}
		holder, found, err := s.conflictHolder(row)
		if err != nil {
			return nil, err
		}
		var res sql.DMLResult
		switch {
		case !found:
			res, out.Inserted = ii.InsertReturning(ins, at(live), row), true
		case ins.OnConflict.Update == nil:
			res = ii.InsertReturning(ins, at(live), row)
			res.Operations, res.RowsAffected, res.GeneratedIDs = nil, 0, nil
			if res.Rows != nil {
				res.Rows = res.Rows[:0]
			}
		default:
			if res, err = iu.ExecuteUpdateAmong(sql.ConflictUpdate(ins), at(live), []codec.UUID{holder}); err != nil {
				return nil, err
			}
		}
		if check != nil {
			if err := check(res); err != nil {
				return nil, err
			}
		}
		out.DML = res
		return res.Operations, nil
	})
	if err != nil {
		return ImmediateInsertResult{}, err
	}
	out.Commit, out.Committed = commit, committed
	if committed {
		out.Head = commit.Hash
	}
	return out, nil
}

// conflictHolder is the document holding row's values of its conflict constraint, by the
// unique-key registry. Called under the write gate, where the registry is exact. A row whose
// tuple has an absent or null part claims nothing, so nothing holds it.
func (s *KdbServerRuntime) conflictHolder(row sql.InsertRow) (codec.UUID, bool, error) {
	if row.Conflict == nil {
		return codec.UUID{}, false, nil
	}
	ns := s.Runtime.DefaultNamespace
	keys, err := transaction.UniqueKeysFor(ns, s.Schema(), document.Document{ID: row.ID, JSON: row.JSON})
	if err != nil {
		return codec.UUID{}, false, err
	}
	fields := strings.Join(row.Conflict, transaction.FieldSeparator)
	var holder codec.UUID
	found := false
	for _, key := range keys {
		if key.Fields != fields {
			continue
		}
		owner, ok := s.UniqueKeys.Owner(key)
		if !ok {
			continue
		}
		if found && owner != holder {
			// An array part claims each element (multikey); elements held by two documents
			// name two rows, and an upsert updates at most one.
			return codec.UUID{}, false, sql.NewPlanningError("ON CONFLICT row matches more than one document", "")
		}
		holder, found = owner, true
	}
	return holder, found, nil
}

// documentExistsAt reports whether id is a document in commit h's tree.
func (s *KdbServerRuntime) documentExistsAt(h codec.Hash, id codec.UUID) (bool, error) {
	c, ok := s.Runtime.DAG.GetCommit(h)
	if !ok {
		return false, nil
	}
	doc, err := s.Runtime.Storage.GetDocument(s.Runtime.DefaultNamespace, id, c.DocumentTreeHash)
	if err != nil {
		return false, err
	}
	return doc != nil, nil
}
