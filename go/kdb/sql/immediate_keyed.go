package sql

import "github.com/limidus/kdb/go/kdb/codec"

// KeyedUpdater is implemented by every engine NewEngine builds: the documents an immediate UPDATE
// can only ever touch, when its WHERE names them by key.
type KeyedUpdater interface {
	// UpdateKeyedTargets: see DMLExecutor.UpdateKeyedTargets.
	UpdateKeyedTargets(upd UpdateStatement, ctx QueryContext) (ids []codec.UUID, ok bool)
}

// UpdateKeyedTargets returns the ids an UPDATE's WHERE names by key - a top-level conjunct
// `kdb_id = <const>` or `kdb_id IN (<const>, ...)` - and ok=true when it has one. No other
// document can match, so the server decides such an UPDATE among these alone at the live head,
// with no candidate search before its write gate and no look at what changed since: a queue's
// "complete this job" (`UPDATE ... WHERE kdb_id = ? LIMIT 1`) used to re-check every document
// the commits queued ahead of it had written. ids is never nil when ok, and may name documents
// that do not exist, which match nothing.
func (d *DMLExecutor) UpdateKeyedTargets(upd UpdateStatement, ctx QueryContext) ([]codec.UUID, bool) {
	env := newEvalEnv(ctx.Schema, ctx.Parameters, upd.Table)
	for _, conj := range conjuncts(upd.Where) {
		if ids, ok := keyLookup(conj, env); ok {
			if ids == nil {
				ids = []codec.UUID{}
			}
			return ids, true
		}
	}
	return nil, false
}

func (e *defaultEngine) UpdateKeyedTargets(upd UpdateStatement, ctx QueryContext) ([]codec.UUID, bool) {
	return e.dml.UpdateKeyedTargets(upd, ctx)
}
