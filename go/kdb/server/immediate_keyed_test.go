package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/sql"
)

func execImmediate(t *testing.T, rt *KdbServerRuntime, q string, params ...sql.Parameter) ImmediateUpdateResult {
	t.Helper()
	out, err := rt.ExecImmediateUpdate(parseImmediate(t, q), sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Schema(), Parameters: params}, auth.Principal{}, "", nil)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return out
}

// An UPDATE whose WHERE names its documents by kdb_id is decided among those alone: the one
// named, those of an IN list, none for an id no document has, and the rest of the WHERE still
// applies to them.
func TestImmediateKeyedUpdateTouchesOnlyWhatItNames(t *testing.T) {
	for name, rt := range immediateRuntimes(t) {
		t.Run(name, func(t *testing.T) {
			for n := 0; n < 10; n++ {
				putJob(t, rt, n, "pending", 0, n)
			}
			one := execImmediate(t, rt, `UPDATE docs SET status = 'done' WHERE _c = 'jobs' AND (kdb_id = ?) LIMIT 1 RETURNING kdb_id`,
				sql.ParamString{Value: immediateJobID(3).String()})
			if ids := returnedIDs(one.DML); len(ids) != 1 || ids[0] != immediateJobID(3).String() || !one.Narrowed {
				t.Fatalf("by kdb_id: %v (narrowed %v)", ids, one.Narrowed)
			}
			in := execImmediate(t, rt, fmt.Sprintf(`UPDATE docs SET attempts = 5 WHERE kdb_id IN ('%s', '%s', '00000000-0000-4000-9000-999999999999') RETURNING kdb_id`,
				immediateJobID(1), immediateJobID(2)))
			if ids := returnedIDs(in.DML); len(ids) != 2 {
				t.Fatalf("by IN: %v", ids)
			}
			missing := execImmediate(t, rt, `UPDATE docs SET status = 'done' WHERE kdb_id = '00000000-0000-4000-9000-999999999999' LIMIT 1 RETURNING kdb_id`)
			if missing.DML.RowsAffected != 0 || missing.Committed {
				t.Fatalf("an id no document has: %+v", missing.DML)
			}
			// The rest of the WHERE: job 3 is no longer pending.
			stale := execImmediate(t, rt, fmt.Sprintf(`UPDATE docs SET status = 'x' WHERE kdb_id = '%s' AND status = 'pending' LIMIT 1 RETURNING kdb_id`, immediateJobID(3)))
			if stale.DML.RowsAffected != 0 {
				t.Fatal("a keyed UPDATE ignored the rest of its WHERE")
			}
			done := execImmediate(t, rt, `UPDATE docs SET touched = true WHERE _c = 'jobs' AND status = 'done' RETURNING kdb_id`)
			if ids := returnedIDs(done.DML); len(ids) != 1 {
				t.Fatalf("documents done: %v, want only job 3", ids)
			}
		})
	}
}

// Claims and keyed completions running together: every job is claimed exactly once per round
// and every completion lands, with no lost or doubled update.
func TestImmediateClaimAndKeyedCompleteUnderConcurrency(t *testing.T) {
	for name, rt := range immediateRuntimes(t) {
		t.Run(name, func(t *testing.T) {
			const jobs, workers, rounds = 20, 8, 30
			for n := 0; n < jobs; n++ {
				putJob(t, rt, n, "pending", 0, n)
			}
			claim := `UPDATE docs SET status = 'processing', attempts = attempts + 1 WHERE _c = 'jobs' AND status = 'pending' ORDER BY createdAt LIMIT 1 RETURNING kdb_id`
			var mu sync.Mutex
			claimed := map[string]int{}
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						out := execImmediate(t, rt, claim)
						ids := returnedIDs(out.DML)
						if len(ids) == 0 {
							continue
						}
						mu.Lock()
						claimed[ids[0]]++
						mu.Unlock()
						execImmediate(t, rt, `UPDATE docs SET status = 'pending' WHERE _c = 'jobs' AND kdb_id = ? AND status = 'processing' LIMIT 1 RETURNING kdb_id`,
							sql.ParamString{Value: ids[0]})
					}
				}()
			}
			wg.Wait()
			total := 0
			for _, n := range claimed {
				total += n
			}
			sum := execImmediate(t, rt, `UPDATE docs SET checked = true WHERE _c = 'jobs' AND status = 'pending' RETURNING attempts`)
			if len(sum.DML.Rows) != jobs {
				t.Fatalf("%d jobs pending after every completion, want %d", len(sum.DML.Rows), jobs)
			}
			attempts := 0
			for _, row := range sum.DML.Rows {
				attempts += int(row.Values[0].(sql.CellLong).Value)
			}
			if attempts != total {
				t.Fatalf("attempts add up to %d, but %d claims were returned", attempts, total)
			}
		})
	}
}
