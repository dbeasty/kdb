package server

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

const immediateClaimSQL = `UPDATE docs SET status = 'processing', attempts = attempts + 1
	WHERE _c = 'jobs' AND status = 'pending' AND attempts < 3
	ORDER BY createdAt LIMIT 1 RETURNING kdb_id`

func immediateJobID(n int) codec.UUID {
	id, err := codec.ParseUUID(fmt.Sprintf("00000000-0000-4000-9000-%012d", n))
	if err != nil {
		panic(err)
	}
	return id
}

func immediateRuntimes(t *testing.T) map[string]*KdbServerRuntime {
	t.Helper()
	mem, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	file, err := embed.OpenFileRuntime(t.TempDir(), "app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(file.Close)
	return map[string]*KdbServerRuntime{"memory": NewKdbServerRuntime(mem), "file": NewKdbServerRuntime(file)}
}

func putJob(t *testing.T, rt *KdbServerRuntime, n int, status string, attempts, created int) {
	t.Helper()
	body := fmt.Sprintf(`{"_c":"jobs","status":%q,"attempts":%d,"createdAt":"t%06d"}`, status, attempts, created)
	if _, err := rt.PutJSON(rt.Runtime.DefaultNamespace, immediateJobID(n), body, nil, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func parseImmediate(t *testing.T, q string) sql.UpdateStatement {
	t.Helper()
	stmt, err := sql.DefaultParser{}.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := sql.ImmediateUpdate(stmt)
	if !ok {
		t.Fatalf("not immediate: %s", q)
	}
	return upd
}

func returnedIDs(res sql.DMLResult) []string {
	var out []string
	for _, r := range res.Rows {
		out = append(out, r.Values[0].(sql.CellString).Value)
	}
	return out
}

// The narrowed decision (candidates found at an older head, plus whatever changed since) must be
// exactly the full evaluation at the live head, whatever happened in between: claims, requeues
// with an older createdAt, new jobs, deletions.
func TestImmediateUpdateNarrowingMatchesFullEvaluation(t *testing.T) {
	for name, rt := range immediateRuntimes(t) {
		t.Run(name, func(t *testing.T) {
			ns := rt.Runtime.DefaultNamespace
			r := rand.New(rand.NewSource(7))
			next := 0
			for ; next < 60; next++ {
				putJob(t, rt, next, "pending", r.Intn(3), 1000+next*10)
			}
			iu := rt.SQLEngine().(sql.ImmediateUpdater)
			for _, limit := range []int{1, 3} {
				upd := parseImmediate(t, strings.Replace(immediateClaimSQL, "LIMIT 1", fmt.Sprintf("LIMIT %d", limit), 1))
				narrowed := 0
				for round := 0; round < 40; round++ {
					readHead, err := rt.Runtime.DAG.Head()
					if err != nil {
						t.Fatal(err)
					}
					k := limit + r.Intn(4)
					ids, complete, err := iu.UpdateCandidates(upd, sql.QueryContext{NamespaceID: ns, Schema: rt.Schema(), AtCommit: &readHead}, k)
					if err != nil {
						t.Fatal(err)
					}
					// Interleaved writes between the candidate read and the gate.
					for i := r.Intn(4); i > 0; i-- {
						switch r.Intn(4) {
						case 0: // another claim takes one of the candidates
							if len(ids) > 0 {
								n := ids[r.Intn(len(ids))]
								if _, err := rt.PutJSON(ns, n, `{"_c":"jobs","status":"processing","attempts":1,"createdAt":"t000000"}`, nil, auth.Principal{}); err != nil {
									t.Fatal(err)
								}
							}
						case 1: // a requeue that now ranks first
							putJob(t, rt, r.Intn(next), "pending", 0, r.Intn(1000))
						case 2: // a brand-new job
							putJob(t, rt, next, "pending", 0, r.Intn(3000))
							next++
						case 3: // an unrelated write
							if _, err := rt.PutJSON(ns, immediateJobID(100000+r.Intn(5)), `{"_c":"other"}`, nil, auth.Principal{}); err != nil {
								t.Fatal(err)
							}
						}
					}
					live, err := rt.Runtime.DAG.Head()
					if err != nil {
						t.Fatal(err)
					}
					ctx := sql.QueryContext{NamespaceID: ns, Schema: rt.Schema(), AtCommit: &live}
					full, err := iu.ExecuteUpdateAmong(upd, ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					among := rt.immediateAmong(readHead, live, ids, complete, limit)
					if among == nil {
						continue
					}
					narrowed++
					got, err := iu.ExecuteUpdateAmong(upd, ctx, among)
					if err != nil {
						t.Fatal(err)
					}
					if g, w := strings.Join(returnedIDs(got), ","), strings.Join(returnedIDs(full), ","); g != w {
						t.Fatalf("round %d limit %d: narrowed picked %s, full evaluation %s", round, limit, g, w)
					}
					// Apply it, so the queue moves.
					if _, err := rt.ExecImmediateUpdate(upd, sql.QueryContext{NamespaceID: ns, Schema: rt.Schema()}, auth.Principal{}, "s", nil); err != nil {
						t.Fatal(err)
					}
				}
				if narrowed < 20 {
					t.Fatalf("limit %d: only %d of 40 rounds were narrowed; the test proves little", limit, narrowed)
				}
			}
		})
	}
}

// Concurrent claims straight on the runtime: every job once, no errors, and the decision is
// narrowed (not a full evaluation under the gate) for most of them.
func TestImmediateUpdateConcurrentClaims(t *testing.T) {
	for name, rt := range immediateRuntimes(t) {
		t.Run(name, func(t *testing.T) {
			const jobs, workers = 120, 12
			for n := 0; n < jobs; n++ {
				putJob(t, rt, n, "pending", 0, n)
			}
			upd := parseImmediate(t, immediateClaimSQL)
			var (
				mu       sync.Mutex
				seen     = map[string]int{}
				narrowed int
				wg       sync.WaitGroup
				failures []error
			)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						out, err := rt.ExecImmediateUpdate(upd, sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Schema()}, auth.Principal{}, "s", nil)
						mu.Lock()
						if err != nil {
							failures = append(failures, err)
							mu.Unlock()
							return
						}
						if out.Narrowed {
							narrowed++
						}
						ids := returnedIDs(out.DML)
						for _, id := range ids {
							seen[id]++
						}
						mu.Unlock()
						if len(ids) == 0 {
							if out.Committed {
								t.Error("a claim that matched nothing committed")
							}
							return
						}
					}
				}()
			}
			wg.Wait()
			if len(failures) > 0 {
				t.Fatalf("%d claims failed: %v", len(failures), failures[0])
			}
			if len(seen) != jobs {
				t.Fatalf("%d jobs claimed, want %d", len(seen), jobs)
			}
			for id, n := range seen {
				if n != 1 {
					t.Fatalf("%s claimed %d times", id, n)
				}
			}
			if narrowed < jobs/2 {
				t.Fatalf("only %d of %d claims were narrowed", narrowed, jobs)
			}
		})
	}
}
