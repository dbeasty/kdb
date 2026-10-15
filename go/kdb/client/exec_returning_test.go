package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/client"
)

const claimSQL = `UPDATE docs SET status = 'processing', attempts = attempts + 1
	WHERE _c = 'jobs' AND status = 'pending' AND attempts < 3
	ORDER BY createdAt LIMIT 1 RETURNING kdb_id, _doc, attempts`

func jobID(n int) string { return fmt.Sprintf("00000000-0000-4000-9000-%012d", n) }

// seedJobs writes m pending jobs, job n created at second n.
func seedJobs(t *testing.T, c *client.Client, m int) {
	t.Helper()
	ctx := context.Background()
	var writes []client.DocWrite
	for n := 0; n < m; n++ {
		body := fmt.Sprintf(`{"_c":"jobs","status":"pending","attempts":0,"createdAt":"2026-01-01T00:%02d:%02dZ","n":%d}`, n/60, n%60, n)
		writes = append(writes, client.DocWrite{DocID: jobID(n), JSON: []byte(body), Replace: true})
	}
	base, err := c.ReplaceJSON(ctx, "app/data", jobID(999), []byte(`{"_c":"meta"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(ctx, client.Transaction{Namespace: "app/data", BaseVersion: base, Writes: writes}); err != nil {
		t.Fatal(err)
	}
}

func TestExecReturningClaimsInOrder(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()
	seedJobs(t, c, 3)
	// A done job older than all of them is not pending, so never claimed.
	if _, err := c.ReplaceJSON(ctx, "app/data", jobID(99), []byte(`{"_c":"jobs","status":"done","attempts":0,"createdAt":"2025-01-01T00:00:00Z"}`)); err != nil {
		t.Fatal(err)
	}
	for want := 0; want < 3; want++ {
		cols, rows, err := c.ExecReturning(ctx, "app/data", claimSQL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(cols) != 3 || cols[0] != "kdb_id" || cols[1] != "_doc" || cols[2] != "attempts" {
			t.Fatalf("columns %v", cols)
		}
		if len(rows) != 1 {
			t.Fatalf("claim %d: %d rows", want, len(rows))
		}
		if rows[0][0] != jobID(want) {
			t.Fatalf("claim %d took %v, want the oldest pending %s", want, rows[0][0], jobID(want))
		}
		var doc struct {
			Status   string
			Attempts int
			N        int
		}
		if err := json.Unmarshal(rows[0][1].(json.RawMessage), &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Status != "processing" || doc.Attempts != 1 || doc.N != want {
			t.Fatalf("returned document %+v: RETURNING is the document after the update", doc)
		}
		if rows[0][2] != int64(1) {
			t.Fatalf("attempts cell %#v (%T), want int64 1", rows[0][2], rows[0][2])
		}
		// The write is committed: a read sees it with no further call.
		body, _, err := c.GetJSON(ctx, "app/data", jobID(want))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &doc); err != nil || doc.Status != "processing" {
			t.Fatalf("stored %s", body)
		}
	}
	// Nothing left: no rows, no error.
	cols, rows, err := c.ExecReturning(ctx, "app/data", claimSQL, nil)
	if err != nil || len(rows) != 0 || len(cols) != 3 {
		t.Fatalf("empty queue: cols=%v rows=%v err=%v", cols, rows, err)
	}
	// Exec runs the same statement without sending a commit for it, and counts the rows.
	if _, err := c.ReplaceJSON(ctx, "app/data", jobID(50), []byte(`{"_c":"jobs","status":"pending","attempts":0,"createdAt":"2026-02-01T00:00:00Z"}`)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Exec(ctx, "app/data", claimSQL, nil); err != nil || n != 1 {
		t.Fatalf("Exec of a claim: n=%d err=%v", n, err)
	}
	// The client's cached head followed the claim's commit: a write after it does not conflict.
	if _, err := c.PutJSON(ctx, "app/data", jobID(51), []byte(`{"_c":"other"}`)); err != nil {
		t.Fatal(err)
	}
	// A plain UPDATE through ExecReturning is buffered and auto-committed as Exec would, with no rows.
	cols, rows, err = c.ExecReturning(ctx, "app/data", `UPDATE docs SET seen = 1 WHERE _c = 'other'`, nil)
	if err != nil || len(rows) != 0 || len(cols) != 0 {
		t.Fatalf("plain update: cols=%v rows=%v err=%v", cols, rows, err)
	}
	if _, _, err := c.GetJSON(ctx, "app/data", jobID(51)); err != nil {
		t.Fatal(err)
	}
	var seen []struct{ Seen int }
	if err := c.Query(ctx, "app/data", `SELECT seen FROM docs WHERE _c = 'other'`, nil, &seen); err != nil || len(seen) != 1 || seen[0].Seen != 1 {
		t.Fatalf("plain update not committed: %v %v", seen, err)
	}
}

// Every job is claimed exactly once however many workers race for the oldest, and none of them
// sees an error: the selection happens under the write gate, at the live head.
func TestExecReturningConcurrentClaimsAreExclusive(t *testing.T) {
	addr, _ := startTestServer(t)
	seeder := connectTestClient(t, addr)
	const workers, jobs = 16, 200
	seedJobs(t, seeder, jobs)

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		errs    []error
		wg      sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		c := connectTestClient(t, addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, rows, err := c.ExecReturning(context.Background(), "app/data", claimSQL, nil)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				for _, r := range rows {
					claimed[r[0].(string)]++
				}
				mu.Unlock()
				if len(rows) == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("%d claims failed, first: %v", len(errs), errs[0])
	}
	if len(claimed) != jobs {
		t.Fatalf("%d distinct jobs claimed, want %d", len(claimed), jobs)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
	var left []struct{ N int }
	if err := seeder.Query(context.Background(), "app/data", `SELECT n FROM docs WHERE _c = 'jobs' AND status = 'pending'`, nil, &left); err != nil || len(left) != 0 {
		t.Fatalf("pending left: %v %v", left, err)
	}
}

// An immediate UPDATE commits on its own, so it cannot run inside a transaction that already
// buffered writes.
func TestExecReturningRefusedInsideOpenTransaction(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()
	seedJobs(t, c, 1)
	tx, err := c.Begin(ctx, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE docs SET touched = 1 WHERE _c = 'jobs'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, claimSQL); err == nil {
		t.Fatal("a claim inside a transaction with pending writes must be refused")
	}
}
