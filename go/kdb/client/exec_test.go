package client_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// Exec reports how many rows the statement affected, so a DELETE that matched nothing is not
// mistaken for one that removed a row.
func TestExecReportsRowsAffected(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	if _, err := c.Exec(ctx, "app/data", `CREATE TABLE items (name VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a", "b"} {
		if n, err := c.Exec(ctx, "app/data", fmt.Sprintf(`INSERT INTO items (name) VALUES ('%s')`, n), nil); err != nil || n != 1 {
			t.Fatalf("insert: n=%d err=%v", n, err)
		}
	}
	if n, err := c.Exec(ctx, "app/data", `DELETE FROM items WHERE name = 'missing'`, nil); err != nil || n != 0 {
		t.Fatalf("delete of a missing row: n=%d err=%v, want 0", n, err)
	}
	if n, err := c.Exec(ctx, "app/data", `DELETE FROM items WHERE name = 'a'`, nil); err != nil || n != 1 {
		t.Fatalf("delete of a present row: n=%d err=%v, want 1", n, err)
	}
}

// Concurrent DML on one namespace shares one session with one pending transaction; without the
// per-namespace lock the second commit finds nothing pending and fails.
func TestConcurrentExecDeletesAllSucceed(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	const n = 50
	if _, err := c.Exec(ctx, "app/data", `CREATE TABLE items (name VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := c.Exec(ctx, "app/data", fmt.Sprintf(`INSERT INTO items (name) VALUES ('d%d')`, i), nil); err != nil {
			t.Fatal(err)
		}
	}
	errs := make([]error, n)
	counts := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			counts[i], errs[i] = c.Exec(ctx, "app/data", fmt.Sprintf(`DELETE FROM items WHERE name = 'd%d'`, i), nil)
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil || counts[i] != 1 {
			t.Fatalf("delete %d: n=%d err=%v", i, counts[i], errs[i])
		}
	}
	var left []struct{ Name string }
	if err := c.Query(ctx, "app/data", `SELECT name FROM items`, nil, &left); err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("%d rows survived", len(left))
	}
}
