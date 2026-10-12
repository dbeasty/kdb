package client_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func seedItems(t *testing.T, c interface {
	Exec(context.Context, string, string, []any) (int, error)
}, n int) {
	t.Helper()
	ctx := context.Background()
	if _, err := c.Exec(ctx, "app/data", `CREATE TABLE items (name VARCHAR NOT NULL, grp VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := c.Exec(ctx, "app/data", fmt.Sprintf(`INSERT INTO items (name, grp) VALUES ('n%04d', 'g%d')`, i, i%3), nil); err != nil {
			t.Fatal(err)
		}
	}
}

// A whole-collection read larger than --max-result-rows fails as a single query (the interim
// behaviour) but streams in pages: every row exactly once, from one snapshot.
func TestQueryRawPagesReadsMoreThanTheResultBound(t *testing.T) {
	addr, rt := startTestServer(t)
	rt.SetMaxResultRows(50)
	c := connectTestClient(t, addr)
	ctx := context.Background()
	seedItems(t, c, 230)

	if _, _, err := c.QueryRaw(ctx, "app/data", `SELECT name FROM items`, nil); err == nil {
		t.Fatal("expected the unpaged read to exceed the 50-row result bound")
	}

	collect := func(sqlText string, pageSize int) ([]string, int) {
		var names []string
		pages := 0
		err := c.QueryRawPages(ctx, "app/data", sqlText, nil, pageSize, func(cols []string, rows [][]string) error {
			pages++
			if len(rows) > 50 {
				t.Fatalf("a page of %d rows exceeds the bound", len(rows))
			}
			for _, r := range rows {
				names = append(names, r[0])
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", sqlText, err)
		}
		return names, pages
	}

	names, pages := collect(`SELECT name FROM items ORDER BY name`, 40)
	if len(names) != 230 || pages != 6 {
		t.Fatalf("ordered: %d rows in %d pages, want 230 in 6", len(names), pages)
	}
	for i, n := range names {
		if n != fmt.Sprintf("n%04d", i) {
			t.Fatalf("row %d = %s: pages overlapped, skipped or reordered", i, n)
		}
	}

	// Unordered: no row twice, none missing.
	names, _ = collect(`SELECT name FROM items`, 40)
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Fatalf("%s returned on two pages", n)
		}
		seen[n] = true
	}
	if len(seen) != 230 {
		t.Fatalf("%d distinct rows, want 230", len(seen))
	}

	// The query's own LIMIT/OFFSET bound the paged result.
	names, _ = collect(`SELECT name FROM items ORDER BY name LIMIT 95 OFFSET 10`, 30)
	if len(names) != 95 || names[0] != "n0010" || names[94] != "n0104" {
		t.Fatalf("limit/offset: %d rows %v..%v", len(names), names[0], names[len(names)-1])
	}

	// A page size above the bound is clamped, not an error; a result that fits is one page.
	if names, pages = collect(`SELECT name FROM items WHERE grp = 'g0' ORDER BY name`, 0); len(names) != 77 || pages != 2 {
		t.Fatalf("clamped: %d rows in %d pages, want 77 in 2", len(names), pages)
	}
}

// Pages are one snapshot: rows written between pages are not seen, and a token cannot be used for
// a different statement.
func TestQueryRawPagesAreOneSnapshotAndBoundToTheirQuery(t *testing.T) {
	addr, rt := startTestServer(t)
	rt.SetMaxResultRows(50)
	c := connectTestClient(t, addr)
	ctx := context.Background()
	seedItems(t, c, 120)

	total, first := 0, true
	err := c.QueryRawPages(ctx, "app/data", `SELECT name FROM items ORDER BY name`, nil, 40, func(_ []string, rows [][]string) error {
		total += len(rows)
		if first {
			first = false
			// A write that lands after the first page must not appear in the later ones.
			if _, err := c.Exec(ctx, "app/data", `INSERT INTO items (name, grp) VALUES ('zzz-late', 'g9')`, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || total != 120 {
		t.Fatalf("snapshot: %d rows, err %v, want 120", total, err)
	}

	if _, _, err := c.QueryRaw(ctx, "app/data", `SELECT name FROM items`, nil); err == nil || !strings.Contains(err.Error(), "50") {
		t.Fatalf("unpaged read should still report the bound, got %v", err)
	}
}
