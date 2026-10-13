package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/wire"
)

// The client's whole load path - point reads and writes, SQL with paging, transactions, conflicts
// - behaves the same over binary frames (B4) as over the JSON envelope.
func TestLoadPathBehavesTheSameOverBinaryAndJSONFrames(t *testing.T) {
	for _, jsonOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("jsonOnly=%v", jsonOnly), func(t *testing.T) {
			addr, _ := startTestServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c, err := client.ConnectWithOptions(ctx, addr, "", client.ConnectOptions{JSONOnly: jsonOnly})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			wantFeatures := wire.SupportedWireFeatures
			if jsonOnly {
				wantFeatures = nil
			}
			if got := c.WireFeatures(); !reflect.DeepEqual(got, wantFeatures) {
				t.Fatalf("negotiated %v, want %v", got, wantFeatures)
			}

			const ns = "app/data"
			id := "6f1c2c4e-0000-4000-8000-000000000001"
			body := `{"text":"quote \" backslash \\ newline \n tab \t é ☃ <html>","n":1}`
			head, err := c.PutJSON(ctx, ns, id, []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			got, at, err := c.GetJSON(ctx, ns, id)
			if err != nil {
				t.Fatal(err)
			}
			if at == "" || !jsonEqual(t, got, []byte(body)) {
				t.Fatalf("read back %s at %q", got, at)
			}
			if _, _, err := c.GetJSON(ctx, ns, "6f1c2c4e-0000-4000-8000-0000000000ff"); err == nil || !errors.Is(err, client.ErrNotFound) {
				t.Fatalf("missing document: %v", err)
			}
			if _, err := c.Upsert(ctx, ns, id, []byte(`{"m":2}`)); err != nil {
				t.Fatal(err)
			}

			// A commit anchored on a stale head loses: a CONFLICT_REPORT reply.
			_, err = c.Commit(ctx, client.Transaction{Namespace: ns, BaseVersion: head, Writes: []client.DocWrite{{DocID: id, JSON: []byte(`{"n":3}`)}}})
			if !errors.Is(err, client.ErrConflict) {
				t.Fatalf("stale commit: %v", err)
			}

			seedItems(t, c, 120)
			cols, rows, err := c.QueryRaw(ctx, ns, `SELECT name, grp FROM items WHERE grp = 'g1' ORDER BY name`, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cols, []string{"name", "grp"}) || len(rows) != 40 || rows[0][0] != "n0001" || rows[39][1] != "g1" {
				t.Fatalf("query: %v, %d rows, first %v", cols, len(rows), rows[0])
			}
			var paged []string
			err = c.QueryRawPages(ctx, ns, `SELECT name FROM items ORDER BY name`, nil, 25, func(_ []string, rows [][]string) error {
				for _, r := range rows {
					if r[0] != "" { // the documents written above have no name
						paged = append(paged, r[0])
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(paged) != 120 || paged[119] != "n0119" {
				t.Fatalf("paged: %d rows", len(paged))
			}

			tx, err := c.Begin(ctx, ns)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO items (name, grp) VALUES ('tx', 'g9')`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			_, rows, err = c.QueryRaw(ctx, ns, `SELECT name FROM items WHERE grp = 'g9'`, nil)
			if err != nil || len(rows) != 1 || rows[0][0] != "tx" {
				t.Fatalf("after tx: %v %v", rows, err)
			}
			if _, _, err := c.QueryRaw(ctx, ns, `SELECT nope FROM`, nil); err == nil {
				t.Fatal("a bad statement succeeded")
			}
		})
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("%s: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return reflect.DeepEqual(x, y)
}
