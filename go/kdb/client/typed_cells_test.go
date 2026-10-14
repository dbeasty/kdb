package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

// QueryValues tells a NULL from "", and a number from text, over binary frames and over the JSON
// envelope alike.
func TestQueryValuesReturnsTypedCells(t *testing.T) {
	for _, jsonOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("jsonOnly=%v", jsonOnly), func(t *testing.T) {
			addr, _ := startTestServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, err := client.ConnectWithOptions(ctx, addr, "", client.ConnectOptions{JSONOnly: jsonOnly})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			const ns = "app/data"
			if _, err := c.Exec(ctx, ns, `CREATE TABLE things (name VARCHAR NOT NULL, n INTEGER, x DOUBLE, ok BOOLEAN, note VARCHAR)`, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Exec(ctx, ns, `INSERT INTO things (name, n, x, ok, note) VALUES ('a', 42, 1.5, TRUE, '')`, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Exec(ctx, ns, `INSERT INTO things (name, n, x, ok) VALUES ('b', -7, 0.25, FALSE)`, nil); err != nil {
				t.Fatal(err)
			}
			cols, rows, err := c.QueryValues(ctx, ns, `SELECT name, n, x, ok, note FROM things ORDER BY name`, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(cols) != 5 || len(rows) != 2 {
				t.Fatalf("cols %v, %d rows", cols, len(rows))
			}
			// A BOOLEAN column is stored as an integer cell, so it reads back as 1/0.
			want := [][]any{{"a", int64(42), 1.5, int64(1), ""}, {"b", int64(-7), 0.25, int64(0), nil}}
			for i := range want {
				for j := range want[i] {
					if rows[i][j] != want[i][j] {
						t.Fatalf("row %d col %s: %#v (%T), want %#v (%T)", i, cols[j], rows[i][j], rows[i][j], want[i][j], want[i][j])
					}
				}
			}
			_, rows, err = c.QueryValues(ctx, ns, `SELECT _doc FROM things WHERE name = 'a'`, nil)
			if err != nil {
				t.Fatal(err)
			}
			doc, ok := rows[0][0].(json.RawMessage)
			if !ok || !json.Valid(doc) {
				t.Fatalf("_doc: %#v", rows[0][0])
			}
		})
	}
}
