package sql_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// The cases live in go/testdata/golden/sql/result_bound.json and are read by the Kotlin tree too
// (kdb-sql jvmTest), so the two engines are held to the same answers for the same queries.
type resultBoundCase struct {
	Name  string   `json:"name"`
	SQL   string   `json:"sql"`
	Rows  []string `json:"rows"`
	Count *int     `json:"count"`
	Error bool     `json:"error"`
}

type resultBoundFixture struct {
	Docs    int               `json:"docs"`
	MaxRows int               `json:"maxRows"`
	Cases   []resultBoundCase `json:"cases"`
}

func TestResultBoundFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "sql", "result_bound.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx resultBoundFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	f := boundedFixture(t, "app/bound-fixture", fx.Docs, fx.MaxRows)
	for _, c := range fx.Cases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := f.engine.Execute(c.SQL, f.ctx)
			if c.Error {
				var limit *sql.ResultRowLimitError
				if !errors.As(err, &limit) {
					t.Fatalf("want *ResultRowLimitError, got %T: %v", err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Count != nil {
				if len(res.Rows) != *c.Count {
					t.Fatalf("got %d rows, want %d", len(res.Rows), *c.Count)
				}
				return
			}
			got := make([]string, len(res.Rows))
			for i, r := range res.Rows {
				got[i] = cellString(r.Values[0])
			}
			if strings.Join(got, ",") != strings.Join(c.Rows, ",") {
				t.Fatalf("got %v, want %v", got, c.Rows)
			}
		})
	}
}
