package sql_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// The cases live in go/testdata/golden/sql/cost_behaviour.json and are read by the Kotlin tree
// too (kdb-sql jvmTest CostBehaviourFixtureTest): keyed kdb_id lookups, the examined-rows
// budget, and the result bound must behave identically in both engines.
type costCase struct {
	Name        string   `json:"name"`
	SQL         string   `json:"sql"`
	Params      []string `json:"params"`
	RowBudget   int      `json:"rowBudget"`
	MaxRows     int      `json:"maxRows"`
	Access      string   `json:"access"`
	Rows        []string `json:"rows"`
	BudgetError int      `json:"budgetError"`
}

var idShorthand = regexp.MustCompile(`\$\{id:(\d+)\}`)

func expandIDs(s string) string {
	return idShorthand.ReplaceAllStringFunc(s, func(m string) string {
		var n int
		_, _ = fmt.Sscanf(idShorthand.FindStringSubmatch(m)[1], "%d", &n)
		return corpusID(n).String()
	})
}

func TestCostBehaviourFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "sql", "cost_behaviour.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		Docs  int        `json:"docs"`
		Cases []costCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	docs := make([]string, fx.Docs)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"name":"n%03d","rank":%d}`, i, i)
	}
	f := newDocFixture(t, "app/cost-fixture", schema.None(), nil, docs...)
	for _, c := range fx.Cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx := f.ctx
			ctx.RowBudget = c.RowBudget
			ctx.MaxRows = c.MaxRows
			for _, p := range c.Params {
				ctx.Parameters = append(ctx.Parameters, sql.ParamString{Value: expandIDs(p)})
			}
			res, err := f.engine.Execute(expandIDs(c.SQL), ctx)
			if c.BudgetError > 0 {
				var be *sql.ScanRowBudgetExceededError
				if !errors.As(err, &be) || be.Budget != c.BudgetError {
					t.Fatalf("want *ScanRowBudgetExceededError(budget %d), got %T: %v", c.BudgetError, err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(res.Rows))
			for i, r := range res.Rows {
				got[i] = cellString(r.Values[0])
			}
			if strings.Join(got, ",") != strings.Join(c.Rows, ",") {
				t.Fatalf("got %v, want %v", got, c.Rows)
			}
			switch c.Access {
			case "keylookup":
				if res.Plan != "keylookup" {
					t.Fatalf("plan %q, want keylookup", res.Plan)
				}
			case "scan":
				if res.Plan == "keylookup" {
					t.Fatalf("served by key, want a scan")
				}
			}
		})
	}
}
