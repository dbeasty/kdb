package sql_test

import (
	"math"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// A continuation token is not signed, so its offset is whatever the client sends. Added to the
// statement's own OFFSET it used to wrap negative and reach applyLimitOffset as a negative slice
// index - a panic instead of an empty page.
func TestPageOffsetOverflowIsAnEmptyPageNotAPanic(t *testing.T) {
	f := newQueryFixture(t, "app/page-offset-overflow", fixtureNames...)
	for _, query := range []string{
		`SELECT name FROM t OFFSET 1`,
		`SELECT name FROM t ORDER BY name OFFSET 1`,
		`SELECT name FROM t LIMIT 3 OFFSET 2`,
	} {
		ctx := f.ctx
		ctx.Page = &sql.PageRequest{Offset: math.MaxInt, Size: 2, OffsetOnly: true}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s: paging past MaxInt panicked: %v", query, r)
				}
			}()
			res, err := f.engine.Execute(query, ctx)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if len(res.Rows) != 0 || res.More {
				t.Fatalf("%s: a page past the end returned %d rows (more=%v)", query, len(res.Rows), res.More)
			}
		}()
	}
}
