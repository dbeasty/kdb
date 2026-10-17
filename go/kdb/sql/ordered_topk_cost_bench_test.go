package sql

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// BenchmarkOrderedWalkVsTopK is #318's two queries at a smaller scale: a HASH index on status
// picks the candidates and the ORDER BY column may also have a BTREE. "early" candidates are
// spread through the sort column's order (the walk finds the first row almost at once); "late"
// candidates all sort after most of the namespace (the walk passes most of the index first).
func BenchmarkOrderedWalkVsTopK(b *testing.B) {
	const docs, candidates = 100_000, 2_000
	for _, btree := range []bool{false, true} {
		indexes := map[string]index.IndexType{"status": index.IndexTypeHash}
		if btree {
			indexes["early"] = index.IndexTypeBTree
			indexes["late"] = index.IndexTypeBTree
		}
		f := newOrderedFixture(&testing.T{}, "app/walk-cost", indexes)
		r := rand.New(rand.NewSource(1))
		writes := map[codec.UUID]string{}
		for i := 0; i < docs; i++ {
			id, _ := codec.RandomUUID()
			status, late := "done", r.Intn(docs)*10
			if i < candidates {
				status, late = "pending", docs*9+r.Intn(docs)*2 // behind 90% of the others
			}
			writes[id] = fmt.Sprintf(`{"status":%q,"early":%d,"late":%d}`, status, r.Intn(docs*10), late)
			if len(writes) == 10_000 {
				f.commit(writes, nil)
				writes = map[codec.UUID]string{}
			}
		}
		f.commit(writes, nil)
		for _, col := range []string{"early", "late"} {
			b.Run(fmt.Sprintf("btree=%v/%s", btree, col), func(b *testing.B) {
				q := "SELECT kdb_id FROM t WHERE status = 'pending' ORDER BY " + col + " LIMIT 1"
				var plan string
				for i := 0; i < b.N; i++ {
					res, _, err := f.run(false, q, nil)
					if err != nil {
						b.Fatal(err)
					}
					plan = res.Plan
				}
				b.Logf("plan %s", plan)
			})
		}
	}
}
