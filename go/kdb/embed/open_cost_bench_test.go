package embed_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// growingDocumentStore writes rewrites whole-document versions of one
// document that gains a padded entry each time - the shape of a long-lived
// append-only aggregate (a game match, an audit trail, an order with a
// growing line-item list). It returns the data root and the final
// document's size in bytes.
//
// This is the pattern that makes open cost superlinear: nothing about the
// live data grows beyond one document, but the delta log holds the
// arithmetic sum of every version, and replay used to materialize all of
// them at once. See docs/benchmarks/open-cost.md.
func growingDocumentStore(tb testing.TB, rewrites, pad int) (root string, finalDocBytes int) {
	tb.Helper()
	root = tb.TempDir()
	ns := "bench/matches"
	rt, err := embed.OpenFileRuntime(root, "bench", ns, schema.None())
	if err != nil {
		tb.Fatal(err)
	}
	type doc struct {
		ID     string           `json:"id"`
		Events []map[string]any `json:"events"`
	}
	d := doc{ID: "11111111-1111-4111-8111-111111111111"}
	blob := strings.Repeat("x", pad)
	for i := 0; i < rewrites; i++ {
		d.Events = append(d.Events, map[string]any{
			"seq": i, "player": fmt.Sprintf("p%d", i%4),
			"action": "play", "card": fmt.Sprintf("%dS", i%13+1),
			"at": "2026-09-06T21:00:00Z", "blob": blob,
		})
		b, err := json.Marshal(d)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := embed.PutJSONDocument(rt, ns, string(b)); err != nil {
			tb.Fatal(err)
		}
	}
	b, err := json.Marshal(d)
	if err != nil {
		tb.Fatal(err)
	}
	rt.Close()
	return root, len(b)
}

func dataDirBytes(tb testing.TB, root string) int64 {
	tb.Helper()
	var total int64
	if err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return err
	}); err != nil {
		tb.Fatal(err)
	}
	return total
}

// TestOpenCostOfGrowingDocument records what it costs to reopen a store
// holding one document that was rewritten many times. It asserts the
// bounds that Track A exists to hold; the numbers themselves are logged so
// a regression shows its shape, not just a failure.
func TestOpenCostOfGrowingDocument(t *testing.T) {
	for _, pad := range []int{0, 3000} {
		t.Run(fmt.Sprintf("pad%d", pad), func(t *testing.T) {
			root, finalDoc := growingDocumentStore(t, 463, pad)
			onDisk := dataDirBytes(t, root)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			rt, err := embed.OpenFileRuntime(root, "bench", "bench/matches", schema.None())
			if err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			churn := after.TotalAlloc - before.TotalAlloc
			// Live heap has to be read after a collection, with the runtime
			// still reachable: HeapAlloc straight after open counts every
			// byte replay allocated and dropped as well as what it kept, so
			// it moves with GC timing rather than with retention - which is
			// the thing under test.
			runtime.GC()
			var settled runtime.MemStats
			runtime.ReadMemStats(&settled)
			live := settled.HeapAlloc
			runtime.KeepAlive(rt)
			rt.Close()

			liveMB := float64(live) / (1024 * 1024)
			t.Logf("final document %7.1f KB | on disk %7.2f MB | open churn %8.2f MB | live heap %7.2f MB",
				float64(finalDoc)/1024,
				float64(onDisk)/(1024*1024),
				float64(churn)/(1024*1024),
				liveMB)

			// The default budgets allow 48MB of history resident (32MB of
			// document versions plus 16MB of commit operations, see
			// storage.ResolvedDocumentCacheBytes and ResolvedCommitOpsBytes)
			// on top of the live data and the test binary's own heap. The
			// ceiling is set well clear of that so ordinary variation does
			// not trip it, and well under what unbounded retention costs:
			// this same row measured 382MB before history stopped being
			// held in full.
			const ceilingMB = 150
			if liveMB > ceilingMB {
				t.Fatalf("holding the namespace open costs %.2f MB for %.1f KB of live data, over the %d MB ceiling - "+
					"retention is following history again", liveMB, float64(finalDoc)/1024, ceilingMB)
			}
		})
	}
}
