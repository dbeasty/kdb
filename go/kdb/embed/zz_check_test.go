package embed_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

func zzHeapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

func TestZZCheck(t *testing.T) {
	for _, strat := range []storage.HistoryStrategy{storage.HistoryStrategyReplay, storage.HistoryStrategyObjects} {
		for _, n := range []int{1000, 4000, 16000} {
			root := t.TempDir()
			open := func() *embed.EmbeddedKdbRuntime {
				o := embed.FileRuntimeOptions{}
				o.Storage.HistoryStrategy = strat
				rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), o)
				if err != nil {
					t.Fatal(err)
				}
				return rt
			}
			rt := open()
			commits := make([]codec.Hash, 0, n)
			for i := 0; i < n; i++ {
				b, _ := json.Marshal(map[string]any{
					"id": "11111111-1111-4111-8111-111111111111", "rev": i,
				})
				res, err := embed.PutJSONDocument(rt, "bench/matches", string(b))
				if err != nil {
					t.Fatal(err)
				}
				commits = append(commits, res.Commit)
			}
			writeHeap := zzHeapMB()
			runtime.KeepAlive(rt)
			rt.Close()

			re := open()
			afterOpen := zzHeapMB()
			for _, i := range []int{0, n / 4, n / 2, 3 * n / 4, n - 1} {
				c, err := re.DAG.GetCommitOrThrow(commits[i])
				if err != nil {
					t.Fatal(err)
				}
				if _, err := re.Storage.GetDocument("bench/matches", codec.UUID{}, c.DocumentTreeHash); err != nil {
					t.Fatal(err)
				}
			}
			afterWalk := zzHeapMB()
			trees := ""
			if e, ok := re.Storage.(*engine.ServerEngine); ok {
				trees = fmt.Sprintf(" | trees %d (%.2f MB)",
					e.HistoryTreesResident(), float64(e.HistoryTreesResidentBytes())/(1024*1024))
			}
			runtime.KeepAlive(re)
			re.Close()
			t.Log(fmt.Sprintf("%-7s %5d commits | write %6.2f MB | open %6.2f MB | after 5 hist reads %6.2f MB%s",
				strat, n, writeHeap, afterOpen, afterWalk, trees))
		}
	}
}
