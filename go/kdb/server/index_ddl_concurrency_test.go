package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// CREATE INDEX while writers commit: the new index is built by a scan at one head while commits
// keep landing, and must still end up matching the documents at head - no commit's update lost
// to, or doubled by, the rebuild.
func TestCreateIndexWhileWritersCommit(t *testing.T) {
	withFlushTiming(t, 0, 0)
	srv, p := openDiskIndexedRuntime(t, t.TempDir())
	defer srv.Release()
	for i := 0; i < 120; i++ {
		flushUpsert(t, srv, i, 0)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 1; !stop.Load(); v++ {
			flushUpsert(t, srv, v%120, v)
		}
	}()
	for j := 0; j < flushTestFields; j++ {
		if err := p.CreateIndex(sql.StmtCreateIndex{
			Name: fmt.Sprintf("by_g%d", j), Table: "docs", Fields: []sql.IndexField{{Path: fmt.Sprintf("g%d", j)}}, Using: "HASH",
		}, sql.QueryContext{NamespaceID: "app/data"}); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	verifyIndexesMatchScan(t, srv, p)
}
