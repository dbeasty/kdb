package server

import (
	"testing"
	"time"
)

// U5: after an unclean stop the index snapshots name an earlier commit than the head. Open
// restores them and applies only the documents that changed since - not a rebuild of every index
// from every document - and the indexes answer exactly as a scan does.
func TestIndexesCatchUpFromSnapshotsBehindTheHead(t *testing.T) {
	withFlushTiming(t, time.Hour, 0)
	dir := t.TempDir()
	srv, p := openDiskIndexedRuntime(t, dir)
	createFlushTestIndexes(t, p)
	for i := 0; i < 300; i++ {
		flushUpsert(t, srv, i, 0)
	}
	if err := p.Flush(); err != nil { // every store on disk at this head
		t.Fatal(err)
	}
	changed := 0
	for i := 0; i < 300; i += 7 {
		flushUpsert(t, srv, i, 1) // values move, fields come and go
		changed++
	}
	for i := 3; i < 300; i += 31 {
		flushDelete(t, srv, i)
		changed++
	}
	for i := 300; i < 320; i++ {
		flushUpsert(t, srv, i, 0) // new documents
		changed++
	}
	p.abandon() // the kill: no snapshot is written past the flush above
	srv.Release()

	srv2, p2 := openDiskIndexedRuntime(t, dir)
	defer srv2.Release()
	verifyIndexesMatchScan(t, srv2, p2)
	got := p2.caughtUpDocs.Load()
	if got == 0 {
		t.Fatal("indexes were rebuilt from the documents, not caught up from their snapshots")
	}
	if got > int64(changed) {
		t.Fatalf("catch-up applied %d documents; only %d changed since the snapshots", got, changed)
	}
	// And the caught-up indexes keep working through later commits and another reopen.
	flushUpsert(t, srv2, 5, 2)
	flushDelete(t, srv2, 6)
	verifyIndexesMatchScan(t, srv2, p2)
}
