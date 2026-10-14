package embed

import (
	"context"
	"log"

	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// DefaultCloseCompactionBytes is how many bytes of SSTables closing a namespace will still merge
// (StorageOptions.CloseCompactionBytes). Small enough that close stays around a second whatever
// the namespace's size - a merge runs at roughly 35 MB/s, most of it per-key work - and large
// enough that a namespace small enough to be opened and closed in quick cycles is still merged
// on every close, as it always was.
const DefaultCloseCompactionBytes int64 = 64 << 20

// closeCompactionLimit resolves StorageOptions.CloseCompactionBytes to the limit close passes to
// engine.CompactBlobStoreUpTo.
func closeCompactionLimit(configured int64) int64 {
	switch {
	case configured == 0:
		return DefaultCloseCompactionBytes
	case configured < 0:
		return 0
	}
	return configured
}

// Why close no longer merges a large store, and what does instead.
//
// Close merged every SSTable into one because, when that was written, nothing else ran
// maintenance: close was the only moment a namespace's tables were ever merged, and so the only
// thing keeping their count and - under history=none - their superseded versions bounded. No
// correctness property depends on it. Truncation is finished before the merge starts and never
// reads its result; the checkpoint names delta segments, not tables; an open discovers whatever
// tables are on disk; and the merge's own crash safety (write the output, swap, then delete) is
// the same wherever it runs.
//
// Now a server's maintenance loop merges on a timer, and close merges only a store small enough
// to merge quickly (engine.CompactBlobStoreUpTo). That leaves an embedded runtime that never
// runs maintenance, which would otherwise gain a table per flush forever; for it a writable open
// that finds a backlog merges it in the background, and closing the namespace cancels that merge
// rather than waiting it out - a cancelled merge has written nothing and leaves the tables as
// they were, so the next open simply tries again.

// backgroundCompaction is one merge started by an open. stop cancels it and waits for it.
type backgroundCompaction struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startBackgroundCompaction merges eng's SSTables in the background when they have reached the
// compaction trigger and the namespace's reclaim mode allows reclaiming at all (under
// storage.ReclaimManual nothing is destroyed until asked, and a merge drops unreachable versions).
// Returns nil, having started nothing, otherwise.
func startBackgroundCompaction(eng *engine.ServerEngine, namespaceID string) *backgroundCompaction {
	if eng == nil || eng.BlobRunCount() < sstable.DefaultCompactionTrigger || !eng.ReclaimMode().Reclaims() {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &backgroundCompaction{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(b.done)
		c, err := eng.CompactBlobStoreContext(ctx)
		switch {
		case ctx.Err() != nil:
			// Closed first. Nothing was written; the next open or maintenance pass merges.
		case err != nil:
			log.Printf("kdb: namespace %s: background sstable merge failed (%v) - it keeps its current tables", namespaceID, err)
		case c.Removed > 0:
			log.Printf("kdb: namespace %s: merged %d sstables into %d in the background, removed %d, dropped %d unreachable version(s)",
				namespaceID, c.Merged, len(c.Outputs), c.Removed, c.Dropped)
		}
	}()
	return b
}

// stop cancels the merge and waits for it to let go of the store. Safe on nil.
func (b *backgroundCompaction) stop() {
	if b == nil {
		return
	}
	b.cancel()
	<-b.done
}
