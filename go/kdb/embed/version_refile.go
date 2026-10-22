package embed

import (
	"context"
	"fmt"
	"log"

	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// recordedFilingThreshold is the threshold meta says the namespace's records are filed for, in
// the engine's terms (0 = never by value).
func recordedFilingThreshold(m namespaceMeta) int64 {
	switch {
	case m.FilingThresholdBytes == 0:
		return storage.DefaultLargeCommitBodyBytes
	case m.FilingThresholdBytes < 0:
		return 0
	}
	return m.FilingThresholdBytes
}

// metaFilingThreshold is threshold as meta records it.
func metaFilingThreshold(threshold int64) int64 {
	if threshold <= 0 {
		return -1
	}
	return threshold
}

// versionRefile is one background re-filing pass started by an open. stop cancels it and waits.
type versionRefile struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startVersionRefile re-files eng's versions in the background when the filing threshold in force
// differs from the one the namespace's marker records - StorageOptions.DocumentCopies changed
// since the last open - and records the new threshold once every version is filed for it. Returns
// nil, having started nothing, when they agree or the namespace has no records of this kind.
//
// Until the pass finishes the records are a mix of both thresholds, every one of them correct; new
// commits and cold reads already file for the new one. A pass cut short by close records nothing,
// so the next open runs it again.
func startVersionRefile(eng *engine.ServerEngine, dataRoot, namespaceID string) *versionRefile {
	if eng == nil || eng.HistoryStrategy() != storage.HistoryStrategyObjects {
		return nil
	}
	meta, _ := readNamespaceMeta(dataRoot, namespaceID)
	want := eng.FilingThresholdBytes()
	recorded := recordedFilingThreshold(meta)
	if recorded == want {
		return nil
	}
	// Towards fewer copies: a higher threshold, or none at all. Decided by the switch rather than
	// by what this pass moves, because a pass that resumes one cut short during its merge finds
	// every location already back and moves nothing - the merge still has to run.
	fewerCopies := want == 0 || (recorded > 0 && want > recorded)
	ctx, cancel := context.WithCancel(context.Background())
	r := &versionRefile{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		st, err := eng.RefileVersions(ctx)
		var merged string
		if err == nil && ctx.Err() == nil && fewerCopies {
			// The copies are shadowed but still on disk, and the size-tiered policy may never
			// reach their tables on a quiet store; a major compaction gives the disk back now, as
			// Cassandra's is the way to apply changed storage options to all of a table at once.
			// Before the marker, so a merge cut short is retried with the pass by the next open.
			var c sstable.CompactionResult
			if c, err = eng.CompactBlobStoreAllContext(ctx); err == nil {
				merged = fmt.Sprintf("; merged %d tables to drop the copies", c.Merged)
			}
		}
		switch {
		case ctx.Err() != nil:
			// Closed first; the marker still names the old threshold, so the next open resumes.
		case err != nil:
			log.Printf("kdb: namespace %s: re-filing versions for document copies failed (%v) - the next open retries", namespaceID, err)
		default:
			// Re-read: the marker may have gained other fields since the open read it.
			m, _ := readNamespaceMeta(dataRoot, namespaceID)
			if m.NamespaceID == "" {
				m.NamespaceID = namespaceID
			}
			m.FilingThresholdBytes = metaFilingThreshold(want)
			if err := writeNamespaceMeta(dataRoot, namespaceID, m); err != nil {
				log.Printf("kdb: namespace %s: re-filed versions but could not record it (%v) - the next open re-files again", namespaceID, err)
				return
			}
			log.Printf("kdb: namespace %s: re-filed versions for a %d-byte threshold across %d frames: %d to body copies, %d back to locations%s",
				namespaceID, want, st.Frames, st.ToValue, st.ToLocation, merged)
		}
	}()
	return r
}

// stop cancels the pass and waits for it to let go of the engine. Safe on nil.
func (r *versionRefile) stop() {
	if r == nil {
		return
	}
	r.cancel()
	<-r.done
}
