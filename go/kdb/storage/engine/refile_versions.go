package engine

import (
	"context"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// RefileStats is what one RefileVersions pass changed.
type RefileStats struct {
	// Frames is how many frames the pass walked.
	Frames int64
	// ToValue counts versions given a body record, ToLocation versions whose body record was
	// replaced by a location (its copy is dropped by the next blob-store merge).
	ToValue, ToLocation int64
}

// RefileVersions re-files every version the delta log holds for the filing threshold in force
// (StorageEngineConfig.ResolvedLargeCommitBodyBytes): by value where filedByValue says so, by
// location otherwise. It is how a change of StorageEngineConfig.DocumentCopies between opens
// reaches versions written before it - the way Cassandra applies a table's changed storage
// options to existing data by rewriting it in the background rather than at the change.
//
// Only versions that already have a record are touched: one the log holds but the blob store does
// not is not this pass's to create, and a body only the blob store holds (a snapshot bootstrap's)
// has no frame here to point a location at, so it is never visited. Every record stays checked
// against its content hash on read, so a pass interrupted part-way leaves a mix of records that
// are all correct; ctx's cancellation stops it between frames, and running it again finishes the
// job. Objects history strategy only, like every other writer of these records.
func (e *ServerEngine) RefileVersions(ctx context.Context) (RefileStats, error) {
	var st RefileStats
	if e.memTable == nil || e.deltaLoader == nil || e.HistoryStrategy() != storage.HistoryStrategyObjects {
		return st, nil
	}
	payloads, ok := e.deltaLoader.reader.(storage.DeltaPayloadStreamer)
	if !ok {
		return st, nil
	}
	threshold := e.config.ResolvedLargeCommitBodyBytes()
	segments, err := e.deltaLoader.reader.ListSegments()
	if err != nil {
		return st, err
	}
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	for _, seg := range segments {
		err := payloads.StreamFramePayloads(seg, func(payload []byte, frameOffset int64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			st.Frames++
			total := frameBodyBytes(payload)
			_ = document.ScanAllCommitWrites(payload, func(id codec.UUID, patch []byte) bool {
				h := hasher.HashBytes(id, patch)
				raw := e.memTable.Get(h)
				if raw == nil {
					return true
				}
				_, _, isLocation := decodeDocumentLocation(raw)
				byValue := threshold > 0 && filedByValue(total-int64(len(patch)), threshold)
				switch {
				case byValue && isLocation:
					// Copied: patch aliases the payload, which the stream reuses.
					e.memTable.Put(h, []byte(string(patch)))
					st.ToValue++
				case !byValue && !isLocation:
					// The body may have been filed from another frame holding the same bytes;
					// any frame that holds them is a correct location.
					e.memTable.Put(h, encodeDocumentLocation(seg.SequenceNumber, frameOffset))
					st.ToLocation++
				}
				return true
			})
			e.maybeFlushMemtable()
			return nil
		})
		if err != nil {
			return st, err
		}
	}
	return st, nil
}

// VersionFiledByValue reports whether the record for contentHash holds the body itself (true)
// or a location in the delta log (false); found is false when there is no record. For tests and
// reporting.
func (e *ServerEngine) VersionFiledByValue(contentHash codec.Hash) (byValue, found bool) {
	if e.memTable == nil {
		return false, false
	}
	raw := e.memTable.Get(contentHash)
	if raw == nil {
		return false, false
	}
	_, _, isLocation := decodeDocumentLocation(raw)
	return !isLocation, true
}
