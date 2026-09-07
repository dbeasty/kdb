package embed

import (
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// commitFrameRef locates the delta frame one commit was written in.
type commitFrameRef struct {
	segment     storage.DeltaSegmentRef
	frameOffset int64
}

// commitOpsLoader serves the operations of commits whose operations the
// DAG's retention budget has evicted, by reading the frame that holds them
// out of the delta log - the log being the only place a commit's
// operations are durable.
//
// It indexes commit hash to frame location on first use and reads one
// frame per lookup after that. The index is built lazily, so a namespace
// nobody asks history of never pays for it, and it holds locations rather
// than commits, so the index itself cannot reintroduce the retention it
// exists to allow removing: 48 bytes per commit against the full text of
// every document that commit wrote.
type commitOpsLoader struct {
	reader storage.DeltaSegmentReader

	once  sync.Once
	build error
	mu    sync.RWMutex
	frame map[codec.Hash]commitFrameRef
}

func newCommitOpsLoader(reader storage.DeltaSegmentReader) *commitOpsLoader {
	return &commitOpsLoader{reader: reader}
}

// load implements dag.CommitOperationsLoader. A commit the log does not
// hold is an error rather than an empty result: the DAG only asks about
// commits it evicted, so not finding one means the log lost something, and
// reporting no operations would look exactly like a commit that wrote
// nothing.
func (l *commitOpsLoader) load(hash codec.Hash) ([]document.Op, error) {
	l.once.Do(func() { l.build = l.index() })
	if l.build != nil {
		return nil, l.build
	}
	l.mu.RLock()
	ref, ok := l.frame[hash]
	l.mu.RUnlock()
	streamer, canRead := l.reader.(storage.DeltaCommitStreamer)
	if !ok || !canRead {
		return nil, &CommitOperationsUnavailableError{CommitHash: hash}
	}
	commit, err := streamer.ReadCommitAt(ref.segment, ref.frameOffset)
	if err != nil {
		return nil, err
	}
	return commit.Operations, nil
}

// index walks every segment once, recording where each commit was framed.
// Torn tails are tolerated as replay tolerates them: whatever was scanned
// before the fault is indexed and the walk moves on.
func (l *commitOpsLoader) index() error {
	segments, err := l.reader.ListSegments()
	if err != nil {
		return err
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		l.mu.Lock()
		l.frame = map[codec.Hash]commitFrameRef{}
		l.mu.Unlock()
		return nil
	}
	frames := make(map[codec.Hash]commitFrameRef)
	for _, seg := range segments {
		segment := seg
		err := streamer.StreamCommits(segment, func(c document.Commit, offset int64) error {
			frames[c.Hash] = commitFrameRef{segment: segment, frameOffset: offset}
			return nil
		})
		var corrupt *delta.CorruptFrameError
		if err != nil && !errors.As(err, &corrupt) {
			return err
		}
	}
	l.mu.Lock()
	l.frame = frames
	l.mu.Unlock()
	return nil
}

// CommitOperationsUnavailableError reports a commit whose operations the
// DAG dropped under its retention budget and the delta log cannot supply -
// the log is missing a commit the DAG says it stored. Detect with
// errors.As.
type CommitOperationsUnavailableError struct {
	CommitHash codec.Hash
}

func (e *CommitOperationsUnavailableError) Error() string {
	return "kdb: commit " + e.CommitHash.Hex() +
		" has no frame in the delta log, so its evicted operations cannot be read back"
}
