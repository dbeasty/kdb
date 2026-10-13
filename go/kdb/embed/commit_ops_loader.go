package embed

import (
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// commitFrameRef locates the delta frame one commit was written in.
type commitFrameRef struct {
	segment     storage.DeltaSegmentRef
	frameOffset int64
}

// frameLoc is commitFrameRef as the index stores it: the segment as a position in the loader's
// segment table, so a value holds no pointers. The index has one entry per commit in the log, and
// a commitFrameRef per entry - a whole DeltaSegmentRef, strings and all - cost about 225 bytes a
// commit at 400,000 commits, all of it scanned by the collector on every cycle; a frameLoc and its
// key are about 60 bytes and are not scanned at all.
type frameLoc struct {
	segment uint32
	offset  int64
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
// exists to allow removing: about 60 bytes per commit (frameLoc) against the
// full text of every document that commit wrote.
//
// The log keeps growing after the index is first built, so a lookup that
// misses brings the index up to date before giving up: segments it has not
// seen are scanned, and so is the newest one it had, which may have been
// appended to since. The index used to be built once and never again - a
// picture of the log at the first cold load - so every commit logged after
// that and then evicted was reported as having no frame at all.
type commitOpsLoader struct {
	reader storage.DeltaSegmentReader

	// refreshMu serializes index builds; mu guards frame and sealed.
	refreshMu sync.Mutex
	mu        sync.RWMutex
	frame     map[codec.Hash]frameLoc
	// segments is the segment table frameLoc.segment points into; segmentIdx finds a sequence's
	// position. A segment rescanned after it grew replaces its entry in place.
	segments   []storage.DeltaSegmentRef
	segmentIdx map[int64]uint32
	// sealed holds segments indexed while a newer one already existed: nothing
	// more can be appended to them, so they are never scanned again.
	sealed map[int64]struct{}
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
	streamer, canRead := l.reader.(storage.DeltaCommitStreamer)
	if !canRead {
		return nil, &CommitOperationsUnavailableError{CommitHash: hash}
	}
	ref, ok := l.lookup(hash)
	if !ok {
		if err := l.refresh(); err != nil {
			return nil, err
		}
		if ref, ok = l.lookup(hash); !ok {
			return nil, &CommitOperationsUnavailableError{CommitHash: hash}
		}
	}
	commit, err := streamer.ReadCommitAt(ref.segment, ref.frameOffset)
	if err != nil {
		return nil, err
	}
	return commit.Operations, nil
}

func (l *commitOpsLoader) lookup(hash codec.Hash) (commitFrameRef, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	loc, ok := l.frame[hash]
	if !ok {
		return commitFrameRef{}, false
	}
	return commitFrameRef{segment: l.segments[loc.segment], frameOffset: loc.offset}, true
}

// refresh indexes every segment not yet known to be sealed, recording where
// each commit was framed. Torn tails are tolerated as replay tolerates them:
// whatever was scanned before the fault is indexed and the walk moves on.
func (l *commitOpsLoader) refresh() error {
	l.refreshMu.Lock()
	defer l.refreshMu.Unlock()
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		return nil
	}
	segments, err := l.reader.ListSegments()
	if err != nil {
		return err
	}
	newest := int64(-1)
	for _, seg := range segments {
		if seg.SequenceNumber > newest {
			newest = seg.SequenceNumber
		}
	}
	l.mu.RLock()
	sealed := l.sealed
	l.mu.RUnlock()
	type found struct {
		hash   codec.Hash
		seq    int64
		offset int64
	}
	var frames []found
	scanned := map[int64]storage.DeltaSegmentRef{}
	var nowSealed []int64
	for _, seg := range segments {
		if _, done := sealed[seg.SequenceNumber]; done {
			continue
		}
		segment := seg
		stream := streamer.StreamCommits
		if seg.Tail.Kind == storage.TailDamaged {
			// Damaged in place, with intact frames after the damage. Open only gets here when a
			// checkpoint already holds everything the segment recorded (replay refuses such a
			// segment); the commits after the damage are as real as those before it, so index
			// them too rather than stopping where the listing's readable prefix ends. A lookup
			// for the damaged frame's own commit still fails loudly.
			if dr, ok := l.reader.(storage.DamageTolerantReader); ok {
				stream = dr.StreamCommitsPastDamage
			}
		}
		scanned[segment.SequenceNumber] = segment
		err := stream(segment, func(c document.Commit, offset int64) error {
			frames = append(frames, found{hash: c.Hash, seq: segment.SequenceNumber, offset: offset})
			return nil
		})
		var corrupt *delta.CorruptFrameError
		if err != nil && !errors.As(err, &corrupt) {
			return err
		}
		if seg.SequenceNumber < newest {
			nowSealed = append(nowSealed, seg.SequenceNumber)
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.frame == nil {
		l.frame = make(map[codec.Hash]frameLoc, len(frames))
		l.segmentIdx = make(map[int64]uint32)
	}
	if l.sealed == nil {
		l.sealed = make(map[int64]struct{})
	}
	for seq, ref := range scanned {
		if i, ok := l.segmentIdx[seq]; ok {
			l.segments[i] = ref
			continue
		}
		l.segmentIdx[seq] = uint32(len(l.segments))
		l.segments = append(l.segments, ref)
	}
	for _, f := range frames {
		l.frame[f.hash] = frameLoc{segment: l.segmentIdx[f.seq], offset: f.offset}
	}
	for _, seq := range nowSealed {
		l.sealed[seq] = struct{}{}
	}
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

// Is lets the DAG and peer sync, which cannot import this package, recognise the one load
// failure a peer's copy of the commit can repair - see dag.RefillUnloggedCommit.
func (e *CommitOperationsUnavailableError) Is(target error) bool {
	return target == dag.ErrOperationsNotLogged
}
