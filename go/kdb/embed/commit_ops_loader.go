package embed

import (
	"crypto/sha256"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/deltaindex"
)

// commitOpsLoader serves the operations of commits whose operations the
// DAG's retention budget has evicted, by reading the frame that holds them
// out of the delta log - the log being the only place a commit's
// operations are durable.
//
// It finds a commit's frame through a deltaindex.Index keyed by commit hash and reads one frame
// per lookup. The index is built lazily, so a namespace nobody asks history of never pays for
// it. It used to be a map with an entry per commit in the log (about 73 bytes a commit after
// #176, never evicted); with a sidecar directory (SetDir) each sealed segment's entries are now a
// memory-mapped sidecar and only the newest segment's are on the heap. See package deltaindex.
//
// The log keeps growing after the index is first built, so a lookup that misses brings the index
// up to date before giving up - a commit logged after the first cold load and then evicted must
// not be reported as having no frame.
type commitOpsLoader struct {
	reader storage.DeltaSegmentReader
	index  *deltaindex.Index
}

func newCommitOpsLoader(reader storage.DeltaSegmentReader) *commitOpsLoader {
	return &commitOpsLoader{
		reader: reader,
		index: deltaindex.New(reader, deltaindex.Options{
			Kind: "commits",
			Keys: func(c document.Commit, emit func(codec.Hash)) { emit(c.Hash) },
		}),
	}
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
	loc, ok, err := l.index.Lookup(hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &CommitOperationsUnavailableError{CommitHash: hash}
	}
	commit, err := streamer.ReadCommitAt(loc.Segment, loc.FrameOffset)
	if err != nil {
		return nil, err
	}
	if commit.Hash != hash {
		// The index is a cache; the frame is the authority.
		return nil, &CommitOperationsUnavailableError{CommitHash: hash}
	}
	return commit.Operations, nil
}

// payload is the commit's logged payload, copied out of the frame, when the log holds it: false
// when the reader cannot hand payloads back or no frame is recorded for hash. The payload's SHA-256
// is checked against hash - the location index is a cache and the frame is the authority - so a
// stale entry reports false rather than another commit's bytes.
func (l *commitOpsLoader) payload(hash codec.Hash) ([]byte, bool, error) {
	pr, ok := l.reader.(storage.DeltaPayloadReader)
	if !ok {
		return nil, false, nil
	}
	loc, ok, err := l.index.Lookup(hash)
	if err != nil || !ok {
		return nil, false, err
	}
	var out []byte
	if err := pr.ReadPayloadAt(loc.Segment, loc.FrameOffset, func(p []byte) error {
		out = append([]byte(nil), p...)
		return nil
	}); err != nil {
		return nil, false, err
	}
	if sum := sha256.Sum256(out); sum != hash.Bytes {
		return nil, false, nil
	}
	return out, true, nil
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
