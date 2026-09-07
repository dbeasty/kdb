package engine

import (
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// coldFrameRef locates the delta frame a document version was written in.
type coldFrameRef struct {
	segment     storage.DeltaSegmentRef
	frameOffset int64
}

// deltaColdLoader serves document versions that shardedDocByHashStore has
// evicted, by finding them again in the namespace's delta log - the only
// place every historical version is durable.
//
// It keeps an index from a version's content hash to the frame that holds
// it, and reads that one frame on demand. Two properties matter:
//
//   - The index holds locations, not documents. Holding the text would
//     reintroduce exactly the unbounded retention that bounding the
//     version store exists to remove; 48 bytes per version does not.
//   - It is built lazily, on the first miss, and never at open. A store
//     that is only ever read at its head - almost all of them - pays
//     nothing, and one that walks its history pays for a single pass
//     instead of a rescan per document, which is what keeps a historical
//     scan linear rather than quadratic.
//
// This is the same bargain a pack index makes: history stays on disk and
// is read through an index when someone actually asks for it.
type deltaColdLoader struct {
	reader storage.DeltaSegmentReader

	once  sync.Once
	build error
	mu    sync.RWMutex
	frame map[codec.Hash]coldFrameRef
}

func newDeltaColdLoader(reader storage.DeltaSegmentReader) *deltaColdLoader {
	return &deltaColdLoader{reader: reader}
}

// load implements coldDocLoader. A version that is not in the log is
// reported as simply not found: that is what a miss meant when the version
// store never evicted, and no caller can tell the difference.
func (l *deltaColdLoader) load(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	l.once.Do(func() { l.build = l.index() })
	if l.build != nil {
		return document.Document{}, false, l.build
	}
	l.mu.RLock()
	ref, ok := l.frame[contentHash]
	l.mu.RUnlock()
	if !ok {
		return document.Document{}, false, nil
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		return document.Document{}, false, nil
	}
	commit, err := streamer.ReadCommitAt(ref.segment, ref.frameOffset)
	if err != nil {
		return document.Document{}, false, err
	}
	for _, op := range commit.Operations {
		w, isWrite := op.(document.WriteOp)
		if !isWrite || w.DocID != docID {
			continue
		}
		doc := documentFromPatch(w.DocID, w.Patch)
		h, err := doc.ContentHash()
		if err != nil || h != contentHash {
			// A commit can write the same document more than once, and
			// several documents in one frame hash differently; only the
			// operation whose content actually matches is the one asked
			// for.
			continue
		}
		return doc, true, nil
	}
	return document.Document{}, false, nil
}

// index walks every segment once, recording where each written version
// lives. Later writes of identical content overwrite the entry, so the
// index names the most recent frame holding a version - the cheapest one
// to reach on a store whose newest segments are warmest.
//
// A torn tail is tolerated as replay tolerates it: versions scanned before
// it are kept and indexing continues. Unlike replay this does not single
// out the most recent segment, because a cold read makes no durability
// claim - the worst case is a version reported missing that a repair would
// recover, which is what a miss already meant here.
func (l *deltaColdLoader) index() error {
	segments, err := l.reader.ListSegments()
	if err != nil {
		return err
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		// Nothing to index against: a reader that cannot report frame
		// offsets cannot be asked for one frame later either.
		l.mu.Lock()
		l.frame = map[codec.Hash]coldFrameRef{}
		l.mu.Unlock()
		return nil
	}
	frames := make(map[codec.Hash]coldFrameRef)
	for _, seg := range segments {
		segment := seg
		err := streamer.StreamCommits(segment, func(c document.Commit, offset int64) error {
			for _, op := range c.Operations {
				w, isWrite := op.(document.WriteOp)
				if !isWrite {
					continue
				}
				h, err := documentFromPatch(w.DocID, w.Patch).ContentHash()
				if err != nil {
					continue
				}
				frames[h] = coldFrameRef{segment: segment, frameOffset: offset}
			}
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

// documentFromPatch mirrors how replay turns a WriteOp back into a
// document (see embed.applyReplayedCommit): a patch that will not parse is
// still the document's bytes and is kept as-is rather than dropped, so the
// content hash computed here matches the one the write path recorded.
func documentFromPatch(docID codec.UUID, patch string) document.Document {
	doc, err := document.FromJSONWithID(docID, patch)
	if err != nil {
		return document.Document{ID: docID, JSON: patch}
	}
	return doc
}
