package engine

import (
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// coldFrameRef locates the delta frame a document version was written in: an index into the
// loader's segment table and the frame's byte offset in that segment.
//
// Pointer-free and 16 bytes on purpose. It used to embed the whole storage.DeltaSegmentRef - a
// namespace string, a UUID, two hashes and a tail descriptor, about 168 bytes with pointers in
// it - once per historical version, and the index is never evicted. A map whose keys and values
// hold no pointers is also one the garbage collector does not scan: the lesson of #159, where a
// pointer-free map cut full-GC mark time ~35x before anything moved off the heap.
type coldFrameRef struct {
	segment     uint32
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
//     version store exists to remove; a 32-byte key and a 16-byte
//     pointer-free location per version does not.
//   - It is built lazily, on the first miss, and never at open. A store
//     that is only ever read at its head - almost all of them - pays
//     nothing, and one that walks its history pays for a single pass
//     instead of a rescan per document, which is what keeps a historical
//     scan linear rather than quadratic.
//   - It catches up with the log on a miss. It used to be built once, a picture of the log at the
//     first miss, so a version written to a later segment and then evicted read as missing - a
//     wrong answer, not a slow one, and one bounded live bodies would hit on live data. A miss now
//     indexes the segments it has not seen and the newest one's growth, and only when the log
//     changed since the last look, so a version that is genuinely absent costs a listing.
//
// This is the same bargain a pack index makes: history stays on disk and
// is read through an index when someone actually asks for it.
type deltaColdLoader struct {
	reader storage.DeltaSegmentReader

	// refreshMu serializes index passes; mu guards what they publish.
	refreshMu sync.Mutex
	mu        sync.RWMutex
	frame     map[codec.Hash]coldFrameRef
	// segments is the segment table coldFrameRef.segment indexes: one ref per segment in the
	// log, rather than one copy per version. slotOf finds a sequence's slot.
	segments []storage.DeltaSegmentRef
	slotOf   map[int64]uint32
	// indexed is how many bytes of each segment have been indexed; a segment whose listed size
	// has not grown is not scanned again.
	indexed map[int64]int64
}

func newDeltaColdLoader(reader storage.DeltaSegmentReader) *deltaColdLoader {
	return &deltaColdLoader{reader: reader}
}

// load implements coldDocLoader. A version that is not in the log is
// reported as simply not found: that is what a miss meant when the version
// store never evicted, and no caller can tell the difference.
func (l *deltaColdLoader) load(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	ref, segment, ok := l.lookup(contentHash)
	if !ok {
		if err := l.index(); err != nil {
			return document.Document{}, false, err
		}
		if ref, segment, ok = l.lookup(contentHash); !ok {
			return document.Document{}, false, nil
		}
	}
	if dr, ok := l.reader.(storage.DeltaDocumentReader); ok {
		return readVersionFromFrame(dr, segment, ref.frameOffset, docID, contentHash)
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		return document.Document{}, false, nil
	}
	commit, err := streamer.ReadCommitAt(segment, ref.frameOffset)
	if err != nil {
		return document.Document{}, false, err
	}
	var hasher document.ContentHasher
	for _, op := range commit.Operations {
		w, isWrite := op.(document.WriteOp)
		if !isWrite || w.DocID != docID {
			continue
		}
		if hasher.Hash(w.DocID, w.Patch) != contentHash {
			// A commit can write the same document more than once, and
			// several documents in one frame hash differently; only the
			// operation whose content actually matches is the one asked
			// for.
			continue
		}
		return documentFromPatch(w.DocID, w.Patch), true, nil
	}
	return document.Document{}, false, nil
}

func (l *deltaColdLoader) lookup(contentHash codec.Hash) (coldFrameRef, storage.DeltaSegmentRef, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ref, ok := l.frame[contentHash]
	if !ok {
		return coldFrameRef{}, storage.DeltaSegmentRef{}, false
	}
	return ref, l.segments[ref.segment], true
}

// contentHashers lends ContentHashers to cold reads, which may run on any goroutine.
var contentHashers = sync.Pool{New: func() any { return new(document.ContentHasher) }}

// readVersionFromFrame reads the version of docID whose content hash is contentHash out of the
// commit framed at frameOffset, decoding nothing of the commit but that document's writes (see
// storage.DeltaDocumentReader). A commit can write the same document more than once; only the
// write whose content matches is the version asked for, and only it is copied out of the frame.
func readVersionFromFrame(dr storage.DeltaDocumentReader, segment storage.DeltaSegmentRef, frameOffset int64, docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	var doc document.Document
	found := false
	err := dr.ReadDocumentAt(segment, frameOffset, docID, func(patch []byte) bool {
		if hasher.HashBytes(docID, patch) != contentHash {
			return true
		}
		doc, found = documentFromPatch(docID, string(patch)), true
		return false
	})
	if err != nil {
		return document.Document{}, false, err
	}
	return doc, found, nil
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
	l.refreshMu.Lock()
	defer l.refreshMu.Unlock()
	segments, err := l.reader.ListSegments()
	if err != nil {
		return err
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		// Nothing to index against: a reader that cannot report frame
		// offsets cannot be asked for one frame later either.
		l.mu.Lock()
		if l.frame == nil {
			l.frame = map[codec.Hash]coldFrameRef{}
		}
		l.mu.Unlock()
		return nil
	}
	l.mu.RLock()
	var todo []storage.DeltaSegmentRef
	for _, seg := range segments {
		if done, seen := l.indexed[seg.SequenceNumber]; !seen || seg.SizeBytes > done {
			todo = append(todo, seg)
		}
	}
	l.mu.RUnlock()
	if len(todo) == 0 {
		l.mu.Lock()
		if l.frame == nil {
			l.frame = map[codec.Hash]coldFrameRef{}
		}
		l.mu.Unlock()
		return nil
	}
	type found struct {
		hash   codec.Hash
		seq    int64
		offset int64
	}
	var frames []found
	// One hasher for the whole pass: ContentHash allocated a record value, a field map and an
	// encode buffer per operation, for every version in the log.
	var hasher document.ContentHasher
	// Past damage where the reader can: a damaged frame loses only the versions it held, not
	// every version after it in its segment. Safe, because load verifies every version it reads
	// against the content hash it was asked for.
	stream := streamer.StreamCommits
	if dr, ok := l.reader.(storage.DamageTolerantReader); ok {
		stream = dr.StreamCommitsPastDamage
	}
	for _, seg := range todo {
		segment := seg
		err := stream(segment, func(c document.Commit, offset int64) error {
			for _, op := range c.Operations {
				w, isWrite := op.(document.WriteOp)
				if !isWrite {
					continue
				}
				// documentFromPatch keeps the patch text verbatim whether or not it parses, so the
				// version's content hash is the hash of (DocID, Patch) - no need to build the
				// document, or to validate its JSON, just to hash it.
				frames = append(frames, found{hash: hasher.Hash(w.DocID, w.Patch), seq: segment.SequenceNumber, offset: offset})
			}
			return nil
		})
		var corrupt *delta.CorruptFrameError
		if err != nil && !errors.As(err, &corrupt) {
			return err
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.frame == nil {
		l.frame = make(map[codec.Hash]coldFrameRef, len(frames))
		l.slotOf = map[int64]uint32{}
		l.indexed = map[int64]int64{}
	}
	for _, seg := range todo {
		if slot, ok := l.slotOf[seg.SequenceNumber]; ok {
			l.segments[slot] = seg
		} else {
			l.slotOf[seg.SequenceNumber] = uint32(len(l.segments))
			l.segments = append(l.segments, seg)
		}
		l.indexed[seg.SequenceNumber] = seg.SizeBytes
	}
	// Later frames overwrite earlier ones, so the index names the most recent frame holding a
	// version, as the single pass always did.
	for _, f := range frames {
		l.frame[f.hash] = coldFrameRef{segment: l.slotOf[f.seq], frameOffset: f.offset}
	}
	return nil
}

// documentFromPatch mirrors how replay turns a WriteOp back into a
// document (see embed.applyReplayedCommit): a patch that will not parse is
// still the document's bytes and is kept as-is rather than dropped, so the
// content hash computed here matches the one the write path recorded.
//
// Both outcomes are the same document - the id and the patch text,
// verbatim - so the patch is not parsed at all. It used to be validated
// first (document.FromJSONWithID unmarshals the whole body into a map),
// only for the result to be thrown away either way.
func documentFromPatch(docID codec.UUID, patch string) document.Document {
	return document.Document{ID: docID, JSON: patch}
}
