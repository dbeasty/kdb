package engine

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/deltaindex"
)

// deltaColdLoader serves document versions that shardedDocByHashStore has
// evicted, by finding them again in the namespace's delta log - the only
// place every historical version is durable.
//
// It reads through a deltaindex.Index from a version's content hash to the
// frame that holds it, and reads that one frame on demand. What matters:
//
//   - The index holds locations, not documents, and holds them off the heap. It used to be a
//     map with an entry for every version in the log, built on the first miss and never
//     evicted: 225 MB at 2M versions, growing with history. Now each sealed segment's
//     locations are a sorted, memory-mapped sidecar beside the log, and only the newest
//     segment's are on the heap - bounded by the segment rotation size, not by history. See
//     package deltaindex.
//   - It is built lazily, on the first miss, and never at open. A store
//     that is only ever read at its head - almost all of them - pays
//     nothing; one that walks its history pays for a single pass, once per
//     segment for the life of the data directory, since the sidecars outlive the process.
//   - It catches up with the log on a miss: a version written to a later segment and then
//     evicted must not read as missing, which bounded live bodies relies on.
//
// Under the objects history strategy a location record in the blob store answers first (see
// ServerEngine.documentFromObject); this is the fallback for the replay strategy and for
// versions no location record was filed for.
type deltaColdLoader struct {
	reader storage.DeltaSegmentReader
	index  *deltaindex.Index
}

func newDeltaColdLoader(reader storage.DeltaSegmentReader) *deltaColdLoader {
	l := &deltaColdLoader{reader: reader}
	l.index = deltaindex.New(reader, deltaindex.Options{Kind: "versions", Keys: versionKeys})
	return l
}

// versionKeys reports the content hash of every version a commit wrote. documentFromPatch keeps
// the patch text verbatim whether or not it parses, so the version's content hash is the hash of
// (DocID, Patch) - no need to build the document, or to validate its JSON, just to hash it.
func versionKeys(c document.Commit, emit func(codec.Hash)) {
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	for _, op := range c.Operations {
		if w, isWrite := op.(document.WriteOp); isWrite {
			emit(hasher.Hash(w.DocID, w.Patch))
		}
	}
}

// load implements coldDocLoader. A version that is not in the log is
// reported as simply not found: that is what a miss meant when the version
// store never evicted, and no caller can tell the difference.
func (l *deltaColdLoader) load(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	loc, ok, err := l.index.Lookup(contentHash)
	if err != nil || !ok {
		return document.Document{}, false, err
	}
	segment := loc.Segment
	if dr, ok := l.reader.(storage.DeltaDocumentReader); ok {
		return readVersionFromFrame(dr, segment, loc.FrameOffset, docID, contentHash)
	}
	streamer, ok := l.reader.(storage.DeltaCommitStreamer)
	if !ok {
		return document.Document{}, false, nil
	}
	commit, err := streamer.ReadCommitAt(segment, loc.FrameOffset)
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
