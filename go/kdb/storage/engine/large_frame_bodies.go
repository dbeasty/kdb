package engine

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Bodies of large commits, filed by value (qtask production plan U4).
//
// The objects strategy files a location record per version - "in segment S, the frame at
// offset O" - and a cold read follows it: read the frame, decompress it, pick the version out.
// That is the right trade for an ordinary commit of a few documents. It is the wrong one for a
// bulk write: a migration's import commits hold hundreds of documents in one multi-megabyte
// frame, the frame cache cannot hold frames that size, and every later read of any one of
// those documents decompressed the whole frame again (a 200-document page read cost 200 frame
// decompressions; 98% of the CPU of a qtask task list).
//
// So a commit whose bodies add up to StorageEngineConfig.LargeCommitBodyBytes files each body
// itself under its content hash - the record shape documentFromObject has always accepted -
// and a cold read finds the document alone, one SSTable block, no frame. Versions filed by
// location before this (or by a replay, which files locations) are re-filed by value the first
// time a read meets their large frame: that read decompresses the frame once and files every
// version in it, so the frame is never decompressed for them again.
//
// Correctness never rests on this. Every record is checked against the content hash it is
// filed under, and a missing record falls back to the location or the scan as before.

// fileBodiesOfLargeCommit files the bodies of a commit's changed versions by value when they add
// up to the large-commit threshold, and reports whether it did (the caller files locations
// otherwise). Bodies come from docsByHash, which pins every version until its commit is logged,
// so all of them are found; one that is not makes the whole commit fall back to locations.
func (e *ServerEngine) fileBodiesOfLargeCommit(changed []TreeChange) bool {
	threshold := e.config.ResolvedLargeCommitBodyBytes()
	if threshold <= 0 || e.memTable == nil || e.HistoryStrategy() != storage.HistoryStrategyObjects {
		return false
	}
	docs := make([]document.Document, len(changed))
	var total int64
	for i, c := range changed {
		d, ok := e.docsByHash.Get(c.ContentHash)
		if !ok {
			return false
		}
		docs[i] = d
		total += int64(len(d.JSON))
	}
	if total < threshold {
		return false
	}
	for i, c := range changed {
		e.memTable.Put(c.ContentHash, []byte(docs[i].JSON))
	}
	e.largeCommitBodies.Add(int64(len(changed)))
	return true
}

// documentFromLargeFrame reads a version through its location record when the frame it points
// at is large, and files every version in that frame by value on the way. handled=false leaves
// the read to documentFromLog: a small frame, or a reader that cannot hand over the payload.
func (e *ServerEngine) documentFromLargeFrame(docID codec.UUID, contentHash codec.Hash, segmentSeq, frameOffset int64) (doc document.Document, found, handled bool) {
	threshold := e.config.ResolvedLargeCommitBodyBytes()
	if threshold <= 0 {
		return document.Document{}, false, false
	}
	dr := e.frameReads.Load()
	if dr == nil {
		return document.Document{}, false, false
	}
	pr, ok := dr.reader.(storage.DeltaPayloadReader)
	if !ok {
		return document.Document{}, false, false
	}
	hasher := contentHashers.Get().(*document.ContentHasher)
	defer contentHashers.Put(hasher)
	err := pr.ReadPayloadAt(storage.DeltaSegmentRef{NamespaceID: dr.namespaceID, SequenceNumber: segmentSeq}, frameOffset, func(payload []byte) error {
		large := int64(len(payload)) >= threshold
		return document.ScanAllCommitWrites(payload, func(id codec.UUID, patch []byte) bool {
			if !large && (found || id != docID) {
				// A small frame is read as before: only the version asked for is hashed.
				return !found
			}
			h := hasher.HashBytes(id, patch)
			if !found && id == docID && h == contentHash {
				doc, found = documentFromPatch(docID, string(patch)), true
			}
			if large {
				// Copied: patch aliases the payload, which the frame cache recycles.
				e.memTable.Put(h, []byte(string(patch)))
				e.largeFrameRefiled.Add(1)
			}
			return true
		})
	})
	if err != nil {
		return document.Document{}, false, false
	}
	return doc, found, true
}

// LargeCommitBodiesFiled is how many versions were filed by value when their commit was written,
// and LargeFrameBodiesRefiled how many were re-filed by value by a cold read that met their large
// frame. For tests and reporting.
func (e *ServerEngine) LargeCommitBodiesFiled() int64  { return e.largeCommitBodies.Load() }
func (e *ServerEngine) LargeFrameBodiesRefiled() int64 { return e.largeFrameRefiled.Load() }
