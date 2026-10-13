package engine

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// scanWindowDocs is how many tree entries a scan resolves together. It bounds the bodies a scan
// holds beyond its batch buffer; see prefetchWindow for why a window rather than one entry.
const scanWindowDocs = 32768

type scanEntry struct {
	id codec.UUID
	h  codec.Hash
}

type frameKey struct{ seq, offset int64 }

// prefetchWindow loads the documents of a window of tree entries that are not already cached, and
// returns them keyed by content hash.
//
// Under the objects strategy a version is stored as a pointer into the delta log, and reading one
// decodes the whole commit frame it lives in. A scan walks the tree in id order, which has nothing
// to do with commit order, so reading entry by entry decoded every frame once for every document
// in it: a namespace loaded in 1,000-document commits cost 1,000 times the decoding it needed, and
// the first scan after a restart (nothing cached) took a minute at 100k documents and the better
// part of half an hour at 1M. Resolving the window's locations first and decoding each distinct
// frame once turns that into one decode per frame per window.
//
// Anything it cannot resolve here is simply absent from the result; the caller falls back to the
// per-document path, so this changes only how fast a document is found, never whether.
func (e *ServerEngine) prefetchWindow(window []scanEntry) map[codec.Hash]document.Document {
	reader := e.frameReader.Load()
	if reader == nil || e.memTable == nil {
		return nil
	}
	type want struct {
		id codec.UUID
		h  codec.Hash
	}
	byFrame := make(map[frameKey][]want)
	for _, entry := range window {
		if e.docsByHash.Contains(entry.h) {
			continue
		}
		raw := e.memTable.Get(entry.h)
		if raw == nil {
			continue
		}
		seq, offset, ok := decodeDocumentLocation(raw)
		if !ok {
			continue
		}
		k := frameKey{seq, offset}
		byFrame[k] = append(byFrame[k], want{entry.id, entry.h})
	}
	if len(byFrame) == 0 {
		return nil
	}
	// Frame order, so the log is read front to back.
	keys := make([]frameKey, 0, len(byFrame))
	for k := range byFrame {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].seq != keys[j].seq {
			return keys[i].seq < keys[j].seq
		}
		return keys[i].offset < keys[j].offset
	})
	out := make(map[codec.Hash]document.Document)
	for _, k := range keys {
		commit, err := (*reader)(k.seq, k.offset)
		if err != nil {
			continue
		}
		wanted := byFrame[k]
		// One pass over the frame's operations, matched to what the window asked of it.
		pending := make(map[codec.UUID][]codec.Hash, len(wanted))
		for _, w := range wanted {
			pending[w.id] = append(pending[w.id], w.h)
		}
		for _, op := range commit.Operations {
			wr, isWrite := op.(document.WriteOp)
			if !isWrite {
				continue
			}
			hashes := pending[wr.DocID]
			for _, h := range hashes {
				if _, done := out[h]; done {
					continue
				}
				if doc, ok := verifiedDocument(wr.DocID, wr.Patch, h); ok {
					out[h] = doc
				}
			}
		}
	}
	return out
}
