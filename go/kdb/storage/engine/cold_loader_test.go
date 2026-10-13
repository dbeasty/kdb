package engine

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// TestColdLoaderIndexMatchesTheLog checks the compact index against a reference built the way
// the index used to be built - document.ContentHash per version, last write wins - over a log of
// several segments: every version resolves to the same (segment, frame) the reference names, and
// load returns exactly the document that was written.
func TestColdLoaderIndexMatchesTheLog(t *testing.T) {
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		t.Run(fmt.Sprintf("codec-%d", comp), func(t *testing.T) {
			reader, versions := writeColdLog(t, storio.NewInMemoryPlatformIO(), comp, 400, 7)
			segs, err := reader.ListSegments()
			if err != nil {
				t.Fatal(err)
			}
			if len(segs) < 4 {
				t.Fatalf("log has %d segments; the test needs several", len(segs))
			}
			type loc struct{ seq, off int64 }
			want := map[codec.Hash]loc{}
			text := map[codec.Hash]coldLogVersion{}
			for _, v := range versions {
				h, err := document.Document{ID: v.docID, JSON: v.text}.ContentHash()
				if err != nil {
					t.Fatal(err)
				}
				want[h] = loc{v.seq, v.off}
				text[h] = v
			}

			l := newDeltaColdLoader(reader)
			var unknown codec.Hash
			unknown.Bytes[0] = 0xEE
			if _, ok, err := l.load(versions[0].docID, unknown); ok || err != nil {
				t.Fatalf("unknown hash: ok=%v err=%v", ok, err)
			}
			if len(l.frame) != len(want) {
				t.Fatalf("index holds %d versions, reference %d", len(l.frame), len(want))
			}
			for h, w := range want {
				ref, ok := l.frame[h]
				if !ok {
					t.Fatalf("version %s not indexed", h.Hex())
				}
				if got := (loc{l.segments[ref.segment].SequenceNumber, ref.frameOffset}); got != w {
					t.Fatalf("version %s indexed at %+v, reference %+v", h.Hex(), got, w)
				}
				v := text[h]
				doc, ok, err := l.load(v.docID, h)
				if err != nil || !ok {
					t.Fatalf("load %s: ok=%v err=%v", h.Hex(), ok, err)
				}
				if doc.ID != v.docID || doc.JSON != v.text {
					t.Fatalf("load %s returned %v %q, want %v %q", h.Hex(), doc.ID, doc.JSON, v.docID, v.text)
				}
				// A version asked for under the wrong document is not found, not mis-served.
				if _, ok, _ := l.load(codec.UUID{MSB: 1, LSB: 1}, h); ok {
					t.Fatalf("version %s served for a document that never wrote it", h.Hex())
				}
			}
		})
	}
}

// TestColdLoaderCatchesUpWithTheLog: versions written after the index was first built - in new
// segments - are found on a later miss. The index used to be built once, so such a version read as
// missing once evicted: harmless while live versions never left memory, a lost read once they can
// (bounded live bodies).
func TestColdLoaderCatchesUpWithTheLog(t *testing.T) {
	shim := storio.NewInMemoryPlatformIO()
	reader, first := writeColdLog(t, shim, storage.CompressionNone, 60, 11)
	l := newDeltaColdLoader(reader)
	h0, err := document.Document{ID: first[0].docID, JSON: first[0].text}.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := l.load(first[0].docID, h0); !ok || err != nil {
		t.Fatalf("a version from the first batch: ok=%v err=%v", ok, err)
	}
	before := len(l.segments)
	_, later := writeColdLog(t, shim, storage.CompressionNone, 60, 12)
	for i, v := range later {
		h, err := document.Document{ID: v.docID, JSON: v.text}.ContentHash()
		if err != nil {
			t.Fatal(err)
		}
		doc, ok, err := l.load(v.docID, h)
		if err != nil || !ok {
			t.Fatalf("version %d, written after the index was built: ok=%v err=%v", i, ok, err)
		}
		if doc.JSON != v.text {
			t.Fatalf("version %d read back wrong", i)
		}
	}
	if len(l.segments) <= before {
		t.Fatalf("segment table did not grow: %d segments before, %d after", before, len(l.segments))
	}
	// A miss with nothing new in the log scans nothing: the indexed sizes all match the listing.
	var unknown codec.Hash
	unknown.Bytes[0] = 0xAB
	if _, ok, err := l.load(first[0].docID, unknown); ok || err != nil {
		t.Fatalf("unknown hash: ok=%v err=%v", ok, err)
	}
	segs, _ := reader.ListSegments()
	for _, s := range segs {
		if l.indexed[s.SequenceNumber] != s.SizeBytes {
			t.Fatalf("segment %d indexed through %d of %d bytes", s.SequenceNumber, l.indexed[s.SequenceNumber], s.SizeBytes)
		}
	}
}
