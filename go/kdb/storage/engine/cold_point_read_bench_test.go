package engine

// Cold point-read measurements for docs/benchmarks/2026-10-13-cold-read-frames.md: one historical
// version, evicted from the version store, read back through deltaColdLoader from an on-disk delta
// log. Kept free of anything the loader's internals name, so the same file runs against older
// trees for the A/B.

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// countingShim counts the segment bytes read through it. It forwards the optional interfaces the
// delta reader looks for, so wrapping a shim does not change which read path is taken.
type countingShim struct {
	storage.PlatformIOShim
	read atomic.Int64
}

func (s *countingShim) ReadFromSegment(name string, offset int64, length int) ([]byte, error) {
	b, err := s.PlatformIOShim.ReadFromSegment(name, offset, length)
	s.read.Add(int64(len(b)))
	return b, err
}

func (s *countingShim) ReadFromSegmentInto(name string, offset int64, dst []byte) (int, error) {
	n, err := s.PlatformIOShim.(storage.SegmentIntoReader).ReadFromSegmentInto(name, offset, dst)
	s.read.Add(int64(n))
	return n, err
}

func (s *countingShim) SegmentStamp(name string) (storage.SegmentStamp, bool, error) {
	if st, ok := s.PlatformIOShim.(storage.SegmentStamper); ok {
		return st.SegmentStamp(name)
	}
	return storage.SegmentStamp{}, false, nil
}

func (s *countingShim) SegmentSize(name string) (int64, error) {
	if sz, ok := s.PlatformIOShim.(storage.SegmentSizer); ok {
		return sz.SegmentSize(name)
	}
	return 0, storage.ErrSegmentSizeUnknown
}

// SegmentGeneration forwards to the wrapped shim's, when it has one (newer trees use it to key
// cached frames). Declared with a local interface so this file compiles against trees without it.
func (s *countingShim) SegmentGeneration(name string) (uint64, bool) {
	type generationer interface {
		SegmentGeneration(string) (uint64, bool)
	}
	if g, ok := s.PlatformIOShim.(generationer); ok {
		return g.SegmentGeneration(name)
	}
	return 0, false
}

type coldPointWant struct {
	id codec.UUID
	h  codec.Hash
}

// writePointLog writes commits of docsPerCommit documents each (~500-byte JSON bodies) through the
// real delta writer, zstd-compressed, and returns one want per version written.
func writePointLog(tb testing.TB, shim storage.PlatformIOShim, commits, docsPerCommit int) (*delta.DefaultReader, []coldPointWant) {
	tb.Helper()
	cfg := storage.StorageEngineConfig{IOShim: shim, CompressionCodec: storage.CompressionZSTD}
	factory := delta.Factory{Config: cfg}
	w, err := factory.OpenWriter(coldTestNS)
	if err != nil {
		tb.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	var wants []coldPointWant
	var parent *codec.Hash
	words := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	for i := 0; i < commits; i++ {
		ops := make([]document.Op, 0, docsPerCommit)
		for j := 0; j < docsPerCommit; j++ {
			id := codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
			text := fmt.Sprintf(`{"n":%d,"j":%d,"name":"%s %s","tags":["%s","%s"],"score":%.4f,"note":"%x"}`,
				i, j, words[rng.Intn(8)], words[rng.Intn(8)], words[rng.Intn(8)], words[rng.Intn(8)],
				rng.Float64(), rng.Int63())
			for len(text) < 500 {
				text = text[:len(text)-1] + fmt.Sprintf(`,"k%d":"%s-%x"}`, len(text), words[rng.Intn(8)], rng.Int31())
			}
			ops = append(ops, document.WriteOp{DocID: id, Patch: text})
			h, err := document.Document{ID: id, JSON: text}.ContentHash()
			if err != nil {
				tb.Fatal(err)
			}
			wants = append(wants, coldPointWant{id, h})
		}
		var parents []codec.Hash
		if parent != nil {
			parents = []codec.Hash{*parent}
		}
		c := document.Commit{
			ParentHashes: parents, NamespaceID: coldTestNS, Timestamp: codec.Timestamp{EpochMillis: int64(i)},
			Operations: ops, DocumentTreeHash: document.EmptyDocumentTree().TreeHash,
		}
		h, err := document.ComputeCommitHash(c)
		if err != nil {
			tb.Fatal(err)
		}
		c.Hash = h
		parent = &h
		payload, err := c.ToPayloadBytes()
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := w.Append(storage.DeltaRecord{CommitHash: h, NamespaceID: coldTestNS, CommitPayload: payload}); err != nil {
			tb.Fatal(err)
		}
		if _, err := w.RotateIfNeeded(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		tb.Fatal(err)
	}
	return factory.OpenReader(coldTestNS), wants
}

// BenchmarkColdPointRead reads random historical versions through the cold loader from an on-disk
// log, with no frame cache in front of it: every read is a frame read, a CRC, a zstd decode and a
// decode of what the loader needs from the commit. docs-1 is one document per commit, docs-100 a
// hundred. Reports segment bytes read per cold read as read-B/op.
func BenchmarkColdPointRead(b *testing.B) {
	for _, shape := range []struct{ commits, docs int }{{4000, 1}, {200, 100}} {
		b.Run(fmt.Sprintf("docs-%d", shape.docs), func(b *testing.B) {
			shim := &countingShim{PlatformIOShim: osShim(b)}
			reader, wants := writePointLog(b, shim, shape.commits, shape.docs)
			rand.New(rand.NewSource(1)).Shuffle(len(wants), func(i, j int) { wants[i], wants[j] = wants[j], wants[i] })
			l := newDeltaColdLoader(reader)
			if _, ok, err := l.load(wants[0].id, wants[0].h); !ok || err != nil {
				b.Fatalf("warm-up load: ok=%v err=%v", ok, err)
			}
			b.ReportAllocs()
			shim.read.Store(0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := wants[i%len(wants)]
				doc, ok, err := l.load(w.id, w.h)
				if !ok || err != nil || doc.ID != w.id {
					b.Fatalf("load: ok=%v err=%v", ok, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(shim.read.Load())/float64(b.N), "read-B/op")
		})
	}
}
