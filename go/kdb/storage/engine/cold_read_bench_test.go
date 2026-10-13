package engine

// Cold-read measurements shared by the cold-loader tests and the A/B in
// docs/benchmarks/2026-10-13-read-path-memory.md. Kept free of anything the
// loader's internals name, so the same file runs against older trees.

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const coldTestNS = "app/cold"

// coldLogVersion is one document version written to a test log, with where it was written.
type coldLogVersion struct {
	docID codec.UUID
	text  string
	seq   int64
	off   int64
}

// writeColdLog writes commits across several delta segments through the real writer and
// reader, rotating at a small cap, and returns every version it wrote in log order. Some
// versions repeat earlier content in a later segment (the index must name the most recent frame),
// some commits write several documents or the same document twice, some patches are not valid
// JSON (kept verbatim, as replay keeps them), and some ops are deletes (not indexed).
func writeColdLog(tb testing.TB, shim storage.PlatformIOShim, comp storage.CompressionCodec, commits int, seed int64) (*delta.DefaultReader, []coldLogVersion) {
	tb.Helper()
	cfg := storage.StorageEngineConfig{IOShim: shim, CompressionCodec: comp, DeltaMaxSegmentBytes: 4 << 10}
	factory := delta.Factory{Config: cfg}
	w, err := factory.OpenWriter(coldTestNS)
	if err != nil {
		tb.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed))
	docs := make([]codec.UUID, 40)
	for i := range docs {
		docs[i] = codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
	}
	var versions []coldLogVersion
	var parent *codec.Hash
	for i := 0; i < commits; i++ {
		var ops []document.Op
		var pending []coldLogVersion
		for j := 0; j < 1+rng.Intn(3); j++ {
			id := docs[rng.Intn(len(docs))]
			var text string
			switch r := rng.Intn(10); {
			case r == 0:
				ops = append(ops, document.DeleteOp{DocID: id})
				continue
			case r == 1:
				text = fmt.Sprintf("not json %d", rng.Intn(5))
			case r < 4 && len(versions) > 0:
				// Re-write content seen before (same doc, same text), possibly in a later segment.
				prev := versions[rng.Intn(len(versions))]
				id, text = prev.docID, prev.text
			default:
				text = fmt.Sprintf(`{"i":%d,"j":%d,"pad":"%s"}`, i, j, string(make([]byte, rng.Intn(64))))
			}
			ops = append(ops, document.WriteOp{DocID: id, Patch: text})
			pending = append(pending, coldLogVersion{docID: id, text: text})
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
		seq := w.SequenceNumber()
		off, err := w.Append(storage.DeltaRecord{CommitHash: h, NamespaceID: coldTestNS, CommitPayload: payload})
		if err != nil {
			tb.Fatal(err)
		}
		for _, v := range pending {
			v.seq, v.off = seq, off
			versions = append(versions, v)
		}
		if _, err := w.RotateIfNeeded(); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		tb.Fatal(err)
	}
	return factory.OpenReader(coldTestNS), versions
}

func osShim(tb testing.TB) storage.PlatformIOShim {
	tb.Helper()
	root := tb.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	return storio.NewFileBackedPlatformIO(storio.DefaultPlatformIOConfig(), store)
}

// TestColdIndexHeapPerVersion reports what the cold-frame index holds per indexed version, live
// after GC. It asserts only a loose ceiling; the number itself is the measurement.
func TestColdIndexHeapPerVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a 60k-commit log")
	}
	reader, _ := writeColdLog(t, storio.NewInMemoryPlatformIO(), storage.CompressionZSTD, 60_000, 3)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	l := newDeltaColdLoader(reader)
	var unknown codec.Hash
	unknown.Bytes[0] = 0xEE
	if _, _, err := l.load(codec.UUID{}, unknown); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	versions := len(l.frame)
	perVersion := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(versions)
	t.Logf("cold index: %d versions, %.1f live heap bytes/version, %d heap objects/version*1000",
		versions, perVersion, (int64(after.HeapObjects)-int64(before.HeapObjects))*1000/int64(versions))
	runtime.KeepAlive(l)
	if perVersion > 1024 {
		t.Fatalf("cold index holds %.0f bytes per version", perVersion)
	}
}

// BenchmarkColdIndexBuild measures the first-miss pass that indexes the whole log.
func BenchmarkColdIndexBuild(b *testing.B) {
	reader, _ := writeColdLog(b, storio.NewInMemoryPlatformIO(), storage.CompressionZSTD, 5_000, 3)
	var unknown codec.Hash
	unknown.Bytes[0] = 0xEE
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := newDeltaColdLoader(reader)
		if _, _, err := l.load(codec.UUID{}, unknown); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkColdLoad reads random historical versions back through the cold loader from an
// on-disk log: one frame read, CRC, zstd decode, commit decode and content-hash check per op.
func BenchmarkColdLoad(b *testing.B) {
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		b.Run(fmt.Sprintf("codec-%d", comp), func(b *testing.B) {
			reader, versions := writeColdLog(b, osShim(b), comp, 5_000, 3)
			l := newDeltaColdLoader(reader)
			type want struct {
				id codec.UUID
				h  codec.Hash
			}
			wants := make([]want, len(versions))
			for i, v := range versions {
				h, err := document.Document{ID: v.docID, JSON: v.text}.ContentHash()
				if err != nil {
					b.Fatal(err)
				}
				wants[i] = want{v.docID, h}
			}
			rand.New(rand.NewSource(1)).Shuffle(len(wants), func(i, j int) { wants[i], wants[j] = wants[j], wants[i] })
			if _, ok, err := l.load(wants[0].id, wants[0].h); !ok || err != nil {
				b.Fatalf("warm-up load: ok=%v err=%v", ok, err)
			}
			b.Run("serial", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					w := wants[i%len(wants)]
					if _, ok, err := l.load(w.id, w.h); !ok || err != nil {
						b.Fatalf("load: ok=%v err=%v", ok, err)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := rand.Intn(len(wants))
					for pb.Next() {
						w := wants[i%len(wants)]
						if _, ok, err := l.load(w.id, w.h); !ok || err != nil {
							b.Errorf("load: ok=%v err=%v", ok, err)
							return
						}
						i++
					}
				})
			})
		})
	}
}
