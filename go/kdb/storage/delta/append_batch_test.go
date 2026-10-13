package delta

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// appendCountingShim counts AppendToSegment calls, can cap appends like the file-backed shim does, and
// can fail the nth append.
type appendCountingShim struct {
	storage.PlatformIOShim
	appends int
	limit   int
	failAt  int // 1-based append call to fail; 0 never
}

func (s *appendCountingShim) AppendToSegment(name string, b []byte) (int64, error) {
	s.appends++
	if s.failAt > 0 && s.appends == s.failAt {
		return 0, errors.New("injected append failure")
	}
	return s.PlatformIOShim.AppendToSegment(name, b)
}

func (s *appendCountingShim) MaxAppendBytes() int { return s.limit }

func segmentBytes(t *testing.T, w *DefaultWriter) []byte {
	t.Helper()
	// Ask for more than can be there: the shim clamps a read to the segment's end, so this is
	// what is actually on disk, independent of what the writer believes its size is.
	b, err := w.shim.ReadFromSegment(w.segmentName, 0, 1<<24)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newWriter(t *testing.T, shim storage.PlatformIOShim, comp storage.CompressionCodec) *DefaultWriter {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := newConfig(shim, comp)
	return NewDefaultWriter(testNS, id, 0, shim, cfg)
}

// TestAppendBatchWritesWhatAppendWrites: a segment written in batches is byte-for-byte the segment
// one Append per record writes, every record at the offset Append reports - so every reader, the
// torn-tail scan and the Kotlin implementation see no difference.
func TestAppendBatchWritesWhatAppendWrites(t *testing.T) {
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		chain := buildChain(t, 300)
		one := newWriter(t, newTestShim(t), comp)
		var want []int64
		for _, c := range chain {
			off, err := one.Append(record(t, c))
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, off)
		}
		shim := &appendCountingShim{PlatformIOShim: newTestShim(t)}
		batched := newWriter(t, shim, comp)
		rng := rand.New(rand.NewSource(1))
		var got []int64
		batches := 0
		for i := 0; i < len(chain); {
			n := 1 + rng.Intn(50)
			if i+n > len(chain) {
				n = len(chain) - i
			}
			var recs []storage.DeltaRecord
			for _, c := range chain[i : i+n] {
				recs = append(recs, record(t, c))
			}
			offs, err := batched.AppendBatch(recs)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, offs...)
			i += n
			batches++
		}
		if !bytes.Equal(segmentBytes(t, one), segmentBytes(t, batched)) {
			t.Fatalf("codec %v: batched segment differs from the one Append writes", comp)
		}
		if len(got) != len(want) {
			t.Fatalf("codec %v: %d offsets, want %d", comp, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("codec %v: record %d at offset %d, want %d", comp, i, got[i], want[i])
			}
		}
		if shim.appends != batches {
			t.Fatalf("codec %v: %d writes for %d batches, want one each", comp, shim.appends, batches)
		}
		if batched.CurrentSizeBytes() != one.CurrentSizeBytes() {
			t.Fatalf("codec %v: size %d, want %d", comp, batched.CurrentSizeBytes(), one.CurrentSizeBytes())
		}
	}
}

// TestAppendBatchSplitsAtTheAppendCap: a batch larger than the platform's append cap goes out in
// several writes, none over the cap, with the same bytes and offsets.
func TestAppendBatchSplitsAtTheAppendCap(t *testing.T) {
	chain := buildChain(t, 40)
	one := newWriter(t, newTestShim(t), storage.CompressionNone)
	for _, c := range chain {
		if _, err := one.Append(record(t, c)); err != nil {
			t.Fatal(err)
		}
	}
	frame := len(rawFrame(t, chain[0], storage.CompressionNone))
	shim := &appendCountingShim{PlatformIOShim: newTestShim(t), limit: 3*frame + frame/2}
	batched := newWriter(t, shim, storage.CompressionNone)
	var recs []storage.DeltaRecord
	for _, c := range chain {
		recs = append(recs, record(t, c))
	}
	offs, err := batched.AppendBatch(recs)
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != len(chain) {
		t.Fatalf("%d offsets for %d records", len(offs), len(chain))
	}
	if shim.appends < 10 || shim.appends > 20 {
		t.Fatalf("expected ~14 capped writes of 3 frames, got %d", shim.appends)
	}
	if !bytes.Equal(segmentBytes(t, one), segmentBytes(t, batched)) {
		t.Fatal("capped batched segment differs from the one Append writes")
	}
}

// TestAppendBatchFailureReportsTheWrittenPrefix: when a write fails part-way, AppendBatch reports
// the offsets of exactly the records that reached the segment, the writer's size matches what is
// on disk, and the segment reads back as those records and nothing else.
func TestAppendBatchFailureReportsTheWrittenPrefix(t *testing.T) {
	chain := buildChain(t, 20)
	frame := len(rawFrame(t, chain[0], storage.CompressionNone))
	shim := &appendCountingShim{PlatformIOShim: newTestShim(t), limit: 4 * frame, failAt: 3}
	w := newWriter(t, shim, storage.CompressionNone)
	var recs []storage.DeltaRecord
	for _, c := range chain {
		recs = append(recs, record(t, c))
	}
	offs, err := w.AppendBatch(recs)
	if err == nil {
		t.Fatal("expected the injected failure")
	}
	if len(offs) == 0 || len(offs) >= len(chain) {
		t.Fatalf("expected the records of the two writes that succeeded, got %d offsets", len(offs))
	}
	// The reference: the same records, one Append each.
	ref := newWriter(t, newTestShim(t), storage.CompressionNone)
	var refOffs []int64
	for _, c := range chain {
		off, err := ref.Append(record(t, c))
		if err != nil {
			t.Fatal(err)
		}
		refOffs = append(refOffs, off)
	}
	end := ref.CurrentSizeBytes()
	if len(offs) < len(refOffs) {
		end = refOffs[len(offs)]
	}
	got := segmentBytes(t, w)
	if int64(len(got)) != w.CurrentSizeBytes() || w.CurrentSizeBytes() != end {
		t.Fatalf("writer size %d, segment holds %d, the %d reported records end at %d", w.CurrentSizeBytes(), len(got), len(offs), end)
	}
	if !bytes.Equal(got, segmentBytes(t, ref)[:end]) {
		t.Fatal("the segment does not hold exactly the reported records")
	}
	for i := range offs {
		if offs[i] != refOffs[i] {
			t.Fatalf("record %d reported at %d, want %d", i, offs[i], refOffs[i])
		}
	}
}

func TestAppendBatchRefusedWhenSealed(t *testing.T) {
	w := newWriter(t, newTestShim(t), storage.CompressionNone)
	if _, err := w.Seal(); err != nil {
		t.Fatal(err)
	}
	if offs, err := w.AppendBatch([]storage.DeltaRecord{record(t, buildChain(t, 1)[0])}); err == nil || len(offs) != 0 {
		t.Fatalf("a sealed segment took a batch: %v %v", offs, err)
	}
}
