package delta

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// N5: a segment's listing says where its intact frames stop and what follows them
// (storage.TailKind), so every consumer applies one rule. A torn tail is bytes at the very end
// of a segment with nothing intact after them; damage that an intact frame follows is not one.
// Zero bytes after the last frame are a preallocated segment's unused space: the end.

// segmentFrames returns where each frame of a segment written by writeChainAcross starts.
func segmentFrames(t *testing.T, raw []byte) []int {
	t.Helper()
	var out []int
	for off := 0; off+frameHeaderSize <= len(raw) && isKdbpFrame(raw, off); {
		end, ok := frameBounds(raw, off)
		if !ok {
			break
		}
		out = append(out, off)
		off = end
	}
	return out
}

func readSegment(t *testing.T, shim storage.PlatformIOShim, seq int64) []byte {
	t.Helper()
	raw, err := shim.ReadFromSegment(segName(seq), 0, 1<<26)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func segName(seq int64) string {
	return fmt.Sprintf("ns/%s/delta/%020d.seg", testNS, seq)
}

func listedRef(t *testing.T, shim storage.PlatformIOShim, cfg storage.StorageEngineConfig, seq int64) storage.DeltaSegmentRef {
	t.Helper()
	refs, err := NewDefaultReader(testNS, shim, cfg).ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	for _, r := range refs {
		if r.SequenceNumber == seq {
			return r
		}
	}
	t.Fatalf("segment %d not listed", seq)
	return storage.DeltaSegmentRef{}
}

func TestListSegmentsClassifiesHowASegmentEnds(t *testing.T) {
	type expect struct {
		kind     storage.TailKind
		stopAt   int // index of the frame the intact run stops at; -1 = the end of the frames
		nextAt   int // index of the next intact frame for TailDamaged
		lastGood int // index of the last commit the ref reports
	}
	cases := []struct {
		name string
		edit func(raw []byte, frames []int) []byte
		want expect
	}{
		{"clean", func(raw []byte, _ []int) []byte { return raw },
			expect{storage.TailClean, -1, -1, 3}},
		{"zero padding", func(raw []byte, _ []int) []byte { return append(raw, make([]byte, 3<<20)...) },
			expect{storage.TailClean, -1, -1, 3}},
		{"torn: cut short", func(raw []byte, _ []int) []byte { return raw[:len(raw)-5] },
			expect{storage.TailTorn, 3, -1, 2}},
		{"torn: body never written, then padding", func(raw []byte, f []int) []byte {
			for i := (f[3] + len(raw)) / 2; i < len(raw); i++ {
				raw[i] = 0
			}
			return append(raw, make([]byte, 1<<20)...)
		}, expect{storage.TailTorn, 3, -1, 2}},
		{"torn: only the magic written", func(raw []byte, f []int) []byte {
			for i := f[3] + 4; i < len(raw); i++ {
				raw[i] = 0
			}
			return raw
		}, expect{storage.TailTorn, 3, -1, 2}},
		{"damaged: garbled header mid-segment", func(raw []byte, f []int) []byte { raw[f[1]] ^= 0xFF; return raw },
			expect{storage.TailDamaged, 1, 2, 0}},
		{"damaged: body flipped mid-segment", func(raw []byte, f []int) []byte { raw[f[1]+frameHeaderSize+2] ^= 0x01; return raw },
			expect{storage.TailDamaged, 1, 2, 0}},
		{"damaged: two frames, intact after both", func(raw []byte, f []int) []byte {
			raw[f[1]] ^= 0xFF
			raw[f[2]+frameHeaderSize+2] ^= 0x01
			return raw
		}, expect{storage.TailDamaged, 1, 3, 0}},
		{"damaged: then zero padding", func(raw []byte, f []int) []byte {
			raw[f[1]] ^= 0xFF
			return append(raw, make([]byte, 1<<20)...)
		}, expect{storage.TailDamaged, 1, 2, 0}},
	}
	for _, tc := range cases {
		for _, seq := range []int64{0, 1} { // an older segment, and the newest
			t.Run(fmt.Sprintf("%s/segment-%d", tc.name, seq), func(t *testing.T) {
				shim := newTestShim(t)
				cfg := newConfig(shim, storage.CompressionNone)
				parts := writeChainAcross(t, cfg, []int64{0, 1}, 4)
				raw := readSegment(t, shim, seq)
				frames := segmentFrames(t, raw)
				rewriteSegmentBytes(t, shim, seq, tc.edit(append([]byte(nil), raw...), frames))

				ref := listedRef(t, shim, cfg, seq)
				if ref.Tail.Kind != tc.want.kind {
					t.Fatalf("tail %v (%+v), want %v", ref.Tail.Kind, ref.Tail, tc.want.kind)
				}
				stop := len(raw)
				if tc.want.stopAt >= 0 {
					stop = frames[tc.want.stopAt]
				}
				if ref.SizeBytes != int64(stop) || ref.Tail.Offset != int64(stop) {
					t.Fatalf("intact frames end at %d (tail %d), want %d", ref.SizeBytes, ref.Tail.Offset, stop)
				}
				if tc.want.kind == storage.TailDamaged && ref.Tail.NextIntactFrame != int64(frames[tc.want.nextAt]) {
					t.Fatalf("next intact frame at %d, want %d", ref.Tail.NextIntactFrame, frames[tc.want.nextAt])
				}
				if ref.LastCommitHash != parts[seq][tc.want.lastGood].Hash {
					t.Fatalf("last commit is not commit %d", tc.want.lastGood)
				}
				// The whole-segment scan agrees with the windowed listing.
				_, tail := ScanSegment(readSegment(t, shim, seq))
				if tail.Kind != ref.Tail.Kind || tail.Offset != ref.Tail.Offset {
					t.Fatalf("ScanSegment says %+v, ListSegments %+v", tail, ref.Tail)
				}
			})
		}
	}
}

// rewriteSegmentBytes replaces segment seq's bytes with raw.
func rewriteSegmentBytes(t *testing.T, shim storage.PlatformIOShim, seq int64, raw []byte) {
	t.Helper()
	if err := shim.DeleteSegment(segName(seq)); err != nil {
		t.Fatal(err)
	}
	if _, err := shim.AppendToSegment(segName(seq), raw); err != nil {
		t.Fatal(err)
	}
}

// bigChain builds n chained commits each carrying about size bytes, so their frames straddle the
// windows ListSegments reads a segment in.
func bigChain(t *testing.T, n, size int) []document.Commit {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	letters := "abcdefghijklmnopqrstuvwxyz0123456789"
	var out []document.Commit
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, parent)
		var b strings.Builder
		for b.Len() < size {
			b.WriteByte(letters[rng.Intn(len(letters))])
		}
		w := c.Operations[0].(document.WriteOp)
		w.Patch = fmt.Sprintf(`{"blob":%q}`, b.String())
		c.Operations = []document.Op{w}
		h, err := document.ComputeCommitHash(c)
		if err != nil {
			t.Fatal(err)
		}
		c.Hash = h
		out = append(out, c)
		parent = &out[len(out)-1].Hash
	}
	return out
}

func TestListSegmentsClassifiesAcrossScanWindows(t *testing.T) {
	// ~300KB frames: several per 1MiB window, and frames cut by every window boundary.
	for _, tc := range []struct {
		name   string
		damage int // frame whose magic is flipped, or -1
		pad    int
		want   storage.TailKind
	}{
		{"clean", -1, 0, storage.TailClean},
		{"clean with padding", -1, 5 << 20, storage.TailClean},
		{"damaged past the first window", 6, 0, storage.TailDamaged},
		{"damaged past the first window, then padding", 6, 5 << 20, storage.TailDamaged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := newTestShim(t)
			cfg := newConfig(shim, storage.CompressionNone)
			commits := bigChain(t, 10, 300<<10)
			writeSealedSegment(t, cfg, 0, commits...)
			raw := readSegment(t, shim, 0)
			frames := segmentFrames(t, raw)
			if len(frames) != 10 {
				t.Fatalf("wrote 10 frames, found %d", len(frames))
			}
			edited := append([]byte(nil), raw...)
			if tc.damage >= 0 {
				edited[frames[tc.damage]] ^= 0xFF
			}
			edited = append(edited, make([]byte, tc.pad)...)
			rewriteSegmentBytes(t, shim, 0, edited)

			ref := listedRef(t, shim, cfg, 0)
			if ref.Tail.Kind != tc.want {
				t.Fatalf("tail %+v, want %v", ref.Tail, tc.want)
			}
			if tc.damage < 0 {
				if ref.SizeBytes != int64(len(raw)) || ref.LastCommitHash != commits[9].Hash {
					t.Fatalf("a clean segment of %d bytes listed as %d bytes", len(raw), ref.SizeBytes)
				}
				return
			}
			if ref.SizeBytes != int64(frames[tc.damage]) || ref.Tail.NextIntactFrame != int64(frames[tc.damage+1]) {
				t.Fatalf("tail %+v, want damage at %d with the next intact frame at %d", ref.Tail, frames[tc.damage], frames[tc.damage+1])
			}
		})
	}
}

func TestScanSegmentFramesCountsWhatFollowsAFault(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	writeChainAcross(t, cfg, []int64{0}, 4)
	raw := readSegment(t, shim, 0)
	frames := segmentFrames(t, raw)

	scan := func(edit func([]byte) []byte) error {
		return ScanSegmentFrames(edit(append([]byte(nil), raw...)), func(ScannedCommit) error { return nil })
	}
	var corrupt *CorruptFrameError

	// A garbled header used to end the walk as quietly as a torn tail.
	err := scan(func(b []byte) []byte { b[frames[1]] ^= 0xFF; return b })
	if !errors.As(err, &corrupt) || corrupt.Torn() || corrupt.IntactFramesAfter != 2 || corrupt.NextIntactFrame != frames[2] {
		t.Fatalf("garbled header mid-segment: %v (%+v), want a fault with 2 intact frames after it", err, corrupt)
	}
	err = scan(func(b []byte) []byte { b[frames[1]+frameHeaderSize+1] ^= 0x01; return b })
	if !errors.As(err, &corrupt) || corrupt.Torn() || corrupt.IntactFramesAfter != 2 {
		t.Fatalf("CRC mismatch mid-segment: %v, want a fault with 2 intact frames after it", err)
	}
	err = scan(func(b []byte) []byte { b[frames[3]+frameHeaderSize+1] ^= 0x01; return b })
	if !errors.As(err, &corrupt) || !corrupt.Torn() {
		t.Fatalf("CRC mismatch in the last frame: %v, want a torn tail", err)
	}
	err = scan(func(b []byte) []byte { b[frames[3]+5] = 0x7f; return b })
	if !errors.As(err, &corrupt) || corrupt.Torn() || !corrupt.Undecodable {
		t.Fatalf("an undecodable last frame: %v, want a fault that is not a torn tail", err)
	}
	if err := scan(func(b []byte) []byte { return b[:len(b)-5] }); err != nil {
		t.Fatalf("a short last frame is a torn tail and stops cleanly: %v", err)
	}
	if err := scan(func(b []byte) []byte {
		for i := frames[3] + 4; i < len(b); i++ {
			b[i] = 0
		}
		return b
	}); err != nil {
		t.Fatalf("a header torn after its magic is a torn tail, not a frame that will not decode: %v", err)
	}
	if err := scan(func(b []byte) []byte { return append(b, make([]byte, 4096)...) }); err != nil {
		t.Fatalf("zero padding is the end of the segment: %v", err)
	}
}

// SkimSegment, which checkpoint matching uses to tell damage in place from a replaced log, must
// read past a garbled header too; it used to stop there.
func TestSkimSegmentReadsPastAGarbledHeader(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	parts := writeChainAcross(t, cfg, []int64{0}, 4)
	raw := readSegment(t, shim, 0)
	frames := segmentFrames(t, raw)
	edited := append([]byte(nil), raw...)
	edited[frames[1]] ^= 0xFF
	rewriteSegmentBytes(t, shim, 0, append(edited, make([]byte, 4096)...))

	r := NewDefaultReader(testNS, shim, cfg)
	skim, err := r.SkimSegment(listedRef(t, shim, cfg, 0))
	if err != nil {
		t.Fatal(err)
	}
	if skim.LastCommitHash != parts[0][3].Hash {
		t.Fatal("skim stopped at the garbled header instead of reading the intact frames after it")
	}
	if len(skim.DamagedFrames) != 1 || skim.DamagedFrames[0] != int64(frames[1]) {
		t.Fatalf("damaged frames %v, want [%d]", skim.DamagedFrames, frames[1])
	}
	if skim.FramesEnd != int64(len(raw)) || skim.TrailingUnreadable || skim.LastFrameDamaged {
		t.Fatalf("skim %+v: frames end at %d, and zero padding is not unreadable", skim, len(raw))
	}
	var seen int
	if err := r.StreamCommitsPastDamage(listedRef(t, shim, cfg, 0), func(document.Commit, int64) error { seen++; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Fatalf("streamed %d commits past the damage, want 3", seen)
	}
}
