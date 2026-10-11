package delta

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// ListSegments used to `continue` past any segment scanSegmentRef could not read, so every
// commit in it vanished from replay, checkpoints and history without a word. These pin that an
// unreadable segment is now a typed error, and that the cases that are not damage - a torn tail,
// a segment deleted between listing and reading - still list cleanly.

// writeChainAcross writes one hash-linked chain of commits across seqs, n commits per segment.
func writeChainAcross(t *testing.T, cfg storage.StorageEngineConfig, seqs []int64, n int) [][]document.Commit {
	t.Helper()
	chain := buildChain(t, n*len(seqs))
	out := make([][]document.Commit, 0, len(seqs))
	for i, seq := range seqs {
		part := chain[i*n : (i+1)*n]
		writeSealedSegment(t, cfg, seq, part...)
		out = append(out, part)
	}
	return out
}

// rewriteSegment replaces a segment's bytes with edit(bytes), through the shim.
func rewriteSegment(t *testing.T, shim storage.PlatformIOShim, seq int64, edit func([]byte) []byte) {
	t.Helper()
	name := storio.SegmentNameBuilder.DeltaSequenced(testNS, seq)
	raw, err := shim.ReadFromSegment(name, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	raw = edit(append([]byte(nil), raw...))
	if err := shim.DeleteSegment(name); err != nil {
		t.Fatal(err)
	}
	if _, err := shim.AppendToSegment(name, raw); err != nil {
		t.Fatal(err)
	}
}

// damageFirstFrameCodec sets the first frame's codec byte to one no build knows. The header is
// outside the CRC, so the frame still walks as intact and only decoding it fails - which is what
// makes scanSegmentRef error rather than stop at a "torn tail".
func damageFirstFrameCodec(raw []byte) []byte {
	raw[5] = 0x7f
	return raw
}

func TestListSegmentsReportsAnUnreadableSegment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		damaged int64
	}{
		{"not the newest", 1},
		{"the newest", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := newTestShim(t)
			cfg := newConfig(shim, storage.CompressionNone)
			writeChainAcross(t, cfg, []int64{0, 1, 2}, 3)
			rewriteSegment(t, shim, tc.damaged, damageFirstFrameCodec)

			refs, err := NewDefaultReader(testNS, shim, cfg).ListSegments()
			var unreadable *UnreadableSegmentError
			if !errors.As(err, &unreadable) {
				t.Fatalf("ListSegments = %d ref(s), err %v; want *UnreadableSegmentError - a segment "+
					"it cannot read must not silently drop out of the listing", len(refs), err)
			}
			if unreadable.Sequence != tc.damaged || unreadable.NamespaceID != testNS {
				t.Fatalf("error names segment %d of %q, want %d of %q", unreadable.Sequence, unreadable.NamespaceID, tc.damaged, testNS)
			}
			var corrupt *CorruptFrameError
			if !errors.As(err, &corrupt) {
				t.Fatalf("the cause should stay reachable through the error: %v", err)
			}
		})
	}
}

func TestListSegmentsStillToleratesATornNewestSegment(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	parts := writeChainAcross(t, cfg, []int64{0, 1, 2}, 3)
	// Cut the newest segment's last frame short: what a kill mid-write leaves.
	rewriteSegment(t, shim, 2, func(raw []byte) []byte { return raw[:len(raw)-5] })

	refs, err := NewDefaultReader(testNS, shim, cfg).ListSegments()
	if err != nil {
		t.Fatalf("a torn tail is not damage: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("listed %d segments, want 3", len(refs))
	}
	if refs[2].LastCommitHash != parts[2][1].Hash {
		t.Fatalf("the torn segment should end at its last intact commit")
	}
}

// vanishingShim deletes a segment the moment it is first read, as truncation running between a
// listing and its scan would.
type vanishingShim struct {
	storage.PlatformIOShim
	victim string
	gone   bool
}

func (s *vanishingShim) ReadFromSegment(name string, offset int64, length int) ([]byte, error) {
	if name == s.victim && !s.gone {
		s.gone = true
		if err := s.PlatformIOShim.DeleteSegment(name); err != nil {
			return nil, err
		}
	}
	return s.PlatformIOShim.ReadFromSegment(name, offset, length)
}

func TestListSegmentsSkipsASegmentDeletedWhileListing(t *testing.T) {
	base := newTestShim(t)
	cfg := newConfig(base, storage.CompressionNone)
	writeChainAcross(t, cfg, []int64{0, 1, 2}, 2)
	shim := &vanishingShim{PlatformIOShim: base, victim: storio.SegmentNameBuilder.DeltaSequenced(testNS, 0)}

	refs, err := NewDefaultReader(testNS, shim, cfg).ListSegments()
	if err != nil {
		t.Fatalf("a segment that is gone is not unreadable: %v", err)
	}
	if len(refs) != 2 || refs[0].SequenceNumber != 1 {
		t.Fatalf("listed %+v, want segments 1 and 2", refs)
	}
}
