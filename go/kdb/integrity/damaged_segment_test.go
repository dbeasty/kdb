package integrity

import (
	"strings"
	"testing"
)

// N5: verify and repair apply the rule replay does (storage.TailKind). A torn tail is bytes at
// the very end of a segment with nothing intact after them; damage that intact frames follow is
// mid-log corruption even in the newest segment, and zero padding after the last frame - a
// preallocated segment's unused space - is no finding at all.

func garbledHeader(f []byte) []byte {
	f = append([]byte(nil), f...)
	f[0] ^= 0xFF
	return f
}

func TestVerifyDamageWithIntactFramesAfterItIsNeverATornTail(t *testing.T) {
	for _, kind := range []string{"crc", "magic"} {
		t.Run(kind, func(t *testing.T) {
			shim := newTestShim(t)
			const ns = "ns1"
			commits := buildChain(t, 3, ns)
			damaged := flippedFrame(t, commits[1])
			if kind == "magic" {
				damaged = garbledHeader(rawFrame(t, commits[1]))
			}
			// The newest (only) segment: the case verify used to call a torn tail.
			appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), damaged, rawFrame(t, commits[2]))

			report, err := Verify(shim, ns, Options{Level: L1})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Findings) != 1 {
				t.Fatalf("want exactly one finding, got %+v", report.Findings)
			}
			f := report.Findings[0]
			if f.Classification != MidLogCorruption || f.Offset != len(rawFrame(t, commits[0])) {
				t.Fatalf("want mid_log_corruption at the damaged frame (offset %d), got %+v", len(rawFrame(t, commits[0])), f)
			}
			if !strings.Contains(f.Detail, "intact frames follow") {
				t.Fatalf("the finding should say intact frames follow the damage: %q", f.Detail)
			}
		})
	}
}

func TestVerifyZeroPaddingAfterTheLastFrameIsClean(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 4, ns)
	pad := make([]byte, 64<<10)
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), rawFrame(t, commits[1]), pad)
	appendSegment(t, shim, ns, 1, rawFrame(t, commits[2]), rawFrame(t, commits[3]), pad)

	report, err := Verify(shim, ns, Options{Level: L2})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Clean() {
		t.Fatalf("a preallocated segment's zero padding is not damage, got %+v", report.Findings)
	}
}

func TestVerifyTornTailBeforeZeroPaddingIsATornTail(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	torn := rawFrame(t, commits[1])
	for i := len(torn) / 2; i < len(torn); i++ {
		torn[i] = 0 // the second half of the append never reached the preallocated file
	}
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), torn, make([]byte, 64<<10))

	report, err := Verify(shim, ns, Options{Level: L1})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Findings) != 1 || report.Findings[0].Classification != TornTail {
		t.Fatalf("want one torn_tail, got %+v", report.Findings)
	}
}

// Repair used to act on the torn_tail verify reported for damage in the newest segment, and
// truncated the segment there - dropping the intact, acknowledged commits after the damage.
func TestRepairRefusesToDropIntactCommitsAfterDamage(t *testing.T) {
	for _, kind := range []string{"crc", "magic"} {
		t.Run(kind, func(t *testing.T) {
			shim := newTestShim(t)
			const ns = "ns1"
			commits := buildChain(t, 3, ns)
			damaged := flippedFrame(t, commits[1])
			if kind == "magic" {
				damaged = garbledHeader(rawFrame(t, commits[1]))
			}
			appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), damaged, rawFrame(t, commits[2]))
			name := "ns/" + ns + "/delta/00000000000000000000.seg"
			before, err := shim.ReadFromSegment(name, 0, 1<<20)
			if err != nil {
				t.Fatal(err)
			}

			report, err := Verify(shim, ns, Options{Level: L1})
			if err != nil {
				t.Fatal(err)
			}
			result, err := Repair(shim, report, Options{Level: L1})
			if err != nil {
				t.Fatal(err)
			}
			if !result.AnyRefused() {
				t.Fatalf("repair must refuse to drop the intact commit after the damage, did %+v", result.Steps)
			}
			found := false
			for _, s := range result.Steps {
				for _, h := range s.MissingHashes {
					if h == commits[2].Hash.Hex() {
						found = true
					}
				}
			}
			if !found {
				t.Fatalf("refusal should name the commit it would drop (%s): %+v", commits[2].Hash.Hex(), result.Steps)
			}
			after, err := shim.ReadFromSegment(name, 0, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("a refused repair must leave the segment untouched")
			}
		})
	}
}
