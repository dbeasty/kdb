package embed

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// The invariant the PreallocateSegments switch exists to enforce: a segment is
// preallocated at exactly the size it rotates at, whatever that size happens to
// be. Preallocating less leaves every segment growing again past the
// preallocated region - quietly giving back the metadata-commit saving that is
// the whole point - and preallocating more wastes the difference on every
// segment. Only equality is correct, so it is derived rather than configured,
// and this test is what stops the two drifting apart if either default moves.
func TestPreallocateSizeTracksTheRotationCap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage StorageOptions
		want    int64
	}{
		{
			name:    "off by default",
			storage: StorageOptions{},
			want:    0,
		},
		{
			name:    "on, cap unset, takes the rotation default",
			storage: StorageOptions{PreallocateSegments: true},
			want:    delta.DefaultDeltaMaxSegmentBytes,
		},
		{
			name:    "on, explicit cap, takes that cap exactly",
			storage: StorageOptions{PreallocateSegments: true, DeltaMaxSegmentBytes: 8 << 20},
			want:    8 << 20,
		},
		{
			// Tuning segment size down for a small instance has to tune
			// preallocation with it, or turning the feature on would cost 64MiB
			// per namespace regardless of what the operator asked for.
			name:    "on, small cap for a small instance",
			storage: StorageOptions{PreallocateSegments: true, DeltaMaxSegmentBytes: 1 << 20},
			want:    1 << 20,
		},
		{
			// The cap is what rotation uses; a size set while the switch is off
			// must not preallocate anything.
			name:    "off, explicit cap, still nothing",
			storage: StorageOptions{DeltaMaxSegmentBytes: 8 << 20},
			want:    0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := preallocateBytesFor(tc.storage); got != tc.want {
				t.Fatalf("preallocateBytesFor = %d, want %d", got, tc.want)
			}
		})
	}
}

// And the derivation must agree with what the writer actually rotates at, not
// merely with a constant that happens to match today.
func TestPreallocateSizeEqualsWhatTheWriterRotatesAt(t *testing.T) {
	for _, cap := range []int64{0, 1 << 20, 8 << 20, 64 << 20} {
		got := preallocateBytesFor(StorageOptions{PreallocateSegments: true, DeltaMaxSegmentBytes: cap})
		want := delta.EffectiveMaxSegmentBytes(cap)
		if got != want {
			t.Fatalf("cap %d: preallocate at %d but the writer rotates at %d", cap, got, want)
		}
	}
}
