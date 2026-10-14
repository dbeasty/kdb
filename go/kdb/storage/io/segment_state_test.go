package io

import (
	"fmt"
	"math/rand"
	"testing"
)

// nullStore is a SegmentByteStore that keeps nothing, so a test can touch a hundred thousand
// segments through the shim in milliseconds and see only what the shim itself retains.
type nullStore struct{}

func (nullStore) Append(string, []byte) (int64, error)        { return 0, nil }
func (nullStore) Read(string, int64, int) ([]byte, error)     { return nil, nil }
func (nullStore) Flush(string, bool) error                    { return nil }
func (nullStore) MarkSealed(string) error                     { return nil }
func (nullStore) List(string) ([]string, error)               { return nil, nil }
func (nullStore) Delete(string) error                         { return nil }
func (nullStore) AvailableBytes() (int64, error)              { return 0, nil }
func (nullStore) ReadSnapshot(string) ([]byte, error)         { return nil, nil }
func (nullStore) WriteSnapshot(string, []byte) error          { return nil }
func (nullStore) DeleteSnapshot(string) error                 { return nil }
func (nullStore) RestoreSegment(string) error                 { return nil }
func (nullStore) Rename(string, string) error                 { return nil }
func (nullStore) Size(string) (int64, error)                  { return 0, nil }
func (nullStore) Stamp(string) (int64, int64, bool, error)    { return 0, 0, true, nil }
func (nullStore) ReadInto(string, int64, []byte) (int, error) { return 0, nil }

func newNullShim() *FileBackedPlatformIO {
	return NewFileBackedPlatformIO(DefaultPlatformIOConfig(), nullStore{})
}

// The shim used to keep a lock, a sealed flag and a generation for every segment name it had
// seen: ~300 bytes a segment, ~31 MB at 100k segments, under stores that are otherwise bounded.
// Touching every way a segment can be touched, for far more segments than any bound, must leave
// the shim holding no more than its bounds.
func TestShimStateDoesNotGrowWithSegmentsTouched(t *testing.T) {
	shim := newNullShim()
	const segments = 100_000
	for i := 0; i < segments; i++ {
		name := fmt.Sprintf("ns/app/sstable/L0/%08d.sst", i)
		if i%2 == 0 {
			name = SegmentNameBuilder.DeltaSequenced("app", int64(i))
		}
		if _, err := shim.AppendToSegment(name, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := shim.ReadFromSegment(name, 0, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := shim.SegmentSize(name); err != nil {
			t.Fatal(err)
		}
		shim.SegmentGeneration(name)
		if err := shim.SealSegment(name); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(shim.sealed.names); n > defaultSealedMemory {
		t.Errorf("the shim remembers %d sealed names, more than its bound of %d", n, defaultSealedMemory)
	}
	if n := len(shim.sealed.ring); n > defaultSealedMemory {
		t.Errorf("the seal ring holds %d entries, more than its bound of %d", n, defaultSealedMemory)
	}
	if n := len(shim.generations.gens); n > defaultGenerationMemory {
		t.Errorf("the shim remembers %d generations, more than its bound of %d", n, defaultGenerationMemory)
	}
	if n := len(shim.sealed.restored); n != 0 {
		t.Errorf("nothing was restored, yet %d directories have restored runs", n)
	}
}

// Restored segments stay sealed however many seals come after them - and however many of them
// there are, since a contiguous restore is one run.
func TestShimRestoredSegmentsStaySealedPastTheSealBound(t *testing.T) {
	shim := newNullShim()
	const restored = 5000
	for seq := int64(restored); seq >= 1; seq-- { // highest first, as RestoreFromArchive does
		if err := shim.RestoreSegment(SegmentNameBuilder.DeltaSequenced("app", seq)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2*defaultSealedMemory; i++ {
		if err := shim.SealSegment(fmt.Sprintf("ns/app/wal/%08d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if runs := shim.sealed.restored["ns/app/delta/"]; len(runs) != 1 {
		t.Fatalf("a contiguous restore is held as %d runs, want 1: %v", len(runs), runs)
	}
	for seq := int64(1); seq <= restored; seq++ {
		if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced("app", seq), []byte("x")); err == nil {
			t.Fatalf("appended into restored segment %d", seq)
		}
	}

	// Deleting one frees its name for a new segment, and leaves its neighbours sealed.
	hole := SegmentNameBuilder.DeltaSequenced("app", 2500)
	if err := shim.DeleteSegment(hole); err != nil {
		t.Fatal(err)
	}
	if _, err := shim.AppendToSegment(hole, []byte("x")); err != nil {
		t.Fatalf("a deleted restored segment's name is still sealed: %v", err)
	}
	for _, seq := range []int64{1, 2499, 2501, restored} {
		if _, err := shim.AppendToSegment(SegmentNameBuilder.DeltaSequenced("app", seq), []byte("x")); err == nil {
			t.Fatalf("deleting segment 2500 unsealed restored segment %d", seq)
		}
	}

	// Releasing the namespace forgets them, as it always has.
	if err := shim.ReleaseHandles(SegmentNameBuilder.NamespacePrefix("app")); err != nil {
		t.Fatal(err)
	}
	if len(shim.sealed.restored) != 0 {
		t.Fatalf("restored runs survive releasing their namespace: %v", shim.sealed.restored)
	}
}

// A seal is refused-into for as long as the shim remembers it, which is the last
// defaultSealedMemory seals; a name sealed again after being forgotten is remembered afresh.
func TestShimRemembersRecentSeals(t *testing.T) {
	shim := newNullShim()
	first := "ns/app/wal/first"
	if err := shim.SealSegment(first); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < defaultSealedMemory-1; i++ {
		if err := shim.SealSegment(fmt.Sprintf("ns/app/wal/%08d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := shim.AppendToSegment(first, []byte("x")); err == nil {
		t.Fatal("appended into a segment sealed within the last defaultSealedMemory seals")
	}
	// A delete and re-seal leaves a stale ring entry for first; reusing that slot must not
	// forget the newer seal.
	if err := shim.DeleteSegment(first); err != nil {
		t.Fatal(err)
	}
	if err := shim.SealSegment(first); err != nil {
		t.Fatal(err)
	}
	if err := shim.SealSegment("ns/app/wal/pushes-the-stale-entry-out"); err != nil {
		t.Fatal(err)
	}
	if _, err := shim.AppendToSegment(first, []byte("x")); err == nil {
		t.Fatal("reusing a stale ring slot forgot a newer seal of the same name")
	}
}

// Renaming between two names on the same stripe must take that stripe once, not deadlock on it.
func TestShimRenameWithinOneStripe(t *testing.T) {
	shim := newNullShim()
	from := "ns/app/sstable-partial/a"
	var to string
	for i := 0; ; i++ {
		to = fmt.Sprintf("ns/app/sstable/L0/%d", i)
		if segmentStripe(to) == segmentStripe(from) {
			break
		}
	}
	if err := shim.RenameSegment(from, to); err != nil {
		t.Fatal(err)
	}
}

// Generations are forgotten past their bound, and a forgotten one comes back as a new number,
// never a reused one.
func TestShimForgottenGenerationIsNew(t *testing.T) {
	shim := newNullShim()
	shim.generations.memory = 1
	name := "ns/app/delta/x"
	before, _ := shim.SegmentGeneration(name)
	shim.SegmentGeneration("ns/app/delta/y") // the only slot: x is forgotten
	if n := len(shim.generations.gens); n != 1 {
		t.Fatalf("the shim remembers %d generations past a bound of 1", n)
	}
	if after, _ := shim.SegmentGeneration(name); after == before {
		t.Fatalf("a forgotten generation came back as the same number %d", before)
	}
}

// seqRuns against a plain set, through random adds and removes.
func TestSeqRunsMatchASet(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var runs seqRuns
	set := map[int64]bool{}
	for step := 0; step < 20000; step++ {
		seq := rng.Int63n(200)
		if rng.Intn(3) == 0 {
			runs = runs.remove(seq)
			delete(set, seq)
		} else {
			runs = runs.add(seq)
			set[seq] = true
		}
		for i := 1; i < len(runs); i++ {
			if runs[i-1].lo > runs[i-1].hi || runs[i-1].hi+1 >= runs[i].lo {
				t.Fatalf("step %d: runs out of order, overlapping or adjacent: %v", step, runs)
			}
		}
		for s := int64(-1); s <= 200; s++ {
			if runs.contains(s) != set[s] {
				t.Fatalf("step %d: contains(%d) = %v, set says %v; runs %v", step, s, runs.contains(s), set[s], runs)
			}
		}
	}
}
