package attachstore

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// bounded is a store that holds a few dozen pointers and spills its index every 32 changes.
func bounded(cfg Config) Config {
	cfg.PointerCacheBytes = 16 << 10
	cfg.indexOverlay = 32
	return cfg
}

// checkResident fails the test if s holds more pointers, or index changes, than its bounds allow.
func checkResident(t *testing.T, s *Store) {
	t.Helper()
	pointers, overlay := s.ResidentPointers()
	if max := int(s.cfg.PointerCacheBytes / 256); pointers > max {
		t.Fatalf("%d pointers resident, over the %d the cache allows", pointers, max)
	}
	if overlay >= s.cfg.indexOverlay {
		t.Fatalf("%d index entries resident in the overlay, bound %d", overlay, s.cfg.indexOverlay)
	}
}

// putMany stores n blobs - small ones inline, every 50th standalone (over SmallMaxBytes) - checking
// the bounds after each.
func putMany(t *testing.T, s *Store, r *rand.Rand, n int) ([]codec.Hash, [][]byte) {
	t.Helper()
	var hs []codec.Hash
	var data [][]byte
	for i := 0; i < n; i++ {
		b := randomBytes(r, 50+r.Intn(200))
		if i%50 == 1 {
			b = randomBytes(r, 2<<10)
		}
		info, err := s.Put(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		hs, data = append(hs, info.Hash), append(data, b)
		checkResident(t, s)
	}
	return hs, data
}

func listNames(t *testing.T, s *Store) []string {
	t.Helper()
	names, err := s.shim.ListSegments(s.ns)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// Resident pointers stay within the bound however many blobs there are; every evicted one reads
// back, through the index, before and after a restart; and holds survive both.
func TestResidentPointersStayBoundedAcrossManyBlobs(t *testing.T) {
	cfg := bounded(Config{})
	s, shim, small := newTestStore(t, cfg)
	hs, data := putMany(t, s, rand.New(rand.NewSource(40)), 1500)
	if runs := len(s.idx.runs); runs == 0 || runs > 8 {
		t.Fatalf("%d index runs for 1500 blobs: the index did not spill, or did not merge", runs)
	}
	for i, h := range hs {
		if got := readBack(t, s, h); !bytes.Equal(got, data[i]) {
			t.Fatalf("blob %d reads back wrong", i)
		}
		checkResident(t, s)
	}
	none := func(codec.Hash) bool { return false }
	if n, err := s.Collect(none); err != nil || n != 0 {
		t.Fatalf("collected %d, %v within the upload grace", n, err)
	}
	checkResident(t, s)

	again := New(shim, "app/data", small, s.cfg)
	if large, err := again.LargeBlobs(); err != nil || len(large) != len(hs) {
		t.Fatalf("after a restart: %d blobs, %v", len(large), err)
	}
	for i := len(hs) - 1; i >= 0; i-- {
		if got := readBack(t, again, hs[i]); !bytes.Equal(got, data[i]) {
			t.Fatalf("blob %d reads back wrong after a restart", i)
		}
	}
	checkResident(t, again)
	if n, err := again.Collect(none); err != nil || n != 0 {
		t.Fatalf("collected %d, %v: the holds did not survive the restart", n, err)
	}
	if err := again.Committed(hs...); err != nil {
		t.Fatal(err)
	}
	if n, err := again.Collect(none); err != nil || n != len(hs) {
		t.Fatalf("collected %d of %d, %v once committed", n, len(hs), err)
	}
	for _, name := range listNames(t, again) {
		if !again.idx.isRun(name) {
			t.Fatalf("left behind: %s", name)
		}
	}
}

// A pointer is read from its segment once evicted, and a torn newer generation (a crash while it
// was written) reads, after a restart, as the generation below it; Collect then retires the tear.
func TestEvictedPointersReloadPastATornGeneration(t *testing.T) {
	cfg := bounded(Config{})
	s, shim, small := newTestStore(t, cfg)
	r := rand.New(rand.NewSource(41))
	target := randomBytes(r, 3<<10)
	ti, err := s.Put(bytes.NewReader(target))
	if err != nil {
		t.Fatal(err)
	}
	hs, data := putMany(t, s, r, 300)
	cur := pointerOf(t, s, ti.Hash)
	torn := s.pointerName(ti.Hash, cur.gen+1)
	shim.AppendToSegment(torn, []byte(pointerMagic+"torn"))
	ghost := document.AttachmentContentHash([]byte("only ever torn"))
	ghostName := s.pointerName(ghost, 0)
	shim.AppendToSegment(ghostName, []byte(pointerMagic))

	again := New(shim, "app/data", small, cfg)
	if got := readBack(t, again, ti.Hash); !bytes.Equal(got, target) {
		t.Fatal("the blob under a torn generation reads wrong")
	}
	for i, h := range hs { // evicts the target again
		if got := readBack(t, again, h); !bytes.Equal(got, data[i]) {
			t.Fatalf("blob %d reads back wrong", i)
		}
	}
	if has, err := again.Has(ghost); err != nil || has {
		t.Fatalf("a hash whose only pointer is torn: has %v, %v", has, err)
	}
	if n, err := again.Collect(func(codec.Hash) bool { return true }); err != nil || n != 0 {
		t.Fatalf("collected %d, %v", n, err)
	}
	for _, name := range listNames(t, again) {
		if name == torn || name == ghostName {
			t.Fatalf("collect left the torn pointer %s", name)
		}
	}
	if got := readBack(t, again, ti.Hash); !bytes.Equal(got, target) {
		t.Fatal("the blob reads wrong after collect")
	}
	// A new generation after the restart goes over the torn one, not under it.
	if held, err := again.Hold(ti.Hash); err != nil || !held {
		t.Fatalf("Hold = %v, %v", held, err)
	}
	checkResident(t, again)
}

// Collect decides from the index, a few pointers resident: what keep rejects goes, what it keeps
// stays, and a reader open on a blob that goes finishes on it.
func TestCollectUnderEvictionKeepsWhatItShould(t *testing.T) {
	cfg := bounded(Config{})
	s, shim, small := newTestStore(t, cfg)
	hs, data := putMany(t, s, rand.New(rand.NewSource(42)), 600)
	if err := s.Committed(hs...); err != nil {
		t.Fatal(err)
	}
	kept := map[codec.Hash]bool{}
	for i, h := range hs {
		if i%2 == 0 {
			kept[h] = true
		}
	}
	rd, err := s.Open(hs[51]) // standalone, and going
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Collect(func(h codec.Hash) bool { return kept[h] }); err != nil || n != 300 {
		t.Fatalf("collected %d, %v; want 300", n, err)
	}
	checkResident(t, s)
	got := make([]byte, rd.Size())
	if _, err := rd.ReadAt(got, 0); err != nil || !bytes.Equal(got, data[51]) {
		t.Fatalf("the pinned reader: %v", err)
	}
	rd.Close()
	if pinned, deferred := s.PinnedSegments(); pinned != 0 || deferred != 0 {
		t.Fatalf("pins left: %d pinned, %d deferred", pinned, deferred)
	}
	for i, h := range hs {
		has, err := s.Has(h)
		if err != nil || has != kept[h] {
			t.Fatalf("blob %d: has %v, %v; kept %v", i, has, err, kept[h])
		}
		if has && !bytes.Equal(readBack(t, s, h), data[i]) {
			t.Fatalf("blob %d reads back wrong", i)
		}
	}
	pointers := 0
	for _, name := range listNames(t, s) {
		switch {
		case s.idx.isRun(name):
		case strings.HasPrefix(name, s.prefix+"h-"):
			pointers++
		default: // the standalone blobs are all odd, all gone
			t.Fatalf("left behind: %s", name)
		}
	}
	if pointers != 300 {
		t.Fatalf("%d pointer segments for 300 blobs", pointers)
	}
	again := New(shim, "app/data", small, cfg)
	if n, err := again.Collect(func(h codec.Hash) bool { return kept[h] }); err != nil || n != 0 {
		t.Fatalf("after a restart collected %d, %v", n, err)
	}
}

// Deltas work with their pointers evicted: a chain is stored, read and kept whole by Collect when
// only its far end is kept, before and after a restart.
func TestDeltaChainsUnderEviction(t *testing.T) {
	cfg := tinyDeltas(Config{PointerCacheBytes: 2 << 10, indexOverlay: 4})
	s, shim, small := newTestStore(t, cfg)
	r := rand.New(rand.NewSource(43))
	v := versions(r, 4, chunk+500)
	var h []codec.Hash
	for _, b := range v {
		info, err := s.Put(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		h = append(h, info.Hash)
	}
	noise, _ := putMany(t, s, r, 40)
	for i := len(h) - 2; i >= 0; i-- { // v0 -> v1 -> v2 -> v3, the newest whole
		if did, err := s.StoreAsDelta(h[i], h[i+1]); err != nil || !did {
			t.Fatalf("delta %d: %v %v", i, did, err)
		}
		checkResident(t, s)
	}
	if d, err := s.Deltas(); err != nil || len(d) != 3 {
		t.Fatalf("deltas %v, %v", d, err)
	}
	only := func(x codec.Hash) bool { return x == h[0] }
	if n, err := s.Collect(only); err != nil || n != len(noise) {
		t.Fatalf("collected %d, %v; want the %d others, not the chain", n, err, len(noise))
	}
	for _, st := range []*Store{s, New(shim, "app/data", small, cfg)} {
		for i := range h {
			if got := readBack(t, st, h[i]); !bytes.Equal(got, v[i]) {
				t.Fatalf("version %d reads wrong", i)
			}
		}
		if d, err := st.Deltas(); err != nil || len(d) != 3 {
			t.Fatalf("deltas %v, %v", d, err)
		}
		checkResident(t, st)
	}
}
