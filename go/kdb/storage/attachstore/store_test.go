package attachstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

type mapBlobs struct {
	mu sync.Mutex
	m  map[codec.Hash][]byte
}

func (b *mapBlobs) ReadBlob(h codec.Hash) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.m[h], nil
}

func (b *mapBlobs) WriteBlob(p []byte) (codec.Hash, error) {
	sum := sha256.Sum256(p)
	var h codec.Hash
	copy(h.Bytes[:], sum[:])
	b.mu.Lock()
	b.m[h] = append([]byte(nil), p...)
	b.mu.Unlock()
	return h, nil
}

func newTestStore(t *testing.T, cfg Config) (*Store, *storio.InMemoryPlatformIO, *mapBlobs) {
	t.Helper()
	shim := storio.NewInMemoryPlatformIO()
	small := &mapBlobs{m: map[codec.Hash][]byte{}}
	if cfg.SmallMaxBytes == 0 {
		cfg.SmallMaxBytes = 1 << 10
	}
	if cfg.ChunkBytes == 0 {
		cfg.ChunkBytes = 4 << 10
	}
	return New(shim, "app/data", small, cfg), shim, small
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	r.Read(b)
	return b
}

func sum(b []byte) codec.Hash {
	s := sha256.Sum256(b)
	var h codec.Hash
	copy(h.Bytes[:], s[:])
	return h
}

func readBack(t *testing.T, s *Store, h codec.Hash) []byte {
	t.Helper()
	r, size, err := s.Open(h)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != size {
		t.Fatalf("Open said %d bytes, read %d", size, len(b))
	}
	return b
}

func TestDeltaRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	base := randomBytes(r, 200_000)
	cases := map[string][]byte{
		"identical":      base,
		"empty":          nil,
		"tiny":           []byte("abc"),
		"unrelated":      randomBytes(r, 50_000),
		"prefix":         base[:1000],
		"edit in middle": append(append(append([]byte(nil), base[:100_000]...), []byte("INSERTED")...), base[100_010:]...),
		"appended":       append(append([]byte(nil), base...), randomBytes(r, 3000)...),
		"reordered":      append(append([]byte(nil), base[150_000:]...), base[:150_000]...),
	}
	for name, target := range cases {
		d := encodeDelta(base, target)
		got, err := applyDelta(base, d, 1<<30)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, target) {
			t.Fatalf("%s: delta did not reproduce the target", name)
		}
		if name == "edit in middle" && len(d) > 1000 {
			t.Fatalf("a small edit cost a %d-byte delta", len(d))
		}
	}
	// A corrupt delta is refused, never applied out of range.
	d := encodeDelta(base, cases["edit in middle"])
	for i := 0; i < len(d) && i < 64; i++ {
		bad := append([]byte(nil), d...)
		bad[i] ^= 0xff
		if got, err := applyDelta(base, bad, 1<<30); err == nil && bytes.Equal(got, cases["edit in middle"]) {
			continue // a flip that happens to be harmless
		}
	}
	if _, err := applyDelta(base, []byte{0x05, opCopy, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x05}, 1<<30); err == nil {
		t.Fatal("a copy past the base was applied")
	}
}

func TestSmallAndLargeBlobsRoundTrip(t *testing.T) {
	s, _, small := newTestStore(t, Config{})
	r := rand.New(rand.NewSource(2))
	for _, n := range []int{0, 1, 1 << 10, 1<<10 + 1, 4 << 10, 50_000} {
		data := randomBytes(r, n)
		info, err := s.Put(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if info.Hash != sum(data) || info.Size != int64(n) {
			t.Fatalf("%d bytes: info %+v", n, info)
		}
		if has, _ := s.Has(info.Hash); !has {
			t.Fatalf("%d bytes: not stored", n)
		}
		if got := readBack(t, s, info.Hash); !bytes.Equal(got, data) {
			t.Fatalf("%d bytes: read back different bytes", n)
		}
		_, inSmall := small.m[info.Hash]
		if wantSmall := n > 0 && int64(n) <= s.cfg.SmallMaxBytes; inSmall != wantSmall {
			t.Fatalf("%d bytes: in the small store = %v, want %v", n, inSmall, wantSmall)
		}
	}
	if _, _, err := s.Open(sum([]byte("never stored"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing blob: %v", err)
	}
}

// The same bytes stored twice are one blob; a reopened store finds what an earlier one wrote.
func TestLargeBlobsDedupAndSurviveReopen(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	data := randomBytes(rand.New(rand.NewSource(3)), 20_000)
	a, _ := s.Put(bytes.NewReader(data))
	b, _ := s.Put(bytes.NewReader(data))
	if a != b {
		t.Fatal("the same bytes stored twice got different infos")
	}
	names, _ := shim.ListSegments("app/data")
	if len(names) != 2 { // one blob, one pointer
		t.Fatalf("segments after a duplicate put: %v", names)
	}
	again := New(shim, "app/data", small, s.cfg)
	if got := readBack(t, again, a.Hash); !bytes.Equal(got, data) {
		t.Fatal("a reopened store read different bytes")
	}
}

func TestUploadOverTheLimitAndAbortLeaveNothing(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{MaxBytes: 10_000})
	if _, err := s.Put(bytes.NewReader(make([]byte, 10_001))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
	u := s.Begin()
	u.Write(make([]byte, 5000))
	u.Abort()
	if names, _ := shim.ListSegments("app/data"); len(names) != 0 {
		t.Fatalf("left behind: %v", names)
	}
}

// A replaced version stored as a delta reads back the same, a chain reads back at every step, and
// Collect keeps a delta's base.
func TestStoreAsDeltaAndCollect(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{})
	r := rand.New(rand.NewSource(4))
	v := [][]byte{randomBytes(r, 100_000)}
	for i := 1; i < 4; i++ {
		next := append([]byte(nil), v[i-1]...)
		copy(next[i*1000:], []byte("edit edit edit"))
		v = append(v, next)
	}
	var hashes []codec.Hash
	for _, b := range v {
		info, err := s.Put(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, info.Hash)
	}
	for i := 0; i < len(hashes)-1; i++ {
		did, err := s.StoreAsDelta(hashes[i], hashes[i+1])
		if err != nil || !did {
			t.Fatalf("version %d as a delta: %v %v", i, did, err)
		}
	}
	for i, h := range hashes {
		if got := readBack(t, s, h); !bytes.Equal(got, v[i]) {
			t.Fatalf("version %d reads back different bytes", i)
		}
	}
	// Keeping only the oldest version keeps the chain under it.
	if _, err := s.Collect(func(h codec.Hash) bool { return h == hashes[0] }); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, s, hashes[0]); !bytes.Equal(got, v[0]) {
		t.Fatal("collect broke a kept delta's chain")
	}
	// Keeping nothing removes everything.
	if n, _ := s.Collect(func(codec.Hash) bool { return false }); n != len(hashes) {
		t.Fatalf("collected %d of %d", n, len(hashes))
	}
	for _, name := range mustList(t, shim) {
		if strings.Contains(name, "/attachment/") {
			t.Fatalf("left behind: %s", name)
		}
	}
}

func mustList(t *testing.T, shim *storio.InMemoryPlatformIO) []string {
	t.Helper()
	names, err := shim.ListSegments("app/data")
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// A crash between writing a delta's pointer and deleting the whole copy's leaves two pointer
// generations; a reopened store follows the newer, and Collect retires the older.
func TestPointerGenerationsAfterACrash(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	r := rand.New(rand.NewSource(5))
	old := randomBytes(r, 40_000)
	newer := append([]byte(nil), old...)
	copy(newer[100:], "changed")
	oi, _ := s.Put(bytes.NewReader(old))
	ni, _ := s.Put(bytes.NewReader(newer))
	// Simulate the crash: write the delta pointer by hand, keeping the old pointer and segment.
	s.mu.Lock()
	cur := s.pointers[oi.Hash]
	s.mu.Unlock()
	seg := s.prefix + "u-crash"
	shim.AppendToSegment(seg, encodeDelta(newer, old))
	s.mu.Lock()
	if _, err := s.writePointerLocked(oi.Hash, pointer{segment: seg, size: int64(len(old)), delta: true, base: ni.Hash}); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	again := New(shim, "app/data", small, s.cfg)
	if got := readBack(t, again, oi.Hash); !bytes.Equal(got, old) {
		t.Fatal("after the crash the old version reads wrong")
	}
	if _, err := again.Collect(func(codec.Hash) bool { return true }); err != nil {
		t.Fatal(err)
	}
	for _, name := range mustList(t, shim) {
		if name == cur.name || name == cur.segment {
			t.Fatalf("the superseded pointer or its segment survived collect: %s", name)
		}
	}
	if got := readBack(t, again, oi.Hash); !bytes.Equal(got, old) {
		t.Fatal("collect broke the delta")
	}
}

// A delta that would not save half the bytes is not stored.
func TestStoreAsDeltaSkipsUnrelatedVersions(t *testing.T) {
	s, _, _ := newTestStore(t, Config{})
	r := rand.New(rand.NewSource(6))
	a, _ := s.Put(bytes.NewReader(randomBytes(r, 30_000)))
	b, _ := s.Put(bytes.NewReader(randomBytes(r, 30_000)))
	if did, err := s.StoreAsDelta(a.Hash, b.Hash); err != nil || did {
		t.Fatalf("unrelated bytes became a delta: %v %v", did, err)
	}
}
