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
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
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

func (b *mapBlobs) WriteKeyedBlob(h codec.Hash, p []byte) error {
	b.mu.Lock()
	b.m[h] = append([]byte(nil), p...)
	b.mu.Unlock()
	return nil
}

func newTestStore(t *testing.T, cfg Config) (*Store, *storio.InMemoryPlatformIO, *mapBlobs) {
	t.Helper()
	shim := storio.NewInMemoryPlatformIO()
	small := &mapBlobs{m: map[codec.Hash][]byte{}}
	if cfg.SmallMaxBytes == 0 {
		cfg.SmallMaxBytes = 1 << 10
	}
	return New(shim, "app/data", small, cfg), shim, small
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	r.Read(b)
	return b
}

func readBack(t *testing.T, s *Store, h codec.Hash) []byte {
	t.Helper()
	r, err := s.Open(h)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != r.Size() {
		t.Fatalf("Open said %d bytes, read %d", r.Size(), len(b))
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
		var d []byte
		if err := encodeDelta(memSource(base), memSource(target), func(b []byte) error {
			d = append(d, b...)
			return nil
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		shim := storio.NewInMemoryPlatformIO()
		seg := "ns/app/data/attachment/d"
		shim.AppendToSegment(seg, d)
		src, err := parseDelta(memSource(base), segSource{shim: shim, name: seg, n: int64(len(d))})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := make([]byte, src.size())
		if err := src.readAt(got, 0); err != nil || !bytes.Equal(got, target) {
			t.Fatalf("%s: delta did not reproduce the target (%v)", name, err)
		}
		// Random access lands anywhere.
		for _, off := range []int64{0, 1, int64(len(target)) / 3, int64(len(target)) - 1} {
			if off < 0 || off >= int64(len(target)) {
				continue
			}
			one := make([]byte, 1)
			if err := src.readAt(one, off); err != nil || one[0] != target[off] {
				t.Fatalf("%s: random read at %d", name, off)
			}
		}
		if name == "edit in middle" && len(d) > 2000 {
			t.Fatalf("a small edit cost a %d-byte delta", len(d))
		}
	}
	shim := storio.NewInMemoryPlatformIO()
	shim.AppendToSegment("ns/x/attachment/bad", []byte{0x05, opCopy, 0xff, 0xff, 0xff, 0x0f, 0x05})
	if _, err := parseDelta(memSource(base[:10]), segSource{shim: shim, name: "ns/x/attachment/bad", n: 7}); err == nil {
		t.Fatal("a copy past the base was accepted")
	}
}

// Blobs of every size round-trip, are named by the chunked content hash, and a small one is held
// inline in its pointer - one segment, and never the engine's blob store.
func TestBlobsRoundTripUnderTheirChunkedHash(t *testing.T) {
	s, shim, small := newTestStore(t, Config{HashWorkers: 3})
	r := rand.New(rand.NewSource(2))
	for _, n := range []int{0, 1, 1 << 10, 1<<10 + 1, chunk - 1, chunk, chunk + 1, 3*chunk + 17} {
		data := randomBytes(r, n)
		info, err := s.Put(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if info.Hash != document.AttachmentContentHash(data) || info.Size != int64(n) {
			t.Fatalf("%d bytes: info %+v", n, info)
		}
		if has, _ := s.Has(info.Hash); !has {
			t.Fatalf("%d bytes: not stored", n)
		}
		if got := readBack(t, s, info.Hash); !bytes.Equal(got, data) {
			t.Fatalf("%d bytes: read back different bytes", n)
		}
		if _, inSmall := small.m[info.Hash]; inSmall {
			t.Fatalf("%d bytes: written to the engine's blob store", n)
		}
		s.mu.Lock()
		p := s.pointers[info.Hash]
		s.mu.Unlock()
		if wantInline := n > 0 && int64(n) <= s.cfg.SmallMaxBytes; (p.inline != nil) != wantInline || (wantInline && p.segment != "") {
			t.Fatalf("%d bytes: inline = %v, segment %q", n, p.inline != nil, p.segment)
		}
	}
	// A reopened store reads the inline blobs back from their pointers.
	again := New(shim, "app/data", nil, Config{})
	data := randomBytes(rand.New(rand.NewSource(2)), 1)
	if got := readBack(t, again, document.AttachmentContentHash(data)); !bytes.Equal(got, data) {
		t.Fatal("an inline blob did not survive a reopen")
	}
	if _, err := s.Open(document.AttachmentContentHash([]byte("never stored"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing blob: %v", err)
	}
	if s.cfg.MaxBytes != 0 {
		t.Fatal("there is a default size limit")
	}
}

// A flipped byte in one chunk is caught at that chunk: the chunks before it read fine.
func TestCorruptChunkIsCaughtAtTheChunk(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{})
	data := randomBytes(rand.New(rand.NewSource(3)), 3*chunk)
	info, _ := s.Put(bytes.NewReader(data))
	s.mu.Lock()
	seg := s.pointers[info.Hash].segment
	s.mu.Unlock()
	raw, _ := shim.ReadFromSegment(seg, 0, 3*chunk)
	raw[2*chunk+5] ^= 0xff
	shim.DeleteSegment(seg)
	shim.AppendToSegment(seg, raw)
	r, err := s.Open(info.Hash)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, 2*chunk)
	if _, err := io.ReadFull(r, buf); err != nil || !bytes.Equal(buf, data[:2*chunk]) {
		t.Fatalf("the intact chunks: %v", err)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("the corrupt chunk read as %v", err)
	}
}

func TestLargeBlobsDedupAndSurviveReopen(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	data := randomBytes(rand.New(rand.NewSource(4)), 20_000)
	a, _ := s.Put(bytes.NewReader(data))
	b, _ := s.Put(bytes.NewReader(data))
	if a != b {
		t.Fatal("the same bytes stored twice got different infos")
	}
	names, _ := shim.ListSegments("app/data")
	if len(names) != 3 { // bytes, leaves, pointer
		t.Fatalf("segments after a duplicate put: %v", names)
	}
	again := New(shim, "app/data", small, s.cfg)
	if got := readBack(t, again, a.Hash); !bytes.Equal(got, data) {
		t.Fatal("a reopened store read different bytes")
	}
}

func TestUploadOverALimitAndAbortLeaveNothing(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{MaxBytes: 10_000})
	if _, err := s.Put(bytes.NewReader(make([]byte, 10_001))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
	u := s.Begin()
	u.Write(make([]byte, chunk+5000))
	u.Abort()
	if names, _ := shim.ListSegments("app/data"); len(names) != 0 {
		t.Fatalf("left behind: %v", names)
	}
}

func versions(r *rand.Rand, n, size int) [][]byte {
	v := [][]byte{randomBytes(r, size)}
	for i := 1; i < n; i++ {
		next := append([]byte(nil), v[i-1]...)
		copy(next[i*1000:], []byte("edit edit edit"))
		v = append(v, next)
	}
	return v
}

// Replaced versions stored as deltas read back the same, a chain reads back at every step, and
// Collect keeps a delta's base.
func TestStoreAsDeltaAndCollect(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{UploadGrace: time.Nanosecond})
	v := versions(rand.New(rand.NewSource(5)), 4, 2*chunk+100)
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
	if _, err := s.Collect(func(h codec.Hash) bool { return h == hashes[0] }); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, s, hashes[0]); !bytes.Equal(got, v[0]) {
		t.Fatal("collect broke a kept delta's chain")
	}
	if n, _ := s.Collect(func(codec.Hash) bool { return false }); n != len(hashes) {
		t.Fatalf("collected %d of %d", n, len(hashes))
	}
	if names, _ := shim.ListSegments("app/data"); len(names) != 0 {
		t.Fatalf("left behind: %v", names)
	}
}

// A read in flight finishes on the version it opened while maintenance turns it into a delta and
// GC collects it; the storage it pinned goes when it closes.
func TestReadersFinishOnTheirVersion(t *testing.T) {
	s, shim, _ := newTestStore(t, Config{UploadGrace: time.Nanosecond})
	v := versions(rand.New(rand.NewSource(6)), 2, 2*chunk+7)
	old, _ := s.Put(bytes.NewReader(v[0]))
	newer, _ := s.Put(bytes.NewReader(v[1]))
	r, err := s.Open(old.Hash)
	if err != nil {
		t.Fatal(err)
	}
	half := make([]byte, chunk)
	if _, err := io.ReadFull(r, half); err != nil {
		t.Fatal(err)
	}
	if did, err := s.StoreAsDelta(old.Hash, newer.Hash); err != nil || !did {
		t.Fatalf("delta: %v %v", did, err)
	}
	if _, err := s.Collect(func(h codec.Hash) bool { return h == newer.Hash }); err != nil {
		t.Fatal(err)
	}
	if _, deferred := s.PinnedSegments(); deferred == 0 {
		t.Fatal("nothing waited on the open reader")
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(half, rest...), v[0]) {
		t.Fatal("the reader did not finish on its version")
	}
	r.Close()
	if pinned, deferred := s.PinnedSegments(); pinned != 0 || deferred != 0 {
		t.Fatalf("after close: %d pinned, %d deferred", pinned, deferred)
	}
	names, _ := shim.ListSegments("app/data")
	if len(names) != 3 {
		t.Fatalf("the collected version's segments were not deleted after close: %v", names)
	}
	if _, err := s.Open(old.Hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the collected version still opens: %v", err)
	}
}

// A reader past its lease is released by the next Collect and fails cleanly.
func TestStalledReaderLeaseExpires(t *testing.T) {
	s, _, _ := newTestStore(t, Config{ReadLease: time.Millisecond})
	data := randomBytes(rand.New(rand.NewSource(7)), 2*chunk)
	info, _ := s.Put(bytes.NewReader(data))
	r, err := s.Open(info.Hash)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := s.Collect(func(codec.Hash) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, ErrReadLeaseExpired) {
		t.Fatalf("expired reader read: %v", err)
	}
	r.Close()
	if pinned, deferred := s.PinnedSegments(); pinned != 0 || deferred != 0 {
		t.Fatalf("%d pinned, %d deferred after expiry", pinned, deferred)
	}
}

// A crash between writing a delta's pointer and deleting the whole copy's leaves two pointer
// generations; a reopened store follows the newer, and Collect retires the older.
func TestPointerGenerationsAfterACrash(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	v := versions(rand.New(rand.NewSource(8)), 2, chunk+500)
	oi, _ := s.Put(bytes.NewReader(v[0]))
	ni, _ := s.Put(bytes.NewReader(v[1]))
	s.mu.Lock()
	cur := s.pointers[oi.Hash]
	s.mu.Unlock()
	seg := s.prefix + "u-crash"
	var d []byte
	encodeDelta(memSource(v[1]), memSource(v[0]), func(b []byte) error { d = append(d, b...); return nil })
	shim.AppendToSegment(seg, d)
	s.mu.Lock()
	if _, err := s.writePointerLocked(oi.Hash, pointer{segment: seg, leaves: cur.leaves, size: int64(len(v[0])), delta: true, base: ni.Hash}); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	again := New(shim, "app/data", small, s.cfg)
	if got := readBack(t, again, oi.Hash); !bytes.Equal(got, v[0]) {
		t.Fatal("after the crash the old version reads wrong")
	}
	if _, err := again.Collect(func(codec.Hash) bool { return true }); err != nil {
		t.Fatal(err)
	}
	for _, name := range func() []string { n, _ := shim.ListSegments("app/data"); return n }() {
		if name == cur.name || name == cur.segment {
			t.Fatalf("the superseded pointer or its segment survived collect: %s", name)
		}
	}
	if got := readBack(t, again, oi.Hash); !bytes.Equal(got, v[0]) {
		t.Fatal("collect broke the delta")
	}
}

func TestStoreAsDeltaSkipsUnrelatedVersions(t *testing.T) {
	s, _, _ := newTestStore(t, Config{})
	r := rand.New(rand.NewSource(9))
	a, _ := s.Put(bytes.NewReader(randomBytes(r, 30_000)))
	b, _ := s.Put(bytes.NewReader(randomBytes(r, 30_000)))
	if did, err := s.StoreAsDelta(a.Hash, b.Hash); err != nil || did {
		t.Fatalf("unrelated bytes became a delta: %v %v", did, err)
	}
	if strings.Contains(a.Hash.Hex(), "x") {
		t.Fatal("unreachable")
	}
}

// The mark survives a reopen and Collect; a torn mark of a newer generation reads as the older
// one and is never written over.
func TestMarkSurvivesReopenAndCollect(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	if m, err := s.Mark(); err != nil || len(m) != 0 {
		t.Fatalf("fresh store mark %v, %v", m, err)
	}
	a, b := codec.Hash{Bytes: [32]byte{1}}, codec.Hash{Bytes: [32]byte{2}}
	if err := s.SetMark([]codec.Hash{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(func(codec.Hash) bool { return false }); err != nil {
		t.Fatal(err)
	}
	// A torn mark two generations ahead, as a crash mid-write would leave.
	torn := s.markName(3)
	if _, err := shim.AppendToSegment(torn, []byte(markMagic)); err != nil {
		t.Fatal(err)
	}
	again := New(shim, "app/data", small, Config{})
	if m, err := again.Mark(); err != nil || len(m) != 2 || m[0] != a || m[1] != b {
		t.Fatalf("reopened mark %v, %v", m, err)
	}
	if err := again.SetMark([]codec.Hash{b}); err != nil {
		t.Fatal(err)
	}
	if _, err := again.Collect(func(codec.Hash) bool { return false }); err != nil {
		t.Fatal(err)
	}
	third := New(shim, "app/data", small, Config{})
	if m, err := third.Mark(); err != nil || len(m) != 1 || m[0] != b {
		t.Fatalf("mark after a torn generation %v, %v", m, err)
	}
	names, _ := shim.ListSegments("app/data")
	marks := 0
	for _, n := range names {
		if strings.HasPrefix(n, s.prefix+"m-") {
			marks++
		}
	}
	if marks != 1 {
		t.Fatalf("%d mark segments left, want 1: %v", marks, names)
	}
}

// A finished upload no commit references yet survives Collect for its grace - in this store and in
// one reopened over it - and is collected after.
func TestAFinishedUploadIsHeldForItsGrace(t *testing.T) {
	for name, size := range map[string]int{"inline": 300, "standalone": 2*chunk + 11} {
		t.Run(name, func(t *testing.T) {
			s, shim, small := newTestStore(t, Config{})
			b := randomBytes(rand.New(rand.NewSource(7)), size)
			info, err := s.Put(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			none := func(codec.Hash) bool { return false }
			if n, err := s.Collect(none); err != nil || n != 0 {
				t.Fatalf("collected %d, %v: an upload awaiting its commit", n, err)
			}
			if !bytes.Equal(readBack(t, s, info.Hash), b) {
				t.Fatal("held blob reads back wrong")
			}
			again := New(shim, "app/data", small, Config{}) // a restart between upload and commit
			if n, err := again.Collect(none); err != nil || n != 0 {
				t.Fatalf("reopened store collected %d, %v", n, err)
			}
			if !bytes.Equal(readBack(t, again, info.Hash), b) {
				t.Fatal("held blob reads back wrong after reopen")
			}
			expired := New(shim, "app/data", small, Config{UploadGrace: time.Nanosecond})
			if n, err := expired.Collect(none); err != nil || n != 1 {
				t.Fatalf("collected %d, %v after the grace", n, err)
			}
			if has, _ := expired.Has(info.Hash); has {
				t.Fatal("blob still stored after its grace")
			}
			if names, _ := shim.ListSegments("app/data"); len(names) != 0 {
				t.Fatalf("segments left after collection: %v", names)
			}
		})
	}
}

// Once a Collect sees a blob referenced, references alone decide: dropping the reference frees it
// without waiting out the grace.
func TestTheHoldEndsOnceTheBlobIsReferenced(t *testing.T) {
	s, _, _ := newTestStore(t, Config{})
	info, err := s.Put(bytes.NewReader(randomBytes(rand.New(rand.NewSource(8)), chunk+5)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(func(h codec.Hash) bool { return h == info.Hash }); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Collect(func(codec.Hash) bool { return false }); err != nil || n != 1 {
		t.Fatalf("collected %d, %v: a dereferenced blob", n, err)
	}
}

// Uploading a blob again, or holding it (a wire upload's Expect), restarts its grace - durably,
// so a reopened store keeps it too.
func TestReuploadAndHoldRestartTheGrace(t *testing.T) {
	for _, how := range []string{"reupload", "hold"} {
		t.Run(how, func(t *testing.T) {
			s, shim, small := newTestStore(t, Config{UploadGrace: time.Hour})
			b := randomBytes(rand.New(rand.NewSource(9)), chunk+5)
			info, err := s.Put(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			// Age the upload past its grace, on disk too.
			s.mu.Lock()
			p := s.pointers[info.Hash]
			p.finished = time.Now().Add(-2 * time.Hour)
			if p, err = s.writePointerLocked(info.Hash, p); err != nil {
				t.Fatal(err)
			}
			s.pointers[info.Hash] = p
			s.held[info.Hash] = p.finished
			s.mu.Unlock()
			if how == "reupload" {
				if _, err := s.Put(bytes.NewReader(b)); err != nil {
					t.Fatal(err)
				}
			} else if held, err := s.Hold(info.Hash); err != nil || !held {
				t.Fatalf("Hold = %v, %v", held, err)
			}
			none := func(codec.Hash) bool { return false }
			if n, err := s.Collect(none); err != nil || n != 0 {
				t.Fatalf("collected %d, %v after the grace restarted", n, err)
			}
			again := New(shim, "app/data", small, Config{UploadGrace: time.Hour})
			if n, err := again.Collect(none); err != nil || n != 0 {
				t.Fatalf("reopened store collected %d, %v", n, err)
			}
			if !bytes.Equal(readBack(t, again, info.Hash), b) {
				t.Fatal("blob reads back wrong")
			}
		})
	}
}

// A v2 pointer (no finish time) still reads, and has no grace.
func TestAVersion2PointerReadsWithoutAGrace(t *testing.T) {
	s, shim, small := newTestStore(t, Config{})
	b := []byte("written by an older build")
	h := document.AttachmentContentHash(b)
	enc := encodePointer(pointer{size: int64(len(b)), inline: b})
	v2 := append([]byte(pointerMagicV2), enc[len(pointerMagic)])
	v2 = append(v2, enc[len(pointerMagic)+1:len(pointerMagic)+9]...) // flags, size
	v2 = append(v2, enc[len(pointerMagic)+17:len(enc)-4]...)         // names, bytes
	sum := sha256.Sum256(v2)
	if err := s.writeSegment(s.pointerName(h, 0), append(v2, sum[:4]...)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readBack(t, s, h), b) {
		t.Fatal("v2 pointer reads back wrong")
	}
	if n, err := New(shim, "app/data", small, Config{}).Collect(func(codec.Hash) bool { return false }); err != nil || n != 1 {
		t.Fatalf("collected %d, %v: a v2 pointer has no grace", n, err)
	}
}
