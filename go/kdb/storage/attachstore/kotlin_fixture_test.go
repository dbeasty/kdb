package attachstore

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Kotlin's AttachmentBlobStore (kdb-embed) writes and reads this package's layout segment for
// segment. These tests pin that both ways:
//
//   - golden bytes: a pointer of each kind and a format-1 delta (the format Kotlin writes; Go
//     writes format 2 and reads both), which Kotlin's AttachmentBlobStoreTest must encode
//     identically;
//   - fixture stores: one store built by each implementation from the same recipe (fixtureRecipe;
//     Kotlin's GoAttachStoreFixtureTest has the twin), dumped segment by segment. Each side opens
//     the other's dump, reads every blob back, and collects without losing anything.
//
// KDB_WRITE_FIXTURES=1 rewrites the Go-built dump the Kotlin test reads
// (kdb-jdbc/src/test/resources/attachstore/go-store.dump.gz); Kotlin's test writes the one read
// here (testdata/kotlin-store.dump.gz) under the same variable.

const fixtureNS = "app/data"

// fixtureText is n bytes of numbered lines: compressible, and with enough repetition between
// versions for deltas to work.
func fixtureText(n, seed int) []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, "seed %d line %07d the quick brown fox jumps over the lazy dog\n", seed, i)
	}
	return b.Bytes()[:n]
}

// fixtureEdit is base with "EDITED" written at every 100000th offset after the first.
func fixtureEdit(base []byte) []byte {
	out := append([]byte(nil), base...)
	for off := 100000; off+6 <= len(out); off += 100000 {
		copy(out[off:], "EDITED")
	}
	return out
}

type fixtureBlob struct {
	name string
	data []byte
}

// fixtureRecipe is what each fixture store holds: an inline blob at each end of the small range, a
// one-chunk and a three-chunk whole blob, and a replaced version stored as a delta against its
// successor (old, then new, last).
func fixtureRecipe() []fixtureBlob {
	newer := fixtureText(1200000, 5)
	return []fixtureBlob{
		{"tiny", []byte("hello attachments")},
		{"small-max", fixtureText(256<<10, 1)},
		{"one-chunk", fixtureText(300000, 2)},
		{"three-chunks", fixtureText(2*chunk+12345, 3)},
		{"delta-old", fixtureEdit(newer)},
		{"delta-new", newer},
	}
}

func fixtureMark() []codec.Hash {
	return []codec.Hash{{Bytes: sha256.Sum256([]byte("c1"))}, {Bytes: sha256.Sum256([]byte("c2"))}}
}

func buildFixture(t *testing.T) *storio.InMemoryPlatformIO {
	t.Helper()
	shim := storio.NewInMemoryPlatformIO()
	s := New(shim, fixtureNS, nil, Config{})
	var hashes []codec.Hash
	for _, b := range fixtureRecipe() {
		info, err := s.Put(bytes.NewReader(b.data))
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, info.Hash)
	}
	did, err := s.StoreAsDelta(hashes[4], hashes[5])
	if err != nil || !did {
		t.Fatalf("StoreAsDelta: %v %v", did, err)
	}
	if err := s.SetMark(fixtureMark()); err != nil {
		t.Fatal(err)
	}
	// The reference index (GC's note, r-<gen>): Kotlin does not use it, but must leave it alone.
	if err := s.SetRefIndex(RefIndex{Heads: fixtureMark(), Refs: SortHashes(append([]codec.Hash(nil), hashes...))}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collect(func(codec.Hash) bool { return true }); err != nil { // retire the replaced pointer
		t.Fatal(err)
	}
	return shim
}

const dumpMagic = "KDBSEGDUMP1\n"

// dumpSegments writes every segment of ns, sorted by name: u32 name length, name, u64 length, bytes.
func dumpSegments(t *testing.T, shim *storio.InMemoryPlatformIO, path string) {
	t.Helper()
	names, err := shim.ListSegments(fixtureNS)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	zw.Write([]byte(dumpMagic))
	for _, name := range names {
		data, err := io.ReadAll(&segReader{shim: shim, name: name})
		if err != nil {
			t.Fatal(err)
		}
		zw.Write(binary.BigEndian.AppendUint32(nil, uint32(len(name))))
		zw.Write([]byte(name))
		zw.Write(binary.BigEndian.AppendUint64(nil, uint64(len(data))))
		zw.Write(data)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

type segReader struct {
	shim *storio.InMemoryPlatformIO
	name string
	off  int64
}

func (r *segReader) Read(p []byte) (int, error) {
	b, err := r.shim.ReadFromSegment(r.name, r.off, len(p))
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, io.EOF
	}
	r.off += int64(len(b))
	return copy(p, b), nil
}

func loadDump(t *testing.T, path string) *storio.InMemoryPlatformIO {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(zr)
	magic := make([]byte, len(dumpMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != dumpMagic {
		t.Fatalf("%s is not a segment dump", path)
	}
	shim := storio.NewInMemoryPlatformIO()
	for {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); errors.Is(err, io.EOF) {
			return shim
		} else if err != nil {
			t.Fatal(err)
		}
		name := make([]byte, n)
		io.ReadFull(r, name)
		var l uint64
		binary.Read(r, binary.BigEndian, &l)
		data := make([]byte, l)
		if _, err := io.ReadFull(r, data); err != nil {
			t.Fatal(err)
		}
		shim.AppendToSegment(string(name), data)
	}
}

// checkFixture opens a store built from the recipe and reads it all back.
func checkFixture(t *testing.T, shim *storio.InMemoryPlatformIO, wantFramed bool) {
	t.Helper()
	before, _ := shim.ListSegments(fixtureNS)
	s := New(shim, fixtureNS, nil, Config{})
	recipe := fixtureRecipe()
	var oldHash codec.Hash
	for i, b := range recipe {
		h := codec.Hash{}
		{
			info, err := New(storio.NewInMemoryPlatformIO(), "x", nil, Config{}).Put(bytes.NewReader(b.data))
			if err != nil {
				t.Fatal(err)
			}
			h = info.Hash
		}
		if i == 4 {
			oldHash = h
		}
		if got := readBack(t, s, h); !bytes.Equal(got, b.data) {
			t.Fatalf("%s reads back wrong", b.name)
		}
		r, _ := s.Open(h)
		mid := make([]byte, 1000)
		if int64(len(b.data)) > 2000 {
			if _, err := r.ReadAt(mid, int64(len(b.data))/2); err != nil || !bytes.Equal(mid, b.data[len(b.data)/2:len(b.data)/2+1000]) {
				t.Fatalf("%s: ranged read: %v", b.name, err)
			}
		}
		r.Close()
	}
	s.mu.Lock()
	p := s.pointers[oldHash]
	s.mu.Unlock()
	if !p.delta {
		t.Fatal("the replaced version is not stored as a delta")
	}
	if p.framed != wantFramed {
		t.Fatalf("the delta is framed=%v, want %v (Go writes format 2, Kotlin format 1)", p.framed, wantFramed)
	}
	if mark, _ := s.Mark(); len(mark) != 2 || mark[0] != fixtureMark()[0] || mark[1] != fixtureMark()[1] {
		t.Fatalf("mark %v", mark)
	}
	if n, err := s.Collect(func(codec.Hash) bool { return true }); err != nil || n != 0 {
		t.Fatalf("collect removed %d: %v", n, err)
	}
	after, _ := shim.ListSegments(fixtureNS)
	if strings.Join(before, ",") != strings.Join(after, ",") {
		t.Fatalf("collect changed the segments:\n%v\n%v", before, after)
	}
}

func TestGoFixtureForKotlin(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "kdb-jdbc", "src", "test", "resources", "attachstore", "go-store.dump.gz")
	if os.Getenv("KDB_WRITE_FIXTURES") == "1" {
		dumpSegments(t, buildFixture(t), path)
	}
	checkFixture(t, loadDump(t, path), true) // what Kotlin reads stays readable here
}

func TestKotlinFixtureReadsInGo(t *testing.T) {
	checkFixture(t, loadDump(t, filepath.Join("testdata", "kotlin-store.dump.gz")), false)
}

// Golden encodings Kotlin's AttachmentStoreGoldenTest reproduces byte for byte.
func TestKotlinGoldenEncodings(t *testing.T) {
	var base codec.Hash
	base.Bytes[0] = 0xab
	at := time.Unix(0, 1760000000123456000)
	inline := []byte("hello attachments")
	got := map[string]string{
		"inline": hex.EncodeToString(encodePointer(pointer{size: int64(len(inline)), inline: inline, finished: at})),
		"whole":  hex.EncodeToString(encodePointer(pointer{segment: "ns/app/data/attachment/u-1", leaves: "ns/app/data/attachment/u-2", size: 3 << 20, finished: at})),
		"delta":  hex.EncodeToString(encodePointer(pointer{segment: "ns/app/data/attachment/u-3", leaves: "ns/app/data/attachment/u-2", size: 3 << 20, delta: true, base: base, finished: at})),
	}
	newer := fixtureText(400000, 7)
	var d []byte
	if err := encodeDeltaV1(memSource(newer), memSource(fixtureEdit(newer)), func(b []byte) error { d = append(d, b...); return nil }); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(d)
	got["delta-bytes"] = fmt.Sprintf("%d:%x", len(d), sum)
	want := map[string]string{
		"inline":      goldenInline,
		"whole":       goldenWhole,
		"delta":       goldenDelta,
		"delta-bytes": goldenDeltaBytes,
	}
	for k, v := range got {
		if want[k] != v {
			t.Errorf("%s: got %s, want %s", k, v, want[k])
		}
	}
}

const (
	goldenInline     = "4b444241545033020000000000000011186cc6acdc0bca00000068656c6c6f206174746163686d656e74730faab79d"
	goldenWhole      = "4b444241545033000000000000300000186cc6acdc0bca001a6e732f6170702f646174612f6174746163686d656e742f752d311a6e732f6170702f646174612f6174746163686d656e742f752d32200b1342"
	goldenDelta      = "4b444241545033010000000000300000186cc6acdc0bca001a6e732f6170702f646174612f6174746163686d656e742f752d331a6e732f6170702f646174612f6174746163686d656e742f752d32ab0000000000000000000000000000000000000000000000000000000000000035950ee4"
	goldenDeltaBytes = "13956:2fd6239864c3eb31fb49521d4d4862edb07007d25d32d80ab52d4eede3bdaa1f"
)
