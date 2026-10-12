package delta

import (
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// countingShim counts the segment bytes read through it. Embedding keeps every optional
// interface of the shim beneath - SegmentStamper included - so the reader sees a real one.
type countingShim struct {
	*storio.FileBackedPlatformIO
	read atomic.Int64
}

func (c *countingShim) ReadFromSegment(name string, off int64, n int) ([]byte, error) {
	b, err := c.FileBackedPlatformIO.ReadFromSegment(name, off, n)
	c.read.Add(int64(len(b)))
	return b, err
}

func diskShim(t *testing.T) (*storio.FileBackedPlatformIO, string) {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store), root
}

// Listing the log used to walk every frame header of every segment on every call - several calls
// per checkpoint, the whole log each time. A sealed segment does not change, so the second
// listing must read nothing and answer the same.
func TestListSegmentsDoesNotRescanSealedSegments(t *testing.T) {
	fb, _ := diskShim(t)
	cfg := newConfig(fb, storage.CompressionNone)
	writeChainAcross(t, cfg, []int64{0, 1, 2}, 5)

	cs := &countingShim{FileBackedPlatformIO: fb}
	r := NewDefaultReader(testNS, cs, cfg)
	first, err := r.ListSegments()
	if err != nil {
		t.Fatal(err)
	}
	if cs.read.Load() == 0 {
		t.Fatal("the first listing read nothing - the scan is not being exercised")
	}
	cs.read.Store(0)
	second, err := r.ListSegments()
	if err != nil {
		t.Fatal(err)
	}
	if n := cs.read.Load(); n != 0 {
		t.Fatalf("second listing of unchanged sealed segments read %d bytes", n)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("cached listing differs:\n first  %+v\n second %+v", first, second)
	}
	seqs, err := r.SegmentSequences()
	if err != nil || !reflect.DeepEqual(seqs, []int64{0, 1, 2}) {
		t.Fatalf("SegmentSequences = %v, %v", seqs, err)
	}
}

// A sealed segment changed in place - same size, outside this process, the way damage arrives -
// must be scanned again, not answered from the cache. Otherwise a checkpoint would fingerprint the
// segment as it was and the damage would go unnoticed.
func TestListSegmentsRescansASegmentChangedInPlace(t *testing.T) {
	fb, root := diskShim(t)
	cfg := newConfig(fb, storage.CompressionNone)
	writeChainAcross(t, cfg, []int64{0, 1}, 4)
	r := NewDefaultReader(testNS, fb, cfg)
	if _, err := r.ListSegments(); err != nil { // warm the cache
		t.Fatal(err)
	}

	path := filepath.Join(root, filepath.FromSlash(segName(1)))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	frames := segmentFrames(t, raw)
	raw[frames[len(frames)-1]+frameHeaderSize+1] ^= 0xFF // last frame's body: its CRC now fails
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := r.ListSegments()
	if err != nil {
		t.Fatal(err)
	}
	want, err := NewDefaultReader(testNS, fb, cfg).ListSegments()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listing after in-place damage came from the cache:\n got  %+v\n want %+v", got, want)
	}
	if got[1].Tail.Kind == storage.TailClean {
		t.Fatal("the damaged segment still lists as clean")
	}
}

// A fingerprint remembered under a stamp is answered without reading the segment, and only while
// the segment still has that stamp.
func TestRememberedFingerprintNeedsTheSameStamp(t *testing.T) {
	fb, root := diskShim(t)
	cfg := newConfig(fb, storage.CompressionNone)
	writeChainAcross(t, cfg, []int64{0}, 3)
	cs := &countingShim{FileBackedPlatformIO: fb}
	r := NewDefaultReader(testNS, cs, cfg)
	st, ok, err := r.StampSegment(0)
	if err != nil || !ok {
		t.Fatalf("cannot stamp: %v %v", ok, err)
	}
	var last codec.Hash
	last.Bytes[0] = 7
	r.RememberFingerprint(0, st, 123, last)
	if _, size, got, ok := r.RememberedFingerprint(0); !ok || size != 123 || got != last {
		t.Fatalf("remembered fingerprint not returned: %v %d", ok, size)
	}
	if n := cs.read.Load(); n != 0 {
		t.Fatalf("answering a remembered fingerprint read %d bytes", n)
	}
	path := filepath.Join(root, filepath.FromSlash(segName(0)))
	raw, _ := os.ReadFile(path)
	if err := os.WriteFile(path, raw, 0o644); err != nil { // same bytes, new mtime
		t.Fatal(err)
	}
	if _, _, _, ok := r.RememberedFingerprint(0); ok {
		t.Fatal("a fingerprint was returned for a segment whose stamp changed")
	}
}
