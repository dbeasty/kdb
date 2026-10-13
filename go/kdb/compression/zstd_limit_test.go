package compression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// allocatedDuring reports how many bytes fn allocated.
func allocatedDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// hugeFrame is a zstd frame whose header declares a content size of declared bytes, followed by
// one small RLE block - a corrupt or hostile frame, the shape the limit exists for.
func hugeFrame(declared uint64) []byte {
	f := []byte{0x28, 0xB5, 0x2F, 0xFD}
	// Frame_Header_Descriptor: FCS field 8 bytes (11), Single_Segment (1), no checksum, no dict.
	f = append(f, 0xE0)
	f = binary.LittleEndian.AppendUint64(f, declared)
	// Block header: last block, RLE, size 16; then the byte to repeat.
	hdr := uint32(1) | uint32(1)<<1 | uint32(16)<<3
	f = append(f, byte(hdr), byte(hdr>>8), byte(hdr>>16), 'x')
	return f
}

// TestDecompressRefusesADeclaredSizeOverTheLimitWithoutAllocatingIt: a frame claiming 1 GiB
// against a 64 KiB limit must be refused from its header. The old path handed it to
// DecodeAll(input, nil), which sizes its destination from the declared content size - a 1 GiB
// allocation before a byte was decoded - and checked the limit only afterwards.
func TestDecompressRefusesADeclaredSizeOverTheLimitWithoutAllocatingIt(t *testing.T) {
	frame := hugeFrame(1 << 30)
	var hdr zstd.Header
	if err := hdr.Decode(frame); err != nil || !hdr.HasFCS || hdr.FrameContentSize != 1<<30 {
		t.Fatalf("test frame header does not parse as intended: %+v %v", hdr, err)
	}
	var err error
	n := allocatedDuring(func() { _, err = Decompress(frame, 64<<10) })
	if !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("got %v, want ErrOutputTooLarge", err)
	}
	if n > 1<<20 {
		t.Fatalf("refusing the frame allocated %d bytes", n)
	}
}

// TestDecompressRefusesARealFrameOverTheLimitBeforeDecoding uses a genuine frame (1 MiB of
// zeros, a few dozen bytes compressed) rather than a hand-made one, so the check is known to
// fire on what the encoder actually writes.
func TestDecompressRefusesARealFrameOverTheLimitBeforeDecoding(t *testing.T) {
	compressed, err := Compress(make([]byte, 1<<20), 3)
	if err != nil {
		t.Fatal(err)
	}
	var hdr zstd.Header
	if err := hdr.Decode(compressed); err != nil || !hdr.HasFCS {
		t.Fatalf("Compress wrote no content size: %+v %v", hdr, err)
	}
	n := allocatedDuring(func() { _, err = Decompress(compressed, 1000) })
	if !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("got %v, want ErrOutputTooLarge", err)
	}
	if n > 64<<10 {
		t.Fatalf("refusing a 1 MiB frame against a 1000-byte limit allocated %d bytes", n)
	}
}

// TestDecompressFrameThatLiesAboutItsSize: a frame declaring less than it holds fails (the
// decoder stops it within a block of its declared size) rather than being accepted.
func TestDecompressFrameThatLiesAboutItsSize(t *testing.T) {
	frame := hugeFrame(4) // declares 4 bytes, its block decodes to 16
	if _, err := Decompress(frame, 1<<20); err == nil {
		t.Fatal("a frame decoding past its declared size was accepted")
	}
}

func TestDecompressIntoReusesALargeEnoughDestination(t *testing.T) {
	input := bytes.Repeat([]byte("reuse me "), 1000)
	compressed, err := Compress(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, 2*len(input))
	out, err := DecompressInto(dst, compressed, len(input))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, input) {
		t.Fatal("round trip mismatch")
	}
	if &out[:1][0] != &dst[:1][0] {
		t.Fatal("decoded into a new buffer although dst had room")
	}
	// Too small a destination is replaced by one of exactly the decoded size.
	out, err = DecompressInto(make([]byte, 0, 10), compressed, len(input))
	if err != nil || !bytes.Equal(out, input) || cap(out) != len(input) {
		t.Fatalf("small dst: err %v, len %d cap %d", err, len(out), cap(out))
	}
}

func TestDecompressExactSizeDestination(t *testing.T) {
	input := bytes.Repeat([]byte("abc"), 5000)
	compressed, err := Compress(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decompress(compressed, len(input))
	if err != nil || !bytes.Equal(out, input) {
		t.Fatalf("err %v", err)
	}
	if cap(out) != len(input) {
		t.Fatalf("destination capacity %d, want exactly %d", cap(out), len(input))
	}
}

func TestDecompressEmpty(t *testing.T) {
	compressed, err := Compress(nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decompress(compressed, 0)
	if err != nil || len(out) != 0 {
		t.Fatalf("empty round trip: %q %v", out, err)
	}
}

// TestDecompressFramesWithoutADeclaredSize covers the fallback: streaming encoders do not know
// the size up front and omit it, and concatenated frames carry more than the first declares.
// Both still decode, and the limit still holds - after decoding, as nothing says the size
// before.
func TestDecompressFramesWithoutADeclaredSize(t *testing.T) {
	// Written in pieces larger than the encoder's buffer, so it starts the frame before it
	// knows the total and cannot declare one.
	input := bytes.Repeat([]byte("streamed "), 300_000)
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(input); off += 64 << 10 {
		end := min(off+64<<10, len(input))
		if _, err := enc.Write(input[off:end]); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	var hdr zstd.Header
	if err := hdr.Decode(buf.Bytes()); err != nil || hdr.HasFCS {
		t.Fatalf("expected a frame with no content size: %+v %v", hdr, err)
	}
	out, err := Decompress(buf.Bytes(), len(input))
	if err != nil || !bytes.Equal(out, input) {
		t.Fatalf("no-FCS frame: %v", err)
	}
	if _, err := Decompress(buf.Bytes(), len(input)-1); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("no-FCS frame over the limit: %v", err)
	}

	a, _ := Compress([]byte("first frame "), 3)
	b, _ := Compress([]byte("second frame"), 3)
	out, err = Decompress(append(append([]byte(nil), a...), b...), 100)
	if err != nil || string(out) != "first frame second frame" {
		t.Fatalf("two frames: %q %v", out, err)
	}
}
