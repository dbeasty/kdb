package core

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"runtime"
	"testing"
	"testing/iotest"
	"time"

	"github.com/limidus/kdb/go/kdb/wire"
)

// chunkReader hands out the stream in pseudo-random chunk sizes, so headers and bodies are
// split across reads at every kind of offset.
type chunkReader struct {
	data []byte
	rng  *rand.Rand
	max  int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := 1 + c.rng.Intn(c.max)
	n = min(n, len(p), len(c.data))
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

func frameSizes() []int {
	sizes := []int{0, 1, 2, 3, 11, 12, 13, 100, 4095, 4096, 4097}
	for _, b := range []int{DefaultReadBufferBytes, eagerFrameBytes} {
		sizes = append(sizes, b-wire.FrameHeaderSize-1, b-wire.FrameHeaderSize, b-wire.FrameHeaderSize+1, b+1)
	}
	return append(sizes, 3<<20+17)
}

func patterned(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + seed)
	}
	return b
}

func TestFrameReaderRoundTripsManySizesAcrossSplitReads(t *testing.T) {
	build := func(sizes []int) ([]byte, [][]byte) {
		var stream []byte
		var want [][]byte
		for i, size := range sizes {
			f := mustFrame(t, wire.MsgSqlExec, i, patterned(size, i))
			want = append(want, f)
			stream = append(stream, f...)
		}
		return stream, want
	}
	// Byte-at-a-time readers get the small sizes only (every header split at every offset);
	// the large sizes go through readers that still split frames, at realistic chunk sizes.
	var small, all []int
	for _, s := range frameSizes() {
		if s <= DefaultReadBufferBytes+1 {
			small = append(small, s)
		}
		all = append(all, s)
	}
	smallStream, smallWant := build(small)
	allStream, allWant := build(all)
	type tc struct {
		mk   func() io.Reader
		want [][]byte
	}
	cases := map[string]tc{
		"one-byte":  {func() io.Reader { return iotest.OneByteReader(bytes.NewReader(smallStream)) }, smallWant},
		"chunks-7":  {func() io.Reader { return &chunkReader{data: smallStream, rng: rand.New(rand.NewSource(1)), max: 7} }, smallWant},
		"whole":     {func() io.Reader { return bytes.NewReader(allStream) }, allWant},
		"chunks-9k": {func() io.Reader { return &chunkReader{data: allStream, rng: rand.New(rand.NewSource(2)), max: 9000} }, allWant},
		"half":      {func() io.Reader { return iotest.HalfReader(bytes.NewReader(allStream)) }, allWant},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewFrameReader(c.mk(), wire.DefaultMaxFrameBytes, nil)
			for i, w := range c.want {
				got, rej, err := r.Next()
				if err != nil || rej != nil {
					t.Fatalf("frame %d: err=%v rejection=%v", i, err, rej)
				}
				if !bytes.Equal(got, w) {
					t.Fatalf("frame %d (%d bytes): content differs", i, len(w))
				}
				if len(got) != cap(got) && len(got) <= eagerFrameBytes {
					t.Fatalf("frame %d: buffer is not exactly sized (len %d cap %d)", i, len(got), cap(got))
				}
			}
			if _, _, err := r.Next(); err != io.EOF {
				t.Fatalf("want io.EOF at a clean boundary, got %v", err)
			}
		})
	}
}

func TestFrameReaderReadsAMaxSizeFrame(t *testing.T) {
	f := mustFrame(t, wire.MsgSqlExec, 1, patterned(wire.DefaultMaxFrameBytes-wire.FrameHeaderSize, 3))
	r := NewFrameReader(&chunkReader{data: f, rng: rand.New(rand.NewSource(3)), max: 1 << 20}, wire.DefaultMaxFrameBytes, nil)
	got, _, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, f) {
		t.Fatal("max-size frame differs")
	}
}

func TestFrameReaderRejectsAnOversizedDeclaredLengthWithoutReadingTheBody(t *testing.T) {
	var hdr [wire.FrameHeaderSize]byte
	writeLen(hdr[:], wire.DefaultMaxFrameBytes+1)
	src := &countingReader{r: io.MultiReader(bytes.NewReader(hdr[:]), neverEnding{})}
	r := NewFrameReaderSize(src, wire.DefaultMaxFrameBytes, nil, 16)
	_, _, err := r.Next()
	var tooLarge *wire.FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("want FrameTooLargeError, got %v", err)
	}
	if src.n > 16 {
		t.Fatalf("read %d bytes past an invalid length prefix", src.n)
	}
}

func TestFrameReaderEndingMidFrameIsUnexpectedEOF(t *testing.T) {
	f := mustFrame(t, wire.MsgSqlExec, 1, patterned(500, 0))
	for _, cut := range []int{2, 4, 8, 12, 13, 400} {
		r := NewFrameReader(bytes.NewReader(f[:cut]), wire.DefaultMaxFrameBytes, nil)
		if _, _, err := r.Next(); err != io.ErrUnexpectedEOF {
			t.Fatalf("cut at %d: want io.ErrUnexpectedEOF, got %v", cut, err)
		}
	}
}

// A peer that declares a large frame and sends little of it must not make the reader allocate
// the declared size up front.
func TestFrameReaderDoesNotAllocateTheDeclaredSizeOfAnIncompleteFrame(t *testing.T) {
	f := mustFrame(t, wire.MsgSqlExec, 1, patterned(wire.DefaultMaxFrameBytes-wire.FrameHeaderSize, 0))
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	r := NewFrameReader(bytes.NewReader(f[:64<<10]), wire.DefaultMaxFrameBytes, nil)
	if _, _, err := r.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("want io.ErrUnexpectedEOF, got %v", err)
	}
	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 4<<20 {
		t.Fatalf("allocated %d bytes for a 64 KiB prefix of a 16 MiB frame", grew)
	}
}

// Admission still happens on the header alone: the admitter runs while the body has not even
// been written to the connection, the shed body is discarded, and the next frame is read intact.
func TestFrameReaderShedsOnTheHeaderBeforeTheBodyArrives(t *testing.T) {
	pr, pw := io.Pipe()
	shed := errors.New("shed")
	decided := make(chan wire.Header, 4)
	r := NewFrameReader(pr, wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		decided <- h
		if h.MessageType == wire.MsgUpsert {
			return []byte("REJECTED"), shed
		}
		return nil, nil
	})
	big := mustFrame(t, wire.MsgUpsert, 42, patterned(8<<20, 1))
	next := mustFrame(t, wire.MsgSqlExec, 43, []byte("{}"))

	type result struct {
		frame, rej []byte
		err        error
	}
	results := make(chan result, 2)
	go func() {
		for i := 0; i < 2; i++ {
			f, rej, err := r.Next()
			results <- result{f, rej, err}
		}
	}()
	if _, err := pw.Write(big[:wire.FrameHeaderSize]); err != nil {
		t.Fatal(err)
	}
	select {
	case h := <-decided:
		if h.CorrelationID != 42 || h.PayloadLength != 8<<20 {
			t.Fatalf("admitter saw %+v", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admitter was not consulted on the header alone")
	}
	res := <-results
	if res.err != nil || string(res.rej) != "REJECTED" || res.frame != nil {
		t.Fatalf("want the rejection for the shed frame, got %+v", res)
	}
	go func() {
		_, _ = pw.Write(big[wire.FrameHeaderSize:])
		_, _ = pw.Write(next)
	}()
	res = <-results
	if res.err != nil || !bytes.Equal(res.frame, next) {
		t.Fatalf("stream did not resynchronize after the shed frame: %+v", res.err)
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

type neverEnding struct{}

func (neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func writeLen(b []byte, n int) {
	b[0], b[1], b[2], b[3] = byte(n), byte(n>>8), byte(n>>16), byte(n>>24)
}

func BenchmarkFrameReaderSmallFrames(b *testing.B) {
	f, _ := wire.EncodeFrameOnly(wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion}, patterned(300, 0))
	stream := bytes.Repeat(f, 1000)
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))
	for i := 0; i < b.N; i++ {
		r := NewFrameReader(bytes.NewReader(stream), wire.DefaultMaxFrameBytes, nil)
		for {
			if _, _, err := r.Next(); err != nil {
				break
			}
		}
	}
}
