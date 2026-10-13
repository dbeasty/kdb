package core

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/limidus/kdb/go/kdb/wire"
)

// DefaultReadBufferBytes is the bufio buffer a FrameReader reads the socket through. 64 KiB
// absorbs a pipelining client's small frames in one read syscall; frames larger than it are
// read straight into their own buffer, bypassing it.
const DefaultReadBufferBytes = 64 << 10

// eagerFrameBytes is the largest body a FrameReader allocates in full as soon as the header
// declares it. A larger declared body is grown as its bytes actually arrive, so a peer cannot
// make the server commit 16 MiB per connection by sending a 4-byte length and then nothing.
const eagerFrameBytes = 1 << 20

// FrameReader reads length-prefixed wire frames from a byte stream.
//
// It replaces the chunk-and-reassemble path (FrameStreamReader fed fixed 4 KiB reads), which
// appended every read to a pending buffer and then copied each frame out of it: two copies of
// every inbound byte, and a pending buffer that grew to the largest frame seen. FrameReader reads
// the 12-byte header, decides admission on it, then reads the body straight into one buffer of
// exactly the frame's size - one copy, from the bufio buffer (or none, for bodies larger than it,
// which bufio reads directly into the destination).
//
// Admission works exactly as it does in FrameStreamReader: the admitter sees the header before a
// single body byte is buffered, and a shed frame's body is discarded through the bufio buffer
// without being assembled.
type FrameReader struct {
	br            *bufio.Reader
	maxFrameBytes int
	admit         FrameAdmitter
	hdr           [wire.FrameHeaderSize]byte
	// skip is the body of a shed frame still to be discarded. It is dropped at the start of the
	// next call rather than before returning the rejection, so the refusal can be written back
	// while the shed body is still arriving.
	skip int
}

// NewFrameReader returns a FrameReader over r. admit may be nil.
func NewFrameReader(r io.Reader, maxFrameBytes int, admit FrameAdmitter) *FrameReader {
	return NewFrameReaderSize(r, maxFrameBytes, admit, DefaultReadBufferBytes)
}

// NewFrameReaderSize is NewFrameReader with an explicit read buffer size.
func NewFrameReaderSize(r io.Reader, maxFrameBytes int, admit FrameAdmitter, bufferBytes int) *FrameReader {
	if maxFrameBytes == 0 {
		maxFrameBytes = wire.DefaultMaxFrameBytes
	}
	if bufferBytes <= 0 {
		bufferBytes = DefaultReadBufferBytes
	}
	return &FrameReader{br: bufio.NewReaderSize(r, bufferBytes), maxFrameBytes: maxFrameBytes, admit: admit}
}

// Next reads the next frame. Exactly one of frame and rejection is non-nil on success: frame is
// an admitted frame the caller owns; rejection is the reply to send for a frame the admitter
// shed, returned as soon as its header has arrived (its body is discarded, never assembled, at the
// start of the next call). A shed frame whose admitter built no reply returns (nil, nil, nil) -
// call Next again.
//
// io.EOF is returned only at a clean frame boundary; a stream that ends mid-frame returns
// io.ErrUnexpectedEOF. A declared length outside the protocol bounds returns the wire package's
// FrameTooLargeError, after which the stream cannot be resynchronized.
func (r *FrameReader) Next() (frame []byte, rejection []byte, err error) {
	if r.skip > 0 {
		n := r.skip
		r.skip = 0
		if _, err := r.br.Discard(n); err != nil {
			return nil, nil, unexpected(err)
		}
	}
	if _, err := io.ReadFull(r.br, r.hdr[:4]); err != nil {
		return nil, nil, err
	}
	frameLength := int(int32(binary.LittleEndian.Uint32(r.hdr[:4])))
	if err := wire.ValidateFrameLength(frameLength, r.maxFrameBytes); err != nil {
		return nil, nil, err
	}
	// ValidateFrameLength guarantees frameLength >= FrameHeaderSize, so the rest of the header
	// is part of this frame.
	if _, err := io.ReadFull(r.br, r.hdr[4:]); err != nil {
		return nil, nil, unexpected(err)
	}
	if r.admit != nil {
		header, ok, perr := wire.PeekHeader(r.hdr[:], r.maxFrameBytes)
		// An unparseable header is not an admission decision - it is left to the normal path,
		// which produces the established decode error once the frame is whole.
		if perr == nil && ok {
			if rej, aerr := r.admit(header); aerr != nil {
				r.skip = frameLength - wire.FrameHeaderSize
				return nil, rej, nil
			}
		}
	}
	frame, err = r.readBody(frameLength)
	if err != nil {
		return nil, nil, unexpected(err)
	}
	return frame, nil, nil
}

func (r *FrameReader) readBody(frameLength int) ([]byte, error) {
	if frameLength <= eagerFrameBytes {
		frame := make([]byte, frameLength)
		copy(frame, r.hdr[:])
		if _, err := io.ReadFull(r.br, frame[wire.FrameHeaderSize:]); err != nil {
			return nil, err
		}
		return frame, nil
	}
	// Grow with the bytes that actually arrive, doubling up to the declared size.
	frame := make([]byte, wire.FrameHeaderSize, eagerFrameBytes)
	copy(frame, r.hdr[:])
	for len(frame) < frameLength {
		if len(frame) == cap(frame) {
			next := make([]byte, len(frame), min(2*cap(frame), frameLength))
			copy(next, frame)
			frame = next
		}
		n, err := io.ReadFull(r.br, frame[len(frame):cap(frame)])
		frame = frame[:len(frame)+n]
		if err != nil {
			return nil, err
		}
	}
	return frame, nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
