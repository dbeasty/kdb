package wire

import (
	"encoding/json"
	"slices"
	"sync"

	"github.com/limidus/kdb/go/kdb/compression"
)

// Codec encodes and decodes length-prefixed wire frames.
type Codec interface {
	Encoding() PayloadEncoding
	Encode(message Message) ([]byte, error)
	Decode(frame []byte) (Message, error)
	EncodeFrameOnly(header Header, payload []byte) ([]byte, error)
	DecodeHeader(frame []byte) (Header, error)
}

// DefaultCodec is the standard wire codec. It encodes the JSON envelope unless built by
// WithWireFeatures with binary frames, and decodes every encoding tag regardless.
type DefaultCodec struct {
	encoding PayloadEncoding
	// binary encodes the core messages as tag-2 bodies (WireFeatureBinary); binary2 the rest of
	// the client set too (WireFeatureBinary2).
	binary  bool
	binary2 bool
	// zstd compresses binary bodies of zstdFrameThreshold bytes or more as tag-3 frames.
	zstd bool
}

// NewCodec returns the default wire codec for the given payload encoding tag.
func NewCodec(encoding PayloadEncoding) *DefaultCodec {
	return &DefaultCodec{encoding: encoding}
}

func (c *DefaultCodec) Encoding() PayloadEncoding { return c.encoding }

// WithWireFeatures returns a codec like c that also encodes in the negotiated features' formats -
// the list a handshake ack carries (NegotiateWireFeatures). Unknown features are ignored, and an
// empty list returns a JSON-only codec.
func (c *DefaultCodec) WithWireFeatures(features []string) *DefaultCodec {
	out := *c
	out.binary = slices.Contains(features, WireFeatureBinary)
	out.binary2 = out.binary && slices.Contains(features, WireFeatureBinary2)
	out.zstd = out.binary && slices.Contains(features, WireFeatureZstd)
	return &out
}

// BinaryFrames reports whether c encodes binary bodies.
func (c *DefaultCodec) BinaryFrames() bool { return c.binary }

// Encode returns one complete frame in a buffer of its own, which the caller may keep.
//
// The JSON envelope is encoded straight into the frame buffer, after a reserved 12-byte header
// and the encoding tag, and the header is patched in once the length is known. It used to build
// three buffers per reply - json.Marshal's result, a tagged payload copy, and the frame copy -
// so every reply paid two full extra copies of its body.
func (c *DefaultCodec) Encode(message Message) ([]byte, error) {
	fw := frameWriter{}
	if err := c.encodeInto(&fw, message); err != nil {
		return nil, err
	}
	return fw.buf, nil
}

// EncodePooled is Encode into a pooled buffer. The returned release must be called exactly once,
// after the last use of frame; frame must not be touched after that.
//
// Only use it where the frame's lifetime is provably bounded - a Send that has finished with the
// slice when it returns (core.SynchronousSender). A transport that retains the slice after Send
// returns (gRPC's ServerStream.SendMsg, for one, documents that the message must not be modified
// afterwards) must get a frame from Encode instead.
func (c *DefaultCodec) EncodePooled(message Message) (frame []byte, release func(), err error) {
	fw := framePool.Get().(*frameWriter)
	if err := c.encodeInto(fw, message); err != nil {
		fw.release()
		return nil, nil, err
	}
	return fw.buf, fw.release, nil
}

func (c *DefaultCodec) encodeInto(fw *frameWriter, message Message) error {
	if t := message.Header().MessageType; c.binary && coreBinaryForm(t) || c.binary2 && moreBinaryForm(t) || attachmentBinaryForm(t) {
		if hint := FrameHeaderSize + 1 + binarySizeHint(message); cap(fw.buf) < hint {
			fw.buf = make([]byte, FrameHeaderSize, hint)
		} else {
			fw.reset()
		}
		fw.buf = append(fw.buf, tagBinary)
		var ok bool
		if fw.buf, ok = appendBinaryBody(fw.buf, message); ok {
			if c.zstd && len(fw.buf)-FrameHeaderSize-1 >= zstdFrameThreshold {
				if err := compressBody(fw); err != nil {
					return err
				}
			}
			return finishFrame(fw, message)
		}
	}
	if page, ok := message.(SnapshotPageMessage); ok {
		fw.reset()
		fw.buf = append(fw.buf, encodingTag(c.encoding))
		var err error
		if fw.buf, err = appendSnapshotPageJSON(fw.buf, page); err != nil {
			return err
		}
		return finishFrame(fw, message)
	}
	env, err := messageToEnvelope(message)
	if err != nil {
		return err
	}
	fw.reset()
	fw.buf = append(fw.buf, encodingTag(c.encoding))
	// json.Encoder writes the pooled encodeState's bytes to fw in one Write, so the body is
	// copied exactly once, into the frame. Its output is json.Marshal's byte for byte (both
	// escape HTML) plus a trailing newline, trimmed below.
	enc := json.NewEncoder(fw)
	if err := enc.Encode(env); err != nil {
		return err
	}
	if n := len(fw.buf); n > 0 && fw.buf[n-1] == '\n' {
		fw.buf = fw.buf[:n-1]
	}
	return finishFrame(fw, message)
}

// finishFrame checks the frame's length and patches the header in.
func finishFrame(fw *frameWriter, message Message) error {
	totalLength := len(fw.buf)
	if err := ValidateFrameLength(totalLength, DefaultMaxFrameBytes); err != nil {
		return err
	}
	h := message.Header()
	writeInt32LE(fw.buf, 0, int32(totalLength))
	writeInt16LE(fw.buf, 4, int16(h.MessageType))
	writeInt16LE(fw.buf, 6, int16(h.ProtocolVersion))
	writeInt32LE(fw.buf, 8, int32(h.CorrelationID))
	return nil
}

// compressBody replaces fw's tag-2 body with a tag-3 zstd frame of it, when that is smaller. The
// frame is built in a second pooled buffer and the two are swapped, so a steady stream of large
// replies allocates nothing for compression.
func compressBody(fw *frameWriter) error {
	tmp := framePool.Get().(*frameWriter)
	tmp.reset()
	tmp.buf = append(tmp.buf, tagBinaryZstd)
	body := fw.buf[FrameHeaderSize+1:]
	out, err := compression.CompressAppend(tmp.buf, body, frameZstdLevel)
	if err != nil {
		tmp.release()
		return err
	}
	tmp.buf = out
	if len(out) < len(fw.buf) {
		fw.buf, tmp.buf = tmp.buf, fw.buf
	}
	tmp.release()
	return nil
}

// frameWriter is the io.Writer a frame is encoded into: a byte slice that starts with the
// reserved header.
type frameWriter struct {
	buf []byte
}

func (w *frameWriter) reset() {
	if cap(w.buf) < FrameHeaderSize+1 {
		// Room for the header and the tag only: the body's single Write then grows the slice to
		// its final size in one step, so an unpooled frame costs one allocation of its own size
		// (plus this small one), not a doubling series.
		w.buf = make([]byte, FrameHeaderSize, 16)
		return
	}
	w.buf = w.buf[:FrameHeaderSize]
}

func (w *frameWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// maxPooledFrameBytes bounds the buffers kept for reuse. A pool that kept whatever it was handed
// would pin the largest reply ever sent (up to 16 MiB) for every concurrent sender until the next
// GC; replies bigger than this are rare, so they are left to the allocator.
const maxPooledFrameBytes = 1 << 20

var framePool = sync.Pool{New: func() any { return &frameWriter{} }}

func (w *frameWriter) release() {
	if cap(w.buf) > maxPooledFrameBytes {
		w.buf = nil
	} else {
		w.buf = w.buf[:0]
	}
	framePool.Put(w)
}

func (c *DefaultCodec) Decode(frame []byte) (Message, error) {
	header, err := DecodeHeader(frame)
	if err != nil {
		return nil, err
	}
	payloadOffset := FrameHeaderSize
	if len(frame) < payloadOffset+1 {
		return nil, newDecodeError("frame too short for payload")
	}
	// A frame declaring an empty payload while carrying trailing bytes passes the check above
	// but would slice frame[13:12] - low greater than high, which panics just as surely as an
	// out-of-range high bound does.
	if header.PayloadLength < 1 {
		return nil, newDecodeError("frame declares an empty payload")
	}
	tag := frame[payloadOffset]
	body := frame[payloadOffset+1 : payloadOffset+header.PayloadLength]
	switch tag {
	case 0, 1:
	case tagBinary:
		return decodeBinaryBody(header, body)
	case tagBinaryZstd:
		// decodeBinaryBody copies every field out of the body, so the decompressed body can live in
		// a pooled buffer.
		tmp := framePool.Get().(*frameWriter)
		defer tmp.release()
		raw, err := compression.DecompressInto(tmp.buf[:0], body, DefaultMaxFrameBytes)
		if err != nil {
			return nil, newDecodeError("invalid zstd frame body")
		}
		tmp.buf = raw
		return decodeBinaryBody(header, raw)
	default:
		return nil, newDecodeError("unsupported encoding tag")
	}
	var env PayloadEnvelope
	if err := unmarshalJSON(body, &env); err != nil {
		return nil, newDecodeError("invalid payload json")
	}
	return envelopeToMessage(header, env)
}

func (c *DefaultCodec) EncodeFrameOnly(header Header, payload []byte) ([]byte, error) {
	return EncodeFrameOnly(header, payload)
}

func (c *DefaultCodec) DecodeHeader(frame []byte) (Header, error) {
	return DecodeHeader(frame)
}
