package wire

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The binary frame encoding (B4 in docs/kdb-memory-cassandra-plan.md).
//
// A frame's payload starts with a one-byte encoding tag. Tags 0 and 1 are the JSON envelope; this
// file adds two more:
//
//	tag 2  binary body: the message's fields, in the protobuf wire format
//	tag 3  a single zstd frame (content size declared) that decompresses to a tag-2 body
//
// The message type is already in the frame header, so a binary body carries no "kind". Each field
// is a key - uvarint(fieldNumber<<3 | wireType) - then its value: a uvarint for wire type 0, a
// uvarint length and that many raw bytes for wire type 2. Strings and byte fields are raw: a
// document body crosses the wire as itself, not as a JSON string escaped inside a JSON envelope,
// and a byte field is not a JSON array of numbers. A reader skips any field number it does not
// know, so fields can be added the same way JSON fields were - additively - without a new tag.
//
// Field conventions, shared with the Kotlin and TS codecs:
//   - an absent field is the zero value: "", 0, false, empty bytes, nil list;
//   - an optional (pointer) field is present exactly when non-nil, even if its value is "";
//   - integers are varints of their two's-complement int64 value, as protobuf's int64.
//
// Which messages have a binary form is binaryMessage's switch; every other message keeps the JSON
// envelope on every connection. A decoder accepts every tag, whatever was negotiated, so a frame
// built without the connection's codec - an admission refusal, say - still decodes.
//
// Neither side sends tag 2 or 3 until the handshake ack lists the matching feature (see
// NegotiateWireFeatures), so a node of this release still talks JSON to an older one, and to the
// Kotlin server, which acks no features.

const (
	// WireFeatureBinary: the peer reads and writes tag-2 binary bodies of the core load-path
	// messages (coreBinaryForm).
	WireFeatureBinary = "binary-frames"
	// WireFeatureBinary2: the peer also reads and writes binary bodies of the rest of the client
	// message set - leases, search, history, revert, cross-namespace commit and DeltaCommit
	// (moreBinaryForm). A separate feature because clients that shipped with WireFeatureBinary
	// cannot decode those bodies. Only meaningful with WireFeatureBinary.
	WireFeatureBinary2 = "binary-frames-2"
	// WireFeatureZstd: the peer reads tag-3 frames, zstd-compressed binary bodies. Only meaningful
	// with WireFeatureBinary.
	WireFeatureZstd = "zstd-frames"

	tagBinary     byte = 2
	tagBinaryZstd byte = 3

	// zstdFrameThreshold is the smallest binary body worth compressing. Below about 8 KiB the
	// saving is a few hundred bytes and the encoder's fixed cost dominates (B7).
	zstdFrameThreshold = 8 << 10
	// frameZstdLevel is the fastest level: replies are compressed on the request path.
	frameZstdLevel = 1
)

// SupportedWireFeatures is every feature this build implements, in the order a client offers them.
var SupportedWireFeatures = []string{WireFeatureBinary, WireFeatureBinary2, WireFeatureZstd}

// NegotiateWireFeatures returns the features in offered that this build supports, in this build's
// order. A server acks with it; a client passes the ack's list to DefaultCodec.WithWireFeatures.
// The other features are dropped without binary frames, since they extend or compress them.
func NegotiateWireFeatures(offered []string) []string {
	var out []string
	for _, f := range SupportedWireFeatures {
		if slices.Contains(offered, f) {
			out = append(out, f)
		}
	}
	if !slices.Contains(out, WireFeatureBinary) {
		return nil
	}
	return out
}

const (
	wtVarint  = 0
	wtFixed64 = 1
	wtBytes   = 2
	wtFixed32 = 5
)

func appendKey(b []byte, field int, wt int) []byte {
	return binary.AppendUvarint(b, uint64(field)<<3|uint64(wt))
}

// appendInt writes v unless it is zero.
func appendInt(b []byte, field int, v int64) []byte {
	if v == 0 {
		return b
	}
	b = appendKey(b, field, wtVarint)
	return binary.AppendUvarint(b, uint64(v))
}

func appendOptInt(b []byte, field int, v *int) []byte {
	if v == nil {
		return b
	}
	b = appendKey(b, field, wtVarint)
	return binary.AppendUvarint(b, uint64(int64(*v)))
}

func appendBool(b []byte, field int, v bool) []byte {
	if !v {
		return b
	}
	return append(appendKey(b, field, wtVarint), 1)
}

// appendString writes s unless it is empty.
func appendString(b []byte, field int, s string) []byte {
	if s == "" {
		return b
	}
	return appendRawString(b, field, s)
}

// appendRawString writes s even when it is empty - for an element of a list, or an optional field
// whose presence is the point.
func appendRawString(b []byte, field int, s string) []byte {
	b = appendKey(b, field, wtBytes)
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendOptString(b []byte, field int, s *string) []byte {
	if s == nil {
		return b
	}
	return appendRawString(b, field, *s)
}

func appendBytes(b []byte, field int, p []byte) []byte {
	if len(p) == 0 {
		return b
	}
	b = appendKey(b, field, wtBytes)
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func appendOptErrorCode(b []byte, field int, c *ErrorCode) []byte {
	if c == nil {
		return b
	}
	return appendRawString(b, field, string(*c))
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// binReader walks the fields of one binary body. Every read is bounds-checked: a body comes off
// the network, and a corrupt or hostile one must fail with a decode error, never panic.
type binReader struct {
	b   []byte
	off int
	err error
	// base is b's offset within the whole body, for a reader over a nested field (see spanOf).
	base int
}

func (r *binReader) fail(msg string) {
	if r.err == nil {
		r.err = newDecodeError(msg)
	}
}

func (r *binReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b[r.off:])
	if n <= 0 {
		r.fail("malformed varint in binary body")
		r.off = len(r.b)
		return 0
	}
	r.off += n
	return v
}

// next returns the next field's number and wire type; ok=false at the end of the body or on error.
func (r *binReader) next() (field int, wt int, ok bool) {
	if r.err != nil || r.off >= len(r.b) {
		return 0, 0, false
	}
	key := r.uvarint()
	if r.err != nil {
		return 0, 0, false
	}
	if key>>3 == 0 || key>>3 > 1<<29 {
		r.fail("invalid field number in binary body")
		return 0, 0, false
	}
	return int(key >> 3), int(key & 7), true
}

// bytes reads a length-delimited value as a view into the body.
func (r *binReader) bytes(wt int) []byte {
	if wt != wtBytes {
		r.fail("binary field has the wrong wire type")
		r.skip(wt)
		return nil
	}
	n := r.uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)-r.off) {
		r.fail("binary field runs past the end of the body")
		r.off = len(r.b)
		return nil
	}
	p := r.b[r.off : r.off+int(n) : r.off+int(n)]
	r.off += int(n)
	return p
}

// span reads a length-delimited value and returns its offset within the whole body and its length.
func (r *binReader) span(wt int) (start, n int) {
	p := r.bytes(wt)
	return r.base + r.off - len(p), len(p)
}

// sub reads a length-delimited value and returns it as a nested reader.
func (r *binReader) sub(wt int) binReader {
	start, n := r.span(wt)
	return binReader{b: r.b[start-r.base : start-r.base+n], base: start}
}

// adopt carries a nested reader's error up to r.
func (r *binReader) adopt(sub *binReader) {
	if sub.err != nil && r.err == nil {
		r.err = sub.err
	}
}

func (r *binReader) str(wt int) string { return string(r.bytes(wt)) }

func (r *binReader) optStr(wt int) *string {
	s := r.str(wt)
	return &s
}

func (r *binReader) int(wt int) int64 {
	if wt != wtVarint {
		r.fail("binary field has the wrong wire type")
		r.skip(wt)
		return 0
	}
	return int64(r.uvarint())
}

func (r *binReader) optInt(wt int) *int {
	v := int(r.int(wt))
	return &v
}

func (r *binReader) bool(wt int) bool { return r.int(wt) != 0 }

func (r *binReader) optErrorCode(wt int) *ErrorCode {
	c := ErrorCode(r.str(wt))
	return &c
}

// skip passes over a field this build does not know.
func (r *binReader) skip(wt int) {
	switch wt {
	case wtVarint:
		r.uvarint()
	case wtBytes:
		n := r.uvarint()
		if r.err == nil && n > uint64(len(r.b)-r.off) {
			r.fail("binary field runs past the end of the body")
			r.off = len(r.b)
			return
		}
		r.off += int(n)
	case wtFixed64, wtFixed32:
		size := 8
		if wt == wtFixed32 {
			size = 4
		}
		if len(r.b)-r.off < size {
			r.fail("binary field runs past the end of the body")
			r.off = len(r.b)
			return
		}
		r.off += size
	default:
		r.fail("unknown wire type in binary body")
		r.off = len(r.b)
	}
}

// appendNested writes a length-delimited field whose contents body appends. The length is not
// known until body has run, so four bytes are reserved for it - a uvarint of up to 2^28, past
// any frame - and the contents moved down over whatever the uvarint did not need.
func appendNested(b []byte, field int, body func([]byte) []byte) []byte {
	b = appendKey(b, field, wtBytes)
	start := len(b)
	b = append(b, 0, 0, 0, 0)
	b = body(b)
	n := len(b) - start - 4
	var lenBuf [binary.MaxVarintLen64]byte
	l := binary.PutUvarint(lenBuf[:], uint64(n))
	copy(b[start:], lenBuf[:l])
	copy(b[start+l:], b[start+4:])
	return b[:start+l+n]
}

// appendDouble writes v as a little-endian IEEE 754 fixed64, unless it is zero.
func appendDouble(b []byte, field int, v float64) []byte {
	if v == 0 {
		return b
	}
	return appendRawDouble(b, field, v)
}

func appendRawDouble(b []byte, field int, v float64) []byte {
	b = appendKey(b, field, wtFixed64)
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
}

func appendOptDouble(b []byte, field int, v *float64) []byte {
	if v == nil {
		return b
	}
	return appendRawDouble(b, field, *v)
}

// appendUUID writes id as its 16 big-endian bytes (the order of its canonical string), unless zero.
func appendUUID(b []byte, field int, id codec.UUID) []byte {
	if id == (codec.UUID{}) {
		return b
	}
	b = appendKey(b, field, wtBytes)
	b = append(b, 16)
	b = binary.BigEndian.AppendUint64(b, uint64(id.MSB))
	return binary.BigEndian.AppendUint64(b, uint64(id.LSB))
}

// appendHash writes h's 32 raw bytes, unless it is the zero hash.
func appendHash(b []byte, field int, h codec.Hash) []byte {
	if h == (codec.Hash{}) {
		return b
	}
	return appendBytes(b, field, h.Bytes[:])
}

func (r *binReader) double(wt int) float64 {
	if wt != wtFixed64 {
		r.fail("binary field has the wrong wire type")
		r.skip(wt)
		return 0
	}
	if len(r.b)-r.off < 8 {
		r.fail("binary field runs past the end of the body")
		r.off = len(r.b)
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.b[r.off:]))
	r.off += 8
	return v
}

func (r *binReader) optDouble(wt int) *float64 {
	v := r.double(wt)
	return &v
}

func (r *binReader) uuid(wt int) codec.UUID {
	p := r.bytes(wt)
	if r.err != nil {
		return codec.UUID{}
	}
	if len(p) != 16 {
		r.fail("binary uuid is not 16 bytes")
		return codec.UUID{}
	}
	return codec.UUID{MSB: int64(binary.BigEndian.Uint64(p)), LSB: int64(binary.BigEndian.Uint64(p[8:]))}
}

func (r *binReader) hash(wt int) codec.Hash {
	p := r.bytes(wt)
	if r.err != nil {
		return codec.Hash{}
	}
	if len(p) != 32 {
		r.fail("binary hash is not 32 bytes")
		return codec.Hash{}
	}
	var h codec.Hash
	copy(h.Bytes[:], p)
	return h
}
