package codec

import (
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// appendLeb128U32 writes v's varint encoding straight onto dst. Every record
// field emits at least one varint (its field id), plus one per array/map length
// and one per string, so returning a freshly-allocated slice per varint - which
// encodeLeb128U32 does - put an allocation on each of them.
func appendLeb128U32(dst []byte, v uint32) []byte {
	cur := v
	for {
		chunk := byte(cur & 0x7F)
		cur >>= 7
		if cur == 0 {
			return append(dst, chunk)
		}
		dst = append(dst, chunk|0x80)
	}
}

// encodeLeb128U32 is appendLeb128U32 for the few callers that genuinely want a
// standalone slice. Prefer appendLeb128U32 on any encode path.
func encodeLeb128U32(v uint32) []byte {
	return appendLeb128U32(nil, v)
}

func readLeb128U64(raw []byte, pos *int) (uint64, error) {
	var result uint64
	var shift uint
	for i := 0; i < 11; i++ {
		if *pos >= len(raw) {
			return 0, kdberr.NewDecodeError("truncated varint", *pos, nil)
		}
		b := raw[*pos]
		*pos++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift > 63 {
			return 0, kdberr.NewDecodeError("varint overflow", *pos, nil)
		}
	}
	return 0, kdberr.NewDecodeError("varint overflow", *pos, nil)
}

type cursor struct {
	raw   []byte
	pos   int
	limit int
}

func (c *cursor) u8() (byte, error) {
	if c.pos >= c.limit {
		return 0, kdberr.NewDecodeError("eof u8", c.pos, nil)
	}
	b := c.raw[c.pos]
	c.pos++
	return b, nil
}

func (c *cursor) rawN(n int) ([]byte, error) {
	if n < 0 || c.pos+n > c.limit {
		return nil, kdberr.NewDecodeError("eof raw", c.pos, nil)
	}
	s := make([]byte, n)
	copy(s, c.raw[c.pos:c.pos+n])
	c.pos += n
	return s, nil
}

func (c *cursor) leb() (uint64, error) {
	return readLeb128U64(c.raw, &c.pos)
}

// checkElementCount rejects an array/map length that the remaining input cannot possibly back,
// before it is used to size an allocation. The count is a varint straight off the wire (or off
// disk), so nine bytes used to be enough to ask for a slice of 2^60 elements: that is a
// makeslice panic on a 64-bit build - and with no recover() on any frame-handling path, a panic
// is a dead process, reachable by any peer that can get a commit payload decoded.
//
// Every element costs at least one byte, so a count above the bytes remaining is malformed by
// construction. That holds for every type in this codebase's schemas (the schema comes from the
// caller, never from the wire, and none of them has a zero-width element); it also turns what
// would otherwise be a 2^60-iteration spin into an immediate error.
func (c *cursor) checkElementCount(n uint64, kind string) error {
	if remaining := c.limit - c.pos; remaining < 0 || n > uint64(remaining) {
		return kdberr.NewDecodeError(kind+" length exceeds remaining input", c.pos, nil)
	}
	return nil
}

func putLe16(v int16) []byte {
	i := uint16(v)
	return []byte{byte(i), byte(i >> 8)}
}

func putLe32s(v int32) []byte {
	out := make([]byte, 4)
	putLe32(uint32(v), out)
	return out
}

func putLe64s(v int64) []byte {
	out := make([]byte, 8)
	putLe64(uint64(v), out)
	return out
}

func readLe16(b []byte) int16 {
	return int16(uint16(b[0]) | uint16(b[1])<<8)
}

// appendLebBytes writes raw length-prefixed straight onto dst - one append
// instead of lebPrefix's two allocations (the varint, then the joined result).
func appendLebBytes(dst []byte, raw []byte) []byte {
	dst = appendLeb128U32(dst, uint32(len(raw)))
	return append(dst, raw...)
}

// appendLebString is appendLebBytes for a string, avoiding the []byte(s)
// conversion's full copy - which, for a document body carried as a string, is a
// copy of the entire document on every encode.
func appendLebString(dst []byte, s string) []byte {
	dst = appendLeb128U32(dst, uint32(len(s)))
	return append(dst, s...)
}

func lebPrefix(raw []byte) []byte {
	return appendLebBytes(nil, raw)
}

func readLebBytes(c *cursor) ([]byte, error) {
	n, err := c.leb()
	if err != nil {
		return nil, err
	}
	return c.rawN(int(n))
}

func readLebString(c *cursor) (string, error) {
	raw, err := readLebBytes(c)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
