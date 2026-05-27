package codec

import (
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

func encodeLeb128U32(v uint32) []byte {
	var out []byte
	cur := v
	for {
		chunk := byte(cur & 0x7F)
		cur >>= 7
		if cur == 0 {
			out = append(out, chunk)
			break
		}
		out = append(out, chunk|0x80)
	}
	return out
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

func lebPrefix(raw []byte) []byte {
	p := encodeLeb128U32(uint32(len(raw)))
	return append(p, raw...)
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
