package index

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Order-preserving binary key encoding.
//
// The keyed index stores every distinct key once, as bytes in a pointer-free arena (see
// keyTable), and keeps a B-tree's keys in sorted order by comparing those bytes directly. For that
// the encoding has to be:
//
//   - injective, so the bytes can stand in for the key when interning (two keys are the same key
//     exactly when their KeyStrings are equal, and exactly when their encodings are);
//   - memcomparable: bytes.Compare(enc(a), enc(b)) has the sign of CompareKeys(a, b) whenever
//     CompareKeys tells the two apart. The only keys CompareKeys calls equal that the encoding
//     does not are -0 and +0 (the encoding puts -0 first) and a NaN against anything; the old
//     range scan sorted those ties in an unspecified order, so any fixed order is one it could
//     have produced;
//   - self-delimiting, so a composite or vector key compares part by part.
//
// Layout: one tag byte (keyTag's order, so different types sort as CompareKeys sorts them), then
//
//	Null       nothing
//	Bool       0x00 / 0x01
//	Int32      4 bytes big-endian, sign bit flipped
//	Int64      8 bytes big-endian, sign bit flipped (Timestamp the same)
//	Float64    8 bytes: the IEEE bits with the sign bit flipped for positives and every bit
//	           flipped for negatives; every NaN is encoded as the canonical one
//	String     the bytes with 0x00 escaped as 0x00 0xFF, then the terminator 0x00 0x01
//	UUID       16 bytes: MSB then LSB, big-endian (the order of the canonical string form)
//	Vector     per element 0x01 + 4-byte float32 in the Float64 manner, then 0x00
//	Composite  per part 0x01 + the part's encoding, then 0x00
const (
	encTagNull      = 0
	encTagBool      = 1
	encTagInt32     = 2
	encTagInt64     = 3
	encTagFloat64   = 4
	encTagTimestamp = 5
	encTagString    = 6
	encTagUUID      = 7
	encTagVector    = 8
	encTagComposite = 9
)

var errKeyEncoding = errors.New("index: corrupt key encoding")

// appendKeyEnc appends k's order-preserving encoding to dst.
func appendKeyEnc(dst []byte, k Key) []byte {
	switch v := k.(type) {
	case BoolKey:
		dst = append(dst, encTagBool)
		if v.Value {
			return append(dst, 1)
		}
		return append(dst, 0)
	case Int32Key:
		dst = append(dst, encTagInt32)
		return binary.BigEndian.AppendUint32(dst, uint32(v.Value)^(1<<31))
	case Int64Key:
		dst = append(dst, encTagInt64)
		return binary.BigEndian.AppendUint64(dst, uint64(v.Value)^(1<<63))
	case Float64Key:
		dst = append(dst, encTagFloat64)
		return binary.BigEndian.AppendUint64(dst, orderFloat64(v.Value))
	case TimestampKey:
		dst = append(dst, encTagTimestamp)
		return binary.BigEndian.AppendUint64(dst, uint64(v.EpochMillis)^(1<<63))
	case StringKey:
		dst = append(dst, encTagString)
		for i := 0; i < len(v.Value); i++ {
			c := v.Value[i]
			dst = append(dst, c)
			if c == 0 {
				dst = append(dst, 0xFF)
			}
		}
		return append(dst, 0x00, 0x01)
	case UUIDKey:
		dst = append(dst, encTagUUID)
		dst = binary.BigEndian.AppendUint64(dst, uint64(v.ID.MSB))
		return binary.BigEndian.AppendUint64(dst, uint64(v.ID.LSB))
	case VectorKey:
		dst = append(dst, encTagVector)
		for _, f := range v.embedding {
			dst = append(dst, 0x01)
			dst = binary.BigEndian.AppendUint32(dst, orderFloat32(f))
		}
		return append(dst, 0x00)
	case CompositeKey:
		dst = append(dst, encTagComposite)
		for _, p := range v.Parts {
			dst = append(dst, 0x01)
			dst = appendKeyEnc(dst, p)
		}
		return append(dst, 0x00)
	default:
		// NullKey, and anything outside the sealed union, which KeyString also files as NULL.
		return append(dst, encTagNull)
	}
}

func orderFloat64(f float64) uint64 {
	if f != f {
		f = math.NaN()
	}
	b := math.Float64bits(f)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

func unorderFloat64(u uint64) float64 {
	if u&(1<<63) != 0 {
		return math.Float64frombits(u &^ (1 << 63))
	}
	return math.Float64frombits(^u)
}

func orderFloat32(f float32) uint32 {
	if f != f {
		f = float32(math.NaN())
	}
	b := math.Float32bits(f)
	if b&(1<<31) != 0 {
		return ^b
	}
	return b | 1<<31
}

func unorderFloat32(u uint32) float32 {
	if u&(1<<31) != 0 {
		return math.Float32frombits(u &^ (1 << 31))
	}
	return math.Float32frombits(^u)
}

// decodeKeyEnc decodes one key from the front of b and returns it with the rest of b.
func decodeKeyEnc(b []byte) (Key, []byte, error) {
	if len(b) == 0 {
		return nil, nil, errKeyEncoding
	}
	tag, b := b[0], b[1:]
	need := func(n int) error {
		if len(b) < n {
			return errKeyEncoding
		}
		return nil
	}
	switch tag {
	case encTagNull:
		return NullKey{}, b, nil
	case encTagBool:
		if err := need(1); err != nil {
			return nil, nil, err
		}
		return BoolKey{Value: b[0] == 1}, b[1:], nil
	case encTagInt32:
		if err := need(4); err != nil {
			return nil, nil, err
		}
		return Int32Key{Value: int32(binary.BigEndian.Uint32(b) ^ (1 << 31))}, b[4:], nil
	case encTagInt64:
		if err := need(8); err != nil {
			return nil, nil, err
		}
		return Int64Key{Value: int64(binary.BigEndian.Uint64(b) ^ (1 << 63))}, b[8:], nil
	case encTagFloat64:
		if err := need(8); err != nil {
			return nil, nil, err
		}
		return Float64Key{Value: unorderFloat64(binary.BigEndian.Uint64(b))}, b[8:], nil
	case encTagTimestamp:
		if err := need(8); err != nil {
			return nil, nil, err
		}
		return TimestampKey{EpochMillis: int64(binary.BigEndian.Uint64(b) ^ (1 << 63))}, b[8:], nil
	case encTagString:
		out := make([]byte, 0, len(b))
		for i := 0; i < len(b); i++ {
			if b[i] != 0 {
				out = append(out, b[i])
				continue
			}
			if i+1 >= len(b) {
				return nil, nil, errKeyEncoding
			}
			switch b[i+1] {
			case 0xFF:
				out = append(out, 0)
				i++
			case 0x01:
				return StringKey{Value: string(out)}, b[i+2:], nil
			default:
				return nil, nil, errKeyEncoding
			}
		}
		return nil, nil, errKeyEncoding
	case encTagUUID:
		if err := need(16); err != nil {
			return nil, nil, err
		}
		id := codec.UUID{MSB: int64(binary.BigEndian.Uint64(b)), LSB: int64(binary.BigEndian.Uint64(b[8:]))}
		return UUIDKey{ID: id}, b[16:], nil
	case encTagVector:
		var vec []float32
		for {
			if err := need(1); err != nil {
				return nil, nil, err
			}
			if b[0] == 0x00 {
				if vec == nil {
					vec = []float32{}
				}
				return VectorKey{embedding: vec}, b[1:], nil
			}
			if b[0] != 0x01 || len(b) < 5 {
				return nil, nil, errKeyEncoding
			}
			vec = append(vec, unorderFloat32(binary.BigEndian.Uint32(b[1:])))
			b = b[5:]
		}
	case encTagComposite:
		var parts []Key
		for {
			if err := need(1); err != nil {
				return nil, nil, err
			}
			if b[0] == 0x00 {
				if parts == nil {
					parts = []Key{}
				}
				return CompositeKey{Parts: parts}, b[1:], nil
			}
			if b[0] != 0x01 {
				return nil, nil, errKeyEncoding
			}
			p, rest, err := decodeKeyEnc(b[1:])
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, p)
			b = rest
		}
	default:
		return nil, nil, errKeyEncoding
	}
}
