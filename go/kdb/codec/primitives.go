package codec

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"
)

// UUID is a 128-bit RFC 4122 identifier.
type UUID struct{ MSB, LSB int64 }

func RandomUUID() (UUID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return UUID{}, err
	}
	b[6] = (b[6]&0x0f | 0x40)
	b[8] = (b[8]&0x3f | 0x80)
	return UUIDFromBytes(b[:])
}

func UUIDFromBytes(b []byte) (UUID, error) {
	if len(b) != 16 {
		return UUID{}, fmt.Errorf("uuid requires 16 bytes")
	}
	return UUID{
		MSB: int64(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
			uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])),
		LSB: int64(uint64(b[8])<<56 | uint64(b[9])<<48 | uint64(b[10])<<40 | uint64(b[11])<<32 |
			uint64(b[12])<<24 | uint64(b[13])<<16 | uint64(b[14])<<8 | uint64(b[15])),
	}, nil
}

func (u UUID) String() string {
	b := u.Bytes()
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
		uint16(b[8])<<8|uint16(b[9]),
		b[10:16])
}

func (u UUID) Bytes() []byte {
	out := make([]byte, 16)
	writeBE64(u.MSB, out, 0)
	writeBE64(u.LSB, out, 8)
	return out
}

// UUIDFromString parses a canonical lowercase/uppercase UUID string.
func UUIDFromString(s string) (UUID, error) {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return UUID{}, fmt.Errorf("uuid string requires 32 hex chars")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return UUID{}, err
	}
	return UUIDFromBytes(b)
}

func writeBE64(v int64, out []byte, off int) {
	u := uint64(v)
	for i := 0; i < 8; i++ {
		out[off+i] = byte(u >> ((7 - i) * 8))
	}
}

// Hash is a SHA-256 digest (32 bytes).
type Hash struct{ Bytes [32]byte }

func HashFromBytes(b []byte) (Hash, error) {
	if len(b) != 32 {
		return Hash{}, fmt.Errorf("hash requires 32 bytes")
	}
	var h Hash
	copy(h.Bytes[:], b)
	return h, nil
}

func HashFromHex(s string) (Hash, error) {
	if len(s) != 64 {
		return Hash{}, fmt.Errorf("hex hash requires 64 chars")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return Hash{}, err
	}
	return HashFromBytes(b)
}

func (h Hash) Hex() string { return hex.EncodeToString(h.Bytes[:]) }

// Timestamp is a logical instant with microsecond resolution within a millisecond.
type Timestamp struct {
	EpochMillis    int64
	MicroRemainder int
}

func (t Timestamp) EpochMicros() int64 { return t.EpochMillis*1000 + int64(t.MicroRemainder) }

func TimestampNow() Timestamp {
	n := time.Now().UnixNano()
	micros := n / 1000
	ms := micros / 1000
	r := int(micros % 1000)
	return Timestamp{EpochMillis: ms, MicroRemainder: r}
}

func TimestampFromEpochMicros(micros int64) Timestamp {
	ms := micros / 1000
	r := int(micros % 1000)
	if micros < 0 && micros%1000 != 0 {
		ms--
		r = int(1000 + micros%1000)
	}
	return Timestamp{EpochMillis: ms, MicroRemainder: r}
}

func readBE64(b []byte, off int) int64 {
	var u uint64
	for i := 0; i < 8; i++ {
		u = (u << 8) | uint64(b[off+i])
	}
	return int64(u)
}

// helpers for wire
func putFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	out := make([]byte, 8)
	putLe64(bits, out)
	return out
}

func readFloat64(b []byte) float64 {
	return math.Float64frombits(readLe64(b, 0))
}

func putFloat32(v float32) []byte {
	bits := math.Float32bits(v)
	out := make([]byte, 4)
	putLe32(uint32(bits), out)
	return out
}

func readFloat32(b []byte) float32 {
	return math.Float32frombits(uint32(readLe32(b, 0)))
}

func putLe32(v uint32, out []byte) {
	for i := 0; i < 4; i++ {
		out[i] = byte(v >> (8 * i))
	}
}

func putLe64(v uint64, out []byte) {
	for i := 0; i < 8; i++ {
		out[i] = byte(v >> (8 * i))
	}
}

func readLe32(b []byte, off int) int32 {
	var v uint32
	for i := 0; i < 4; i++ {
		v |= uint32(b[off+i]) << (8 * i)
	}
	return int32(v)
}

func readLe64(b []byte, off int) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v |= uint64(b[off+i]) << (8 * i)
	}
	return v
}
