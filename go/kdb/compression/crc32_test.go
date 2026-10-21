package compression

import (
	"math/rand"
	"testing"
)

// tableCRC32 is the byte-at-a-time implementation CRC32 used before hash/crc32: the reference
// every checksum already on disk was written with.
func tableCRC32(data []byte, offset, length int) uint32 {
	var table [256]uint32
	for i := range table {
		c := uint32(i)
		for j := 0; j < 8; j++ {
			if c&1 != 0 {
				c = 0xEDB88320 ^ (c >> 1)
			} else {
				c >>= 1
			}
		}
		table[i] = c
	}
	if length < 0 {
		length = len(data) - offset
	}
	end := offset + length
	if end > len(data) {
		end = len(data)
	}
	crc := uint32(0xFFFFFFFF)
	for i := offset; i < end; i++ {
		crc = table[(crc^uint32(data[i]))&0xFF] ^ (crc >> 8)
	}
	return ^crc
}

func TestCRC32MatchesTheTableImplementation(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 7, 8, 15, 16, 63, 64, 255, 4096, 65537} {
		data := make([]byte, n)
		rng.Read(data)
		if got, want := CRC32All(data), tableCRC32(data, 0, -1); got != want {
			t.Fatalf("len %d: CRC32All %08x, table %08x", n, got, want)
		}
		for i := 0; i < 20 && n > 0; i++ {
			off, length := rng.Intn(n), rng.Intn(n+8)-4 // negative and past-the-end lengths too
			if got, want := CRC32(data, off, length), tableCRC32(data, off, length); got != want {
				t.Fatalf("len %d off %d length %d: CRC32 %08x, table %08x", n, off, length, got, want)
			}
		}
	}
	// An offset at or past the end is an empty range, which both give as 0.
	data := []byte("abc")
	for _, off := range []int{3, 4} {
		if got, want := CRC32(data, off, 2), tableCRC32(data, off, 2); got != want || got != 0 {
			t.Fatalf("off %d: CRC32 %08x, table %08x", off, got, want)
		}
	}
}

func BenchmarkCRC32Frame(b *testing.B) {
	data := make([]byte, 64<<10)
	rand.New(rand.NewSource(1)).Read(data)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		CRC32All(data)
	}
}

func BenchmarkCRC32FrameTable(b *testing.B) {
	data := make([]byte, 64<<10)
	rand.New(rand.NewSource(1)).Read(data)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		tableCRC32(data, 0, -1)
	}
}
