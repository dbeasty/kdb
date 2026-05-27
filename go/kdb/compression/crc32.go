package compression

// CRC32 computes IEEE CRC-32 (same polynomial as Kotlin dev.kdb.compression.Crc32).
func CRC32(data []byte, offset, length int) uint32 {
	if length < 0 {
		length = len(data) - offset
	}
	end := offset + length
	if end > len(data) {
		end = len(data)
	}
	var crc uint32 = 0xFFFFFFFF
	for i := offset; i < end; i++ {
		crc = crc32Table[(crc^uint32(data[i]))&0xFF] ^ (crc >> 8)
	}
	return ^crc
}

// CRC32All computes CRC-32 over the entire slice.
func CRC32All(data []byte) uint32 {
	return CRC32(data, 0, len(data))
}

var crc32Table [256]uint32

func init() {
	for i := 0; i < 256; i++ {
		c := uint32(i)
		for j := 0; j < 8; j++ {
			if c&1 != 0 {
				c = 0xEDB88320 ^ (c >> 1)
			} else {
				c >>= 1
			}
		}
		crc32Table[i] = c
	}
}
