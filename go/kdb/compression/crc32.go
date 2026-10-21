package compression

import "hash/crc32"

// CRC32 computes IEEE CRC-32 (same polynomial as Kotlin dev.kdb.compression.Crc32).
//
// hash/crc32 computes exactly this checksum with the CPU's CRC instructions on arm64 and the
// slicing/CLMUL paths on amd64. The byte-at-a-time table loop it replaces was 16% of
// kdb-service's CPU under qtask's mixed workload, every cold read CRC-ing a whole frame.
func CRC32(data []byte, offset, length int) uint32 {
	if length < 0 {
		length = len(data) - offset
	}
	end := offset + length
	if end > len(data) {
		end = len(data)
	}
	if offset >= end {
		return 0
	}
	return crc32.ChecksumIEEE(data[offset:end])
}

// CRC32All computes CRC-32 over the entire slice.
func CRC32All(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
