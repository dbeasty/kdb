package sstable

import (
	"encoding/binary"
	"math/bits"

	"github.com/limidus/kdb/go/kdb/codec"
)

// A blocked Bloom filter over a table's keys, stored in the table's sidecar (sidecar.go).
//
// Blocked: every key's bits fall in one 64-byte block, chosen by one hash, so a probe touches one
// cache line - and, since the filter is read through a memory mapping, at most one page - however
// many bits it checks. A classic filter spreads its k bits over the whole array, which for a
// filter on disk is k page touches per miss. The price is a slightly higher false-positive rate
// at the same size (about 1% instead of 0.8% at 10 bits per key), which is the trade RocksDB's
// and Cassandra's newer filters make too.
const (
	bloomBlockBytes = 64
	bloomBlockBits  = bloomBlockBytes * 8
	// bloomBitsPerKey sizes the filter: ~1% false positives with bloomProbes bits per key.
	bloomBitsPerKey = 10
	// bloomProbes is how many bits a key sets in its block. 7 is optimal for 10 bits per key,
	// and 7 nine-bit positions fit in one 64-bit hash.
	bloomProbes = 7
)

// bloomBlocksFor is how many blocks a filter over n keys gets. Zero for an empty table: a filter
// with no blocks rejects every key, which is the right answer for a table with none.
func bloomBlocksFor(n int) int {
	if n <= 0 {
		return 0
	}
	return (n*bloomBitsPerKey + bloomBlockBits - 1) / bloomBlockBits
}

// bloomHashes derives the filter's two hashes from a key. Keys here are SHA-256 digests and
// already uniform, but the store does not enforce that every key is one, so all four words are
// mixed in rather than any used raw: a filter fed structured keys must still spread them.
func bloomHashes(key *codec.Hash) (block, probes uint64) {
	k := key.Bytes[:]
	h := mix64(binary.LittleEndian.Uint64(k[24:]))
	h = mix64(h + binary.LittleEndian.Uint64(k[16:]))
	h = mix64(h + binary.LittleEndian.Uint64(k[8:]))
	h = mix64(h + binary.LittleEndian.Uint64(k[0:]))
	return h, mix64(h + 0x9e3779b97f4a7c15)
}

// mix64 is the splitmix64 finalizer.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// bloomBlock returns the block of filter (nblocks blocks) that key's bits live in.
func bloomBlock(filter []byte, nblocks uint64, h uint64) []byte {
	i, _ := bits.Mul64(h, nblocks) // fast range reduction: uniform h -> [0, nblocks)
	off := i * bloomBlockBytes
	return filter[off : off+bloomBlockBytes : off+bloomBlockBytes]
}

// bloomAdd sets key's bits in filter.
func bloomAdd(filter []byte, nblocks uint64, key *codec.Hash) {
	if nblocks == 0 {
		return
	}
	h1, h2 := bloomHashes(key)
	blk := bloomBlock(filter, nblocks, h1)
	for i := 0; i < bloomProbes; i++ {
		bit := (h2 >> (9 * i)) & (bloomBlockBits - 1)
		blk[bit>>3] |= 1 << (bit & 7)
	}
}

// bloomMayContain reports whether key may be in the set filter was built from. false is
// definite; true is wrong about 1% of the time for a key that is not.
func bloomMayContain(filter []byte, nblocks uint64, key *codec.Hash) bool {
	if nblocks == 0 {
		return false
	}
	h1, h2 := bloomHashes(key)
	blk := bloomBlock(filter, nblocks, h1)
	for i := 0; i < bloomProbes; i++ {
		bit := (h2 >> (9 * i)) & (bloomBlockBits - 1)
		if blk[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}
