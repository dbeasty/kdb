package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// graphBloom is a split-block bloom filter over one segment's commit hashes and transaction ids -
// Cassandra's per-SSTable bloom filter (bloom_filter_fp_chance), and for the same reason: almost
// every lookup a write makes is for something no segment holds (its own new commit hash, its new
// transaction id), and without a filter each of those is a search of every segment. With one, a
// miss costs one cache line per segment and never touches the mapping.
//
// Split-block (as Parquet and Impala build them): a key picks one 256-bit block and sets one bit
// in each of its eight 32-bit words, so a probe reads one cache line. ~10 bits a key gives about
// 1% false positives. The blocks are a pointer-free slice the collector does not scan.
//
// Built when a segment is mapped, from the segment's own bytes, so it is never stale and never on
// disk: the segment format does not change.
type graphBloom struct {
	blocks [][8]uint32
}

// graphBloomBitsPerKey sizes the filter: ~1% false positives.
const graphBloomBitsPerKey = 10

var bloomSalt = [8]uint32{0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31}

func newGraphBloom(keys int) graphBloom {
	n := (keys*graphBloomBitsPerKey + 255) / 256
	if n < 1 {
		n = 1
	}
	return graphBloom{blocks: make([][8]uint32, n)}
}

func (f *graphBloom) block(k uint64) *[8]uint32 {
	return &f.blocks[((k>>32)*uint64(len(f.blocks)))>>32]
}

func (f *graphBloom) add(k uint64) {
	b, x := f.block(k), uint32(k)
	for i := range b {
		b[i] |= 1 << ((x * bloomSalt[i]) >> 27)
	}
}

func (f *graphBloom) has(k uint64) bool {
	if len(f.blocks) == 0 {
		return true // no filter: every probe must search
	}
	b, x := f.block(k), uint32(k)
	for i := range b {
		if b[i]&(1<<((x*bloomSalt[i])>>27)) == 0 {
			return false
		}
	}
	return true
}

func (f *graphBloom) bytes() int64 { return int64(len(f.blocks)) * 32 }

// bloomKeyHash is a commit hash's filter key. The hash is SHA-256, so any 8 of its bytes are
// already uniform; these are not the ones the fanout and interpolation search use.
func bloomKeyHash(h *codec.Hash) uint64 { return le.Uint64(h.Bytes[16:24]) }

// bloomKeyHashBytes is bloomKeyHash for a hash still in a segment's OIDS section.
func bloomKeyHashBytes(b []byte) uint64 { return le.Uint64(b[16:24]) }

// bloomKeyTx is a transaction id's filter key. Ids are usually random, but nothing requires it,
// so they are mixed; the constant keeps them apart from commit-hash keys.
func bloomKeyTx(tx codec.UUID) uint64 {
	return splitmix64(uint64(tx.MSB) ^ splitmix64(uint64(tx.LSB)^0x6a09e667f3bcc909))
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
