package document

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// ContentHasher computes the same hash as Document.ContentHash, reusing one scratch buffer
// instead of allocating per call.
//
// ContentHash goes through the generic codec: it builds a RecordValue (a map of boxed fields),
// resolves the DocumentBody schema and encodes into a fresh buffer - five or six allocations and
// a full copy of the body for every hash. That is fine on a write, and was most of the cost of
// building the cold-frame index, which hashes every version in the log in one pass.
//
// The bytes hashed here are DocumentBody's wire encoding written out by hand:
//
//	leb128(len(body))
//	body = leb128(1) tag(Fixed)  uuid(16, big-endian MSB then LSB)
//	       leb128(2) tag(String) leb128(len(json)) json
//
// TestContentHasherMatchesContentHash holds the two to identical output, so a change to the
// DocumentBody schema that is not mirrored here fails loudly rather than silently changing the
// hash a cold read verifies against.
//
// Not safe for concurrent use; give each goroutine its own.
type ContentHasher struct {
	buf []byte
}

// Hash returns the content hash of the document with this id and JSON text.
func (h *ContentHasher) Hash(id codec.UUID, json string) codec.Hash {
	bodyLen := 1 + 1 + 16 + 1 + 1 + lebLen(uint32(len(json))) + len(json)
	b := h.buf[:0]
	b = binary.AppendUvarint(b, uint64(uint32(bodyLen)))
	b = append(b, 1, schema.PhysicalFixed.Tag())
	b = binary.BigEndian.AppendUint64(b, uint64(id.MSB))
	b = binary.BigEndian.AppendUint64(b, uint64(id.LSB))
	b = append(b, 2, schema.PhysicalString.Tag())
	b = binary.AppendUvarint(b, uint64(uint32(len(json))))
	b = append(b, json...)
	h.buf = b
	return codec.Hash{Bytes: sha256.Sum256(b)}
}

// lebLen is how many bytes the codec's unsigned LEB128 takes for v - the same encoding as
// binary.AppendUvarint for a uint32.
func lebLen(v uint32) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
