package codec

import "crypto/sha256"

// DocIDNamespace is KDB_DOC_ID_NAMESPACE from kdb-spec-layer16 §9.4: the fixed UUID whose 16 raw
// bytes prefix every derived document id's hash input. Shared verbatim with the Kotlin tree so
// both produce identical ids for identical strings (pinned by
// go/testdata/golden/search/derived_id_vectors.json).
var DocIDNamespace = mustUUIDFromBytes([]byte{
	0x6f, 0x5b, 0x9a, 0x1c, 0x2d, 0x3e, 0x4f, 0x70, 0x8a, 0x9b, 0x1c, 0x2d, 0x3e, 0x4f, 0x5a, 0x6b,
}) // 6f5b9a1c-2d3e-4f70-8a9b-1c2d3e4f5a6b

func mustUUIDFromBytes(b []byte) UUID {
	id, err := UUIDFromBytes(b)
	if err != nil {
		panic(err)
	}
	return id
}

// DerivedUUID maps an arbitrary non-UUID document id string to a stable UUID:
// uuid8(sha256(DocIDNamespace ‖ utf8(s))) - the first 16 bytes of the digest with the version
// nibble forced to 8 and the RFC 4122 variant bits to 10. Version 8 is the "custom/vendor"
// version, so a derived id can never be mistaken for a random v4 one minted by RandomUUID.
//
// This is what lets a document carry a natural-key `id` ("order-1234", a Mongo ObjectId hex
// string, ...) and still be addressed by UUID everywhere else in the engine, without the engine
// rewriting the body to say so (§9.4: nothing is injected, nothing is reordered).
func DerivedUUID(s string) UUID {
	h := sha256.New()
	h.Write(DocIDNamespace.Bytes())
	h.Write([]byte(s))
	sum := h.Sum(nil)
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x80
	b[8] = (b[8] & 0x3f) | 0x80
	id, _ := UUIDFromBytes(b[:]) // 16 bytes by construction; UUIDFromBytes cannot fail
	return id
}
