package document

import (
	"encoding/binary"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// ScanCommitWrites walks an encoded CommitPayload and calls fn with the patch of every OpWrite of
// docID, in operation order, until fn returns false. It reads one document out of a commit
// without decoding the commit.
//
// FromPayloadBytes builds the whole commit to answer the same question: a boxed codec value per
// field of every operation, a string per patch, and a SHA-256 of the whole payload for the commit
// hash. For a cold read of one version out of a hundred-document commit that was a hundred patches
// copied to keep one. Here every operation is a union branch byte and a length-prefixed record, so
// an operation that is not the wanted document's write is stepped over by its length, and the
// wanted one's patch is handed to fn in place: patch aliases payload and is valid only during the
// call. Nothing is allocated.
//
// The other commit fields are walked, not decoded, but they are walked strictly - every field id,
// tag and length is checked against the CommitPayload schema, required fields must be present,
// and the walk finishes even after fn stops - so a payload this accepts has the layout
// FromPayloadBytes would accept. What it does not check is the inside of operations other than
// writes; the frame CRC is what vouches for those bytes, as it does for a full decode.
// TestScanCommitWritesMatchesFullDecode holds the two to the same answer.
//
// The layout walked is CommitPayload's in WireRegistry, which the Kotlin implementation writes
// identically (commit hashes depend on it):
//
//	leb128(len) fields...                        CommitPayload record
//	field 6 = leb128(id) tag(Array) leb128(n) op*
//	op      = branch(u8) leb128(len) fields...   union of OpWrite (0), OpDelete, OpFileWrite, OpSchemaMigration
//	OpWrite = leb128(1) tag(Fixed) uuid(16)  leb128(2) tag(String) leb128(len) patch
func ScanCommitWrites(payload []byte, docID codec.UUID, fn func(patch []byte) bool) error {
	var s commitScanner
	s.raw = payload
	slab, err := s.lenPrefixed("commit payload")
	if err != nil {
		return err
	}
	if s.pos != len(payload) {
		return NewCommitDecodeError("commit payload: trailing bytes", nil)
	}
	f := commitScanner{raw: slab}
	var seen uint16 // bit i: field i present
	stopped := false
	for f.pos < len(f.raw) {
		id, err := f.uvarint("field id")
		if err != nil {
			return err
		}
		tag, err := f.byte("field tag")
		if err != nil {
			return err
		}
		if id < 16 {
			seen |= 1 << id
		}
		switch id {
		case 1: // parentHashes: array of Hash32
			if err := f.expectTag(tag, schema.PhysicalArray); err != nil {
				return err
			}
			n, err := f.uvarint("parent count")
			if err != nil {
				return err
			}
			if n > uint64(len(f.raw)-f.pos)/32 {
				return NewCommitDecodeError("parentHashes", nil)
			}
			f.pos += int(n) * 32
		case 2, 9: // namespaceId, message
			if err := f.expectTag(tag, schema.PhysicalString); err != nil {
				return err
			}
			if _, err := f.lenPrefixed("string field"); err != nil {
				return err
			}
		case 3, 5: // transactionId, authorNodeId
			if err := f.expectTag(tag, schema.PhysicalFixed); err != nil {
				return err
			}
			if err := f.skip(16, "uuid field"); err != nil {
				return err
			}
		case 4: // timestamp
			if err := f.expectTag(tag, schema.PhysicalInt64); err != nil {
				return err
			}
			if err := f.skip(8, "timestamp"); err != nil {
				return err
			}
		case 6:
			if err := f.expectTag(tag, schema.PhysicalArray); err != nil {
				return err
			}
			if err := f.scanOperations(docID, fn, &stopped); err != nil {
				return err
			}
		case 7: // documentTreeHash
			if err := f.expectTag(tag, schema.PhysicalFixed); err != nil {
				return err
			}
			if err := f.skip(32, "documentTreeHash"); err != nil {
				return err
			}
		case 8: // schemaHash: nullable Hash32
			if err := f.expectTag(tag, schema.PhysicalUnion); err != nil {
				return err
			}
			m, err := f.byte("schemaHash")
			if err != nil {
				return err
			}
			switch m {
			case 0:
			case 1:
				if err := f.skip(32, "schemaHash"); err != nil {
					return err
				}
			default:
				return NewCommitDecodeError("schemaHash", nil)
			}
		case 10: // attachmentTreeHash: nullable Hash32
			if err := f.expectTag(tag, schema.PhysicalUnion); err != nil {
				return err
			}
			m, err := f.byte("attachmentTreeHash")
			if err != nil {
				return err
			}
			switch m {
			case 0:
			case 1:
				if err := f.skip(32, "attachmentTreeHash"); err != nil {
					return err
				}
			default:
				return NewCommitDecodeError("attachmentTreeHash", nil)
			}
		default:
			// An unknown field, skipped as the codec skips one: primitives only.
			if err := f.skipPrimitive(tag); err != nil {
				return err
			}
		}
	}
	// Every field FromPayloadBytes requires (all but the nullable schemaHash).
	const required = 1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5 | 1<<6 | 1<<7 | 1<<9
	if seen&required != required {
		return NewCommitDecodeError("commit payload: missing field", nil)
	}
	return nil
}

// scanOperations walks the operations array, calling fn for docID's writes until it returns
// false (recorded in *stopped). The walk itself always finishes, so a payload is held to the same
// layout however early fn stops.
func (s *commitScanner) scanOperations(docID codec.UUID, fn func([]byte) bool, stopped *bool) error {
	n, err := s.uvarint("operation count")
	if err != nil {
		return err
	}
	if n > uint64(len(s.raw)-s.pos) {
		return NewCommitDecodeError("operations", nil)
	}
	for i := uint64(0); i < n; i++ {
		branch, err := s.byte("operation branch")
		if err != nil {
			return err
		}
		if branch > 5 { // 4, 5: attachment put and delete
			return NewCommitDecodeError("operation: unknown union branch", nil)
		}
		body, err := s.lenPrefixed("operation")
		if err != nil {
			return err
		}
		if branch != 0 || *stopped {
			continue
		}
		id, patch, err := scanOpWrite(body)
		if err != nil {
			return err
		}
		if id == docID && !fn(patch) {
			*stopped = true
		}
	}
	return nil
}

// scanOpWrite reads an OpWrite record's docId and patch, skipping unknown primitive fields as the
// codec would. Both fields are required, as OpFromValue requires them.
func scanOpWrite(body []byte) (id codec.UUID, patch []byte, err error) {
	r := commitScanner{raw: body}
	var hasID, hasPatch bool
	for r.pos < len(r.raw) {
		fid, err := r.uvarint("OpWrite field id")
		if err != nil {
			return id, nil, err
		}
		tag, err := r.byte("OpWrite field tag")
		if err != nil {
			return id, nil, err
		}
		switch fid {
		case 1:
			if err := r.expectTag(tag, schema.PhysicalFixed); err != nil {
				return id, nil, err
			}
			if err := r.skip(16, "OpWrite docId"); err != nil {
				return id, nil, err
			}
			b := r.raw[r.pos-16 : r.pos]
			id = codec.UUID{MSB: int64(binary.BigEndian.Uint64(b)), LSB: int64(binary.BigEndian.Uint64(b[8:]))}
			hasID = true
		case 2:
			if err := r.expectTag(tag, schema.PhysicalString); err != nil {
				return id, nil, err
			}
			if patch, err = r.lenPrefixed("OpWrite patch"); err != nil {
				return id, nil, err
			}
			hasPatch = true
		default:
			if err := r.skipPrimitive(tag); err != nil {
				return id, nil, err
			}
		}
	}
	if !hasID || !hasPatch {
		return id, nil, NewCommitDecodeError("OpWrite: missing field", nil)
	}
	return id, patch, nil
}

// commitScanner is a bounds-checked cursor over encoded bytes that never copies them.
type commitScanner struct {
	raw []byte
	pos int
}

func (s *commitScanner) uvarint(what string) (uint64, error) {
	// The codec's LEB128 is binary.Uvarint's encoding; reject the same overlong forms it does.
	v, n := binary.Uvarint(s.raw[s.pos:])
	if n <= 0 {
		return 0, NewCommitDecodeError(what+": bad varint", nil)
	}
	s.pos += n
	return v, nil
}

func (s *commitScanner) byte(what string) (byte, error) {
	if s.pos >= len(s.raw) {
		return 0, NewCommitDecodeError(what+": truncated", nil)
	}
	b := s.raw[s.pos]
	s.pos++
	return b, nil
}

func (s *commitScanner) skip(n int, what string) error {
	if n < 0 || n > len(s.raw)-s.pos {
		return NewCommitDecodeError(what+": truncated", nil)
	}
	s.pos += n
	return nil
}

func (s *commitScanner) lenPrefixed(what string) ([]byte, error) {
	n, err := s.uvarint(what)
	if err != nil {
		return nil, err
	}
	if n > uint64(len(s.raw)-s.pos) {
		return nil, NewCommitDecodeError(what+": truncated", nil)
	}
	start := s.pos
	s.pos += int(n)
	return s.raw[start:s.pos:s.pos], nil
}

func (s *commitScanner) expectTag(tag byte, want schema.PhysicalKind) error {
	if tag != want.Tag() {
		return NewCommitDecodeError("commit payload: record tag mismatch", nil)
	}
	return nil
}

// skipPrimitive skips an unknown field's value the way the codec's skipTagged does.
func (s *commitScanner) skipPrimitive(tag byte) error {
	kind, ok := schema.PhysicalFromTag(tag)
	if !ok {
		return NewCommitDecodeError("commit payload: unknown record tag", nil)
	}
	switch kind {
	case schema.PhysicalInt32, schema.PhysicalFloat32:
		return s.skip(4, "field")
	case schema.PhysicalInt64, schema.PhysicalFloat64:
		return s.skip(8, "field")
	case schema.PhysicalString, schema.PhysicalBytes:
		_, err := s.lenPrefixed("field")
		return err
	case schema.PhysicalBool, schema.PhysicalInt8:
		return s.skip(1, "field")
	case schema.PhysicalInt16:
		return s.skip(2, "field")
	default:
		return NewCommitDecodeError("commit payload: cannot skip unknown field", nil)
	}
}
