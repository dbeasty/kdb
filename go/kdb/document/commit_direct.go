package document

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// A direct CommitPayload codec: the layout ScanCommitWrites walks, written and read straight from
// a Commit's fields.
//
// The generic codec builds a codec.Value tree first - a RecordValue map per commit and per
// operation, a boxed value per field, a fresh 32-byte slice per hash - then encodes each record
// body into its own buffer before copying it into its parent's. That tree is thrown away at once,
// yet it was the largest per-commit cost of a replayed catch-up: every received commit was
// decoded, re-encoded to log it, re-encoded again to hash-check it, and the host encoded and
// decoded each one besides. Here an encode is one allocation of the exact size (none, to hash),
// and a decode allocates only what the Commit keeps.
//
// The generic codec stays the definition. The encoder produces its bytes exactly (a commit always
// encodes canonically: fields in id order, schemaHash omitted when nil), and the decoder accepts
// only that canonical layout - anything else (fields out of order or repeated, unknown fields, a
// null schemaHash written out, an operation type it does not know) falls back to the generic
// path, which then decides the result and the error. TestDirectCommitCodecMatchesGeneric holds
// the two to the same bytes and the same commits.

const (
	tagArray  = byte(schema.PhysicalArray)
	tagString = byte(schema.PhysicalString)
	tagFixed  = byte(schema.PhysicalFixed)
	tagInt64  = byte(schema.PhysicalInt64)
	tagUnion  = byte(schema.PhysicalUnion)
)

// lebLenOf is lebLen of a length.
func lebLenOf(v int) int { return lebLen(uint32(v)) }

// opBodyLen is the length of op's record body (its fields, without the record's length prefix),
// or -1 for an op this codec does not write directly.
func opBodyLen(op Op) int {
	switch o := op.(type) {
	case WriteOp:
		return 2 + 16 + 2 + lebLenOf(len(o.Patch)) + len(o.Patch)
	case DeleteOp:
		return 2 + 16
	case FileWriteOp:
		return 2 + lebLenOf(len(o.Path)) + len(o.Path) + 2 + 32
	case SchemaMigrationOp:
		return 2 + 16 + 2 + lebLenOf(len(o.MigrationPayload)) + len(o.MigrationPayload)
	default:
		return -1
	}
}

// commitBodyLen is the length of c's CommitPayload record body, or -1 if c has an operation the
// direct encoder does not know or an attachment root.
func commitBodyLen(c *Commit) int {
	if c.AttachmentTreeHash != nil {
		return -1 // field 10: left to the generic codec
	}
	n := 2 + lebLenOf(len(c.ParentHashes)) + 32*len(c.ParentHashes)
	n += 2 + lebLenOf(len(c.NamespaceID)) + len(c.NamespaceID)
	n += 2 + 16 // transactionId
	n += 2 + 8  // timestamp
	n += 2 + 16 // authorNodeId
	n += 2 + lebLenOf(len(c.Operations))
	for _, op := range c.Operations {
		b := opBodyLen(op)
		if b < 0 {
			return -1
		}
		n += 1 + lebLenOf(b) + b
	}
	n += 2 + 32 // documentTreeHash
	if c.SchemaHash != nil {
		n += 2 + 1 + 32
	}
	n += 2 + lebLenOf(len(c.Message)) + len(c.Message)
	return n
}

func appendLeb(dst []byte, v int) []byte {
	return binary.AppendUvarint(dst, uint64(uint32(v)))
}

func appendUUID(dst []byte, u codec.UUID) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(u.MSB))
	return binary.BigEndian.AppendUint64(dst, uint64(u.LSB))
}

func appendStr(dst []byte, s string) []byte {
	dst = appendLeb(dst, len(s))
	return append(dst, s...)
}

// appendCommitPayload appends c's canonical payload bytes, given its body length from
// commitBodyLen.
func appendCommitPayload(dst []byte, c *Commit, body int) []byte {
	dst = appendLeb(dst, body)
	dst = append(dst, 1, tagArray)
	dst = appendLeb(dst, len(c.ParentHashes))
	for i := range c.ParentHashes {
		dst = append(dst, c.ParentHashes[i].Bytes[:]...)
	}
	dst = append(dst, 2, tagString)
	dst = appendStr(dst, c.NamespaceID)
	dst = append(dst, 3, tagFixed)
	dst = appendUUID(dst, c.TransactionID)
	dst = append(dst, 4, tagInt64)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(c.Timestamp.EpochMicros()))
	dst = append(dst, 5, tagFixed)
	dst = appendUUID(dst, c.AuthorNodeID)
	dst = append(dst, 6, tagArray)
	dst = appendLeb(dst, len(c.Operations))
	for _, op := range c.Operations {
		b := opBodyLen(op)
		switch o := op.(type) {
		case WriteOp:
			dst = append(dst, 0)
			dst = appendLeb(dst, b)
			dst = append(dst, 1, tagFixed)
			dst = appendUUID(dst, o.DocID)
			dst = append(dst, 2, tagString)
			dst = appendStr(dst, o.Patch)
		case DeleteOp:
			dst = append(dst, 1)
			dst = appendLeb(dst, b)
			dst = append(dst, 1, tagFixed)
			dst = appendUUID(dst, o.DocID)
		case FileWriteOp:
			dst = append(dst, 2)
			dst = appendLeb(dst, b)
			dst = append(dst, 1, tagString)
			dst = appendStr(dst, o.Path)
			dst = append(dst, 2, tagFixed)
			dst = append(dst, o.BlobHash.Bytes[:]...)
		case SchemaMigrationOp:
			dst = append(dst, 3)
			dst = appendLeb(dst, b)
			dst = append(dst, 1, tagFixed)
			dst = appendUUID(dst, o.MigrationID)
			dst = append(dst, 2, tagString)
			dst = appendStr(dst, o.MigrationPayload)
		}
	}
	dst = append(dst, 7, tagFixed)
	dst = append(dst, c.DocumentTreeHash.Bytes[:]...)
	if c.SchemaHash != nil {
		dst = append(dst, 8, tagUnion, 1)
		dst = append(dst, c.SchemaHash.Bytes[:]...)
	}
	dst = append(dst, 9, tagString)
	return appendStr(dst, c.Message)
}

// encodeCommitDirect returns c's payload bytes in one allocation, or ok=false for a commit the
// generic codec must encode.
func encodeCommitDirect(c *Commit) (out []byte, ok bool) {
	body := commitBodyLen(c)
	if body < 0 {
		return nil, false
	}
	return appendCommitPayload(make([]byte, 0, lebLenOf(body)+body), c, body), true
}

var hashBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

// hashCommitDirect returns the SHA-256 of c's payload bytes, encoding them into a pooled buffer.
func hashCommitDirect(c *Commit) (codec.Hash, bool) {
	body := commitBodyLen(c)
	if body < 0 {
		return codec.Hash{}, false
	}
	bp := hashBufPool.Get().(*[]byte)
	b := appendCommitPayload((*bp)[:0], c, body)
	h := codec.Hash{Bytes: sha256.Sum256(b)}
	if cap(b) <= 64<<10 { // a commit with a huge patch should not pin its buffer in the pool
		*bp = b[:0]
		hashBufPool.Put(bp)
	}
	return h, true
}

// decodeCommitDirect decodes canonical payload bytes; ok=false means the bytes are not in the
// canonical layout (or not valid) and the generic decoder must decide.
func decodeCommitDirect(payload []byte) (c Commit, ok bool) {
	s := commitScanner{raw: payload}
	slab, err := s.lenPrefixed("commit payload")
	if err != nil || s.pos != len(payload) {
		return Commit{}, false
	}
	r := commitScanner{raw: slab}
	field := func(id uint64, tag byte) bool {
		fid, err := r.uvarint("")
		if err != nil || fid != id {
			return false
		}
		t, err := r.byte("")
		return err == nil && t == tag
	}
	if !field(1, tagArray) {
		return Commit{}, false
	}
	n, err := r.uvarint("")
	if err != nil || n > uint64(len(r.raw)-r.pos)/32 {
		return Commit{}, false
	}
	c.ParentHashes = make([]codec.Hash, n)
	for i := range c.ParentHashes {
		copy(c.ParentHashes[i].Bytes[:], r.raw[r.pos:])
		r.pos += 32
	}
	if !field(2, tagString) {
		return Commit{}, false
	}
	ns, err := r.lenPrefixed("")
	if err != nil {
		return Commit{}, false
	}
	c.NamespaceID = string(ns)
	if !field(3, tagFixed) || r.skip(16, "") != nil {
		return Commit{}, false
	}
	c.TransactionID = uuidAt(r.raw[r.pos-16:])
	if !field(4, tagInt64) || r.skip(8, "") != nil {
		return Commit{}, false
	}
	c.Timestamp = codec.TimestampFromEpochMicros(int64(binary.LittleEndian.Uint64(r.raw[r.pos-8:])))
	if !field(5, tagFixed) || r.skip(16, "") != nil {
		return Commit{}, false
	}
	c.AuthorNodeID = uuidAt(r.raw[r.pos-16:])
	if !field(6, tagArray) {
		return Commit{}, false
	}
	n, err = r.uvarint("")
	if err != nil || n > uint64(len(r.raw)-r.pos) {
		return Commit{}, false
	}
	c.Operations = make([]Op, n)
	for i := range c.Operations {
		branch, err := r.byte("")
		if err != nil {
			return Commit{}, false
		}
		body, err := r.lenPrefixed("")
		if err != nil {
			return Commit{}, false
		}
		op, ok := decodeOpDirect(branch, body)
		if !ok {
			return Commit{}, false
		}
		c.Operations[i] = op
	}
	if !field(7, tagFixed) || r.skip(32, "") != nil {
		return Commit{}, false
	}
	copy(c.DocumentTreeHash.Bytes[:], r.raw[r.pos-32:])
	// schemaHash (8) is present only when set; a written-out null is left to the generic decoder.
	if r.pos < len(r.raw) && r.raw[r.pos] == 8 {
		if !field(8, tagUnion) {
			return Commit{}, false
		}
		if m, err := r.byte(""); err != nil || m != 1 || r.skip(32, "") != nil {
			return Commit{}, false
		}
		h := new(codec.Hash)
		copy(h.Bytes[:], r.raw[r.pos-32:])
		c.SchemaHash = h
	}
	if !field(9, tagString) {
		return Commit{}, false
	}
	msg, err := r.lenPrefixed("")
	if err != nil || r.pos != len(r.raw) {
		return Commit{}, false
	}
	c.Message = string(msg)
	c.Hash = codec.Hash{Bytes: sha256.Sum256(payload)}
	return c, true
}

// decodeOpDirect decodes one canonical operation record body (fields in id order, none unknown).
func decodeOpDirect(branch byte, body []byte) (Op, bool) {
	r := commitScanner{raw: body}
	field := func(id uint64, tag byte) bool {
		fid, err := r.uvarint("")
		if err != nil || fid != id {
			return false
		}
		t, err := r.byte("")
		return err == nil && t == tag
	}
	uuid := func(id uint64) (codec.UUID, bool) {
		if !field(id, tagFixed) || r.skip(16, "") != nil {
			return codec.UUID{}, false
		}
		return uuidAt(r.raw[r.pos-16:]), true
	}
	str := func(id uint64) (string, bool) {
		if !field(id, tagString) {
			return "", false
		}
		b, err := r.lenPrefixed("")
		if err != nil {
			return "", false
		}
		return string(b), true
	}
	var op Op
	switch branch {
	case 0:
		id, ok := uuid(1)
		if !ok {
			return nil, false
		}
		patch, ok := str(2)
		if !ok {
			return nil, false
		}
		op = WriteOp{DocID: id, Patch: patch}
	case 1:
		id, ok := uuid(1)
		if !ok {
			return nil, false
		}
		op = DeleteOp{DocID: id}
	case 2:
		path, ok := str(1)
		if !ok || !field(2, tagFixed) || r.skip(32, "") != nil {
			return nil, false
		}
		var h codec.Hash
		copy(h.Bytes[:], r.raw[r.pos-32:])
		op = FileWriteOp{Path: path, BlobHash: h}
	case 3:
		id, ok := uuid(1)
		if !ok {
			return nil, false
		}
		payload, ok := str(2)
		if !ok {
			return nil, false
		}
		op = SchemaMigrationOp{MigrationID: id, MigrationPayload: payload}
	default:
		return nil, false
	}
	if r.pos != len(r.raw) {
		return nil, false
	}
	return op, true
}

func uuidAt(b []byte) codec.UUID {
	return codec.UUID{MSB: int64(binary.BigEndian.Uint64(b)), LSB: int64(binary.BigEndian.Uint64(b[8:]))}
}
