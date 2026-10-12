package dag

import (
	"encoding/binary"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// A commit record: one commit's fixed-width graph data, in the one layout both tiers use. The
// resident tier holds these in heap chunks and the graph file holds them mapped, so moving a
// commit to disk is a byte copy, and every read decodes the same bytes the same way whichever
// tier they came from.
//
// Little-endian, graphRecordSize bytes:
//
//	  0  hash          32
//	 32  parent 0      32
//	 64  parent 1      32
//	 96  tree hash     32
//	128  schema hash   32   (valid when flags & grFlagSchema)
//	160  tx            16   (MSB u64, LSB u64)
//	176  author        16
//	192  epoch millis  i64
//	200  micro rem.    i64
//	208  generation    u32
//	212  parent count  u32  (parents beyond two live in the extra blob / side map)
//	216  flags         u32
//	220  message len   u32
//	224  message off   u64  (file only: offset into the msgs blob)
//	232  extra off     u64  (file only: offset into the extra blob)
//	240  ns index      u16  (memory only: index into the store's namespace table; 0 on disk)
//	242  reserved      14   (zero; room for fields a later version adds without a new layout)
//
// The spare bytes are deliberate. A format with no slack forces a version bump and a rewrite of
// every file for the smallest addition; 14 bytes covers a further timestamp, a second generation
// number, or a position for the deferred position-indexed cursor.
const (
	graphRecordSize = 256

	grHash     = 0
	grParent0  = 32
	grParent1  = 64
	grTree     = 96
	grSchema   = 128
	grTxMSB    = 160
	grTxLSB    = 168
	grAuthMSB  = 176
	grAuthLSB  = 184
	grEpochMs  = 192
	grMicro    = 200
	grGen      = 208
	grNParents = 212
	grFlags    = 216
	grMsgLen   = 220
	grMsgOff   = 224
	grExtraOff = 232
	grNsIdx    = 240
	grReserved = 242

	grFlagSchema = 1
)

var le = binary.LittleEndian

func recHash(r []byte) (h codec.Hash) {
	copy(h.Bytes[:], r[grHash:grHash+32])
	return h
}

func recTree(r []byte) (h codec.Hash) {
	copy(h.Bytes[:], r[grTree:grTree+32])
	return h
}

func recTx(r []byte) codec.UUID {
	return codec.UUID{MSB: int64(le.Uint64(r[grTxMSB:])), LSB: int64(le.Uint64(r[grTxLSB:]))}
}

func recGeneration(r []byte) uint32       { return le.Uint32(r[grGen:]) }
func setRecGeneration(r []byte, g uint32) { le.PutUint32(r[grGen:], g) }
func recNParents(r []byte) uint32         { return le.Uint32(r[grNParents:]) }
func recNsIdx(r []byte) uint16            { return le.Uint16(r[grNsIdx:]) }

// recParents01 returns the inline parents and the total parent count.
func recParents01(r []byte) (p0, p1 codec.Hash, n uint32) {
	copy(p0.Bytes[:], r[grParent0:grParent0+32])
	copy(p1.Bytes[:], r[grParent1:grParent1+32])
	return p0, p1, recNParents(r)
}

// encodeRecord writes c into r (graphRecordSize bytes). Blob offsets are left zero: only the file
// writer knows them.
func encodeRecord(r []byte, c document.Commit, gen uint32, nsIdx uint16) {
	clear(r[:graphRecordSize])
	copy(r[grHash:], c.Hash.Bytes[:])
	if len(c.ParentHashes) > 0 {
		copy(r[grParent0:], c.ParentHashes[0].Bytes[:])
	}
	if len(c.ParentHashes) > 1 {
		copy(r[grParent1:], c.ParentHashes[1].Bytes[:])
	}
	copy(r[grTree:], c.DocumentTreeHash.Bytes[:])
	var flags uint32
	if c.SchemaHash != nil {
		copy(r[grSchema:], c.SchemaHash.Bytes[:])
		flags |= grFlagSchema
	}
	le.PutUint64(r[grTxMSB:], uint64(c.TransactionID.MSB))
	le.PutUint64(r[grTxLSB:], uint64(c.TransactionID.LSB))
	le.PutUint64(r[grAuthMSB:], uint64(c.AuthorNodeID.MSB))
	le.PutUint64(r[grAuthLSB:], uint64(c.AuthorNodeID.LSB))
	le.PutUint64(r[grEpochMs:], uint64(c.Timestamp.EpochMillis))
	le.PutUint64(r[grMicro:], uint64(int64(c.Timestamp.MicroRemainder)))
	le.PutUint32(r[grGen:], gen)
	le.PutUint32(r[grNParents:], uint32(len(c.ParentHashes)))
	le.PutUint32(r[grFlags:], flags)
	le.PutUint32(r[grMsgLen:], uint32(len(c.Message)))
	le.PutUint16(r[grNsIdx:], nsIdx)
}

// decodeRecord rebuilds the commit r describes. msg and extra come from wherever the tier keeps
// variable-length data; everything returned is a copy, never a reference into r.
func decodeRecord(r []byte, ns, msg string, extra []codec.Hash, ops []document.Op) document.Commit {
	c := document.Commit{
		Hash:             recHash(r),
		NamespaceID:      ns,
		TransactionID:    recTx(r),
		Timestamp:        codec.Timestamp{EpochMillis: int64(le.Uint64(r[grEpochMs:])), MicroRemainder: int(int64(le.Uint64(r[grMicro:])))},
		AuthorNodeID:     codec.UUID{MSB: int64(le.Uint64(r[grAuthMSB:])), LSB: int64(le.Uint64(r[grAuthLSB:]))},
		DocumentTreeHash: recTree(r),
		Message:          msg,
		Operations:       ops,
	}
	p0, p1, n := recParents01(r)
	if n > 0 {
		c.ParentHashes = make([]codec.Hash, 0, n)
		c.ParentHashes = append(c.ParentHashes, p0)
		if n > 1 {
			c.ParentHashes = append(c.ParentHashes, p1)
		}
		if n > 2 {
			c.ParentHashes = append(c.ParentHashes, extra...)
		}
	}
	if le.Uint32(r[grFlags:])&grFlagSchema != 0 {
		var sc codec.Hash
		copy(sc.Bytes[:], r[grSchema:grSchema+32])
		c.SchemaHash = &sc
	}
	return c
}
