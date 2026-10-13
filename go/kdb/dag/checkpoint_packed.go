package dag

import (
	"encoding/binary"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Packed checkpoint commits.
//
// A checkpoint carried its commits as one generic codec record each, and the snapshot built a
// []CheckpointCommit of whole document.Commit values first. At two million commits that was
// gigabytes of transient heap at every checkpoint and again at every open (graph file off). Packed,
// a commit is ~150 bytes of plain fields, appended by the snapshot as it walks the DAG and decoded
// one at a time by RestoreCheckpoint, so neither side ever holds the whole list as structs.
//
// Per commit, little-endian:
//
//	hash 32 | tree 32 | tx 16 | author 16 | epochMs i64 | micro u16 | flags u8 | nParents u8 |
//	opCount u32 | msgLen u32 | parents nParents x 32 | schema 32 (if flags&1) | message msgLen

const packedCommitFixed = 32 + 32 + 16 + 16 + 8 + 2 + 1 + 1 + 4 + 4

// PackedCommitMinBytes is the smallest a packed commit can be (no parents, schema or message), so
// len(packed)/PackedCommitMinBytes is an upper bound on how many commits packed holds.
const PackedCommitMinBytes = packedCommitFixed

func appendPackedCommit(dst []byte, c document.Commit, opCount int) ([]byte, error) {
	if len(c.ParentHashes) > 255 {
		return nil, fmt.Errorf("dag: commit %s has %d parents, more than a packed checkpoint holds", c.Hash.Hex(), len(c.ParentHashes))
	}
	var fixed [packedCommitFixed]byte
	copy(fixed[0:], c.Hash.Bytes[:])
	copy(fixed[32:], c.DocumentTreeHash.Bytes[:])
	binary.LittleEndian.PutUint64(fixed[64:], uint64(c.TransactionID.MSB))
	binary.LittleEndian.PutUint64(fixed[72:], uint64(c.TransactionID.LSB))
	binary.LittleEndian.PutUint64(fixed[80:], uint64(c.AuthorNodeID.MSB))
	binary.LittleEndian.PutUint64(fixed[88:], uint64(c.AuthorNodeID.LSB))
	binary.LittleEndian.PutUint64(fixed[96:], uint64(c.Timestamp.EpochMillis))
	binary.LittleEndian.PutUint16(fixed[104:], uint16(c.Timestamp.MicroRemainder))
	if c.SchemaHash != nil {
		fixed[106] = 1
	}
	fixed[107] = byte(len(c.ParentHashes))
	binary.LittleEndian.PutUint32(fixed[108:], uint32(opCount))
	binary.LittleEndian.PutUint32(fixed[112:], uint32(len(c.Message)))
	dst = append(dst, fixed[:]...)
	for _, p := range c.ParentHashes {
		dst = append(dst, p.Bytes[:]...)
	}
	if c.SchemaHash != nil {
		dst = append(dst, c.SchemaHash.Bytes[:]...)
	}
	return append(dst, c.Message...), nil
}

// ForEachPackedCommit decodes packed commits one at a time. namespaceID fills each commit's
// NamespaceID, which a checkpoint does not store per commit.
func ForEachPackedCommit(packed []byte, namespaceID string, fn func(CheckpointCommit) error) error {
	for off := 0; off < len(packed); {
		if len(packed)-off < packedCommitFixed {
			return fmt.Errorf("dag: packed checkpoint commit truncated at %d", off)
		}
		f := packed[off : off+packedCommitFixed]
		var c document.Commit
		copy(c.Hash.Bytes[:], f[0:32])
		copy(c.DocumentTreeHash.Bytes[:], f[32:64])
		c.TransactionID = codec.UUID{MSB: int64(binary.LittleEndian.Uint64(f[64:])), LSB: int64(binary.LittleEndian.Uint64(f[72:]))}
		c.AuthorNodeID = codec.UUID{MSB: int64(binary.LittleEndian.Uint64(f[80:])), LSB: int64(binary.LittleEndian.Uint64(f[88:]))}
		c.Timestamp = codec.Timestamp{EpochMillis: int64(binary.LittleEndian.Uint64(f[96:])), MicroRemainder: int(binary.LittleEndian.Uint16(f[104:]))}
		hasSchema, nParents := f[106]&1 != 0, int(f[107])
		opCount := int(binary.LittleEndian.Uint32(f[108:]))
		msgLen := int(binary.LittleEndian.Uint32(f[112:]))
		off += packedCommitFixed
		need := nParents*32 + msgLen
		if hasSchema {
			need += 32
		}
		if len(packed)-off < need {
			return fmt.Errorf("dag: packed checkpoint commit %s truncated", c.Hash.Hex())
		}
		if nParents > 0 {
			c.ParentHashes = make([]codec.Hash, nParents)
			for i := range c.ParentHashes {
				copy(c.ParentHashes[i].Bytes[:], packed[off:off+32])
				off += 32
			}
		}
		if hasSchema {
			var sh codec.Hash
			copy(sh.Bytes[:], packed[off:off+32])
			c.SchemaHash = &sh
			off += 32
		}
		c.Message = string(packed[off : off+msgLen])
		off += msgLen
		c.NamespaceID = namespaceID
		if err := fn(CheckpointCommit{Commit: c, OperationCount: opCount}); err != nil {
			return err
		}
	}
	return nil
}

// PackCheckpointCommits packs commits, for a caller holding them as structs.
func PackCheckpointCommits(commits []CheckpointCommit) ([]byte, error) {
	var out []byte
	var err error
	for _, cc := range commits {
		if out, err = appendPackedCommit(out, cc.Commit, cc.OperationCount); err != nil {
			return nil, err
		}
	}
	return out, nil
}
