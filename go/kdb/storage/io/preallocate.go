package io

import "os"

// zeroFillChunk is the buffer size zeroFill writes through. Large enough that a
// 64MiB segment is a few hundred write calls rather than tens of thousands,
// small enough not to be a memory event of its own.
const zeroFillChunk = 1 << 20 // 1MiB

// zeroFill writes `bytes` of zeros to f starting at offset 0.
//
// The zeros are the point, not a side effect. Three ways to make a file N bytes
// long and only this one helps:
//
//   - ftruncate leaves a sparse file: writes land in holes, allocate extents on
//     the spot and update metadata, so a later fdatasync still has metadata to
//     commit.
//   - fallocate alone can leave extents marked "unwritten"; the first write to
//     one flips its state, which is again a metadata update.
//   - actually writing zeros forces every block to an allocated, initialized
//     state, after which appends into the range are in-place overwrites with no
//     metadata to commit at all.
//
// etcd's WAL zero-fills its segments for exactly this reason.
func zeroFill(f *os.File, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	buf := make([]byte, zeroFillChunk)
	var written int64
	for written < bytes {
		n := int64(len(buf))
		if remaining := bytes - written; remaining < n {
			n = remaining
		}
		if _, err := f.WriteAt(buf[:n], written); err != nil {
			return err
		}
		written += n
	}
	// The zeros have to reach the device before the segment is used, or a crash
	// could leave the range unallocated after all and the first real append
	// would pay the metadata commit anyway - having spent the zero-fill cost for
	// nothing.
	return f.Sync()
}
