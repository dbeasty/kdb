package io

import (
	"os"
	"runtime"
)

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
	return zeroFillTrickle(f, bytes, 0)
}

// zeroFillTrickle is zeroFill, syncing the data written so far every trickle bytes (0: only at
// the end) - see trickleSyncBytes.
func zeroFillTrickle(f *os.File, bytes, trickle int64) error {
	if bytes <= 0 {
		return nil
	}
	buf := make([]byte, zeroFillChunk)
	var written, synced int64
	for written < bytes {
		n := int64(len(buf))
		if remaining := bytes - written; remaining < n {
			n = remaining
		}
		if _, err := f.WriteAt(buf[:n], written); err != nil {
			return err
		}
		written += n
		if trickle > 0 && written-synced >= trickle && written < bytes {
			if err := syncFile(f, SyncModeFast); err != nil {
				return err
			}
			synced = written
		}
	}
	// The zeros have to reach the device before the segment is used, or a crash
	// could leave the range unallocated after all and the first real append
	// would pay the metadata commit anyway - having spent the zero-fill cost for
	// nothing.
	return f.Sync()
}

// trickleSyncBytes is how much a background writer of a large file - a spare segment's zeros, an
// SSTable - writes between syncs, on Linux.
//
// A durable commit waits for the commit log's fdatasync, and a sync waits for the device to take
// whatever is queued ahead of it. Written in one go and synced once at the end, a 64 MiB spare or
// a flushed memtable's SSTable put tens of megabytes in that queue at once: traced under durable
// updates at 32 workers, the commit log's fdatasync took 19-30 ms exactly while one of those final
// syncs ran, and the slowest commits waited 30-36 ms for it. Syncing every megabyte keeps what any
// one sync can find queued to about a megabyte. Cassandra's trickle_fsync (trickle_fsync_interval)
// does the same for its SSTable and commit-log writers, for the same reason.
//
// Off Linux there is no trickle: on macOS the sync that reaches the device (F_FULLFSYNC) costs
// milliseconds each, and neighbours pay for every one of them.
const trickleSyncBytes = 1 << 20

// trickleSupported is whether background writers sync as they go (see trickleSyncBytes).
var trickleSupported = runtime.GOOS == "linux"
