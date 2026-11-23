package dag

import (
	"os"
	"syscall"
)

// syncGraphFile syncs a segment plainly. On macOS os.File.Sync is F_FULLFSYNC, which flushes the
// whole device cache and stalls every neighbouring writer for milliseconds - the commit log
// included - and segments are flushed every few seconds under load. A segment is a cache: one torn
// by a power loss fails its checksum and costs a rebuild or a replay, never a misread.
func syncGraphFile(f *os.File) error { return syscall.Fsync(int(f.Fd())) }

// graphTrickleBytes is how often a segment write syncs what it has written so far (0: only at the
// end). Off Linux there is no trickle - see the Linux file.
const graphTrickleBytes = 0
