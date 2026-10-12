//go:build windows

package embed

import (
	"os"

	"golang.org/x/sys/windows"
)

// Windows byte-range locks are mandatory: a locked range cannot be read or written through any
// other handle. Locking the file's data would make the holder hint unreadable, so the lock covers
// one byte far past anything ever written. LockFileEx accepts ranges beyond end of file, and
// shared/exclusive on the same range give exactly flock's LOCK_SH/LOCK_EX relationships. The
// kernel releases the lock when the handle closes, including when the process dies.
const (
	lockOffsetHigh = 0x7fffffff
	lockOffsetLow  = 0xfffffffe
)

func lockOverlapped() *windows.Overlapped {
	return &windows.Overlapped{Offset: lockOffsetLow, OffsetHigh: lockOffsetHigh}
}

func lockFile(f *os.File, exclusive bool) error {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	return windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, lockOverlapped())
}

func unlockFile(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, lockOverlapped())
}
