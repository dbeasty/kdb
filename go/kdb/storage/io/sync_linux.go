package io

import (
	"os"
	"syscall"
)

func syncFile(f *os.File, mode SyncMode) error {
	if mode == SyncModeFast {
		// fdatasync: data plus the metadata needed to read it back (size), but
		// not timestamps and other inode noise - one journal commit fewer than
		// fsync on most filesystems.
		if err := syscall.Fdatasync(int(f.Fd())); err == nil {
			return nil
		}
	}
	return f.Sync()
}
