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
		//
		// Through SyscallConn rather than f.Fd(): Control holds the descriptor open for the
		// call, where a raw Fd() can be closed under it by a concurrent Close (OSByteStore closes
		// a sealed segment's handle while a group-commit flush may still be using it) and its
		// number reused by an unrelated open - syncing the wrong file and reporting success. A
		// closed file falls through to f.Sync, which reports os.ErrClosed.
		serr := error(syscall.EBADF)
		if rc, err := f.SyscallConn(); err == nil {
			_ = rc.Control(func(fd uintptr) { serr = syscall.Fdatasync(int(fd)) })
		}
		if serr == nil {
			return nil
		}
	}
	return f.Sync()
}
