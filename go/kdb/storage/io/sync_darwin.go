package io

import (
	"os"
	"syscall"
)

// fcntl F_BARRIERFSYNC (undeclared in the syscall package): flush the file's
// dirty data to the device and issue a write barrier behind it, without
// forcing the device cache to media the way F_FULLFSYNC (what os.File.Sync
// issues on darwin) does. The barrier keeps cross-file write ordering, which
// is what log-structured recovery actually needs from a sync.
const fBarrierFsync = 85

func syncFile(f *os.File, mode SyncMode) error {
	if mode == SyncModeFast {
		// Through SyscallConn rather than f.Fd(): Control holds the descriptor open for the
		// call, where a raw Fd() can be closed under it by a concurrent Close (OSByteStore closes
		// a sealed segment's handle while a group-commit flush may still be using it) and its
		// number reused by an unrelated open - syncing the wrong file and reporting success. A
		// closed file falls through to f.Sync, which reports os.ErrClosed.
		var errno syscall.Errno = syscall.EBADF
		if rc, err := f.SyscallConn(); err == nil {
			_ = rc.Control(func(fd uintptr) {
				_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, fBarrierFsync, 0)
			})
		}
		if errno == 0 {
			return nil
		}
		// Filesystems without barrier support (network mounts, some FUSE
		// filesystems): fall through to the full sync rather than not syncing.
	}
	return f.Sync()
}
