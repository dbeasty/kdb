package mmapfile

import (
	"syscall"
	"unsafe"
)

// dropResident takes b's pages out of the process's resident set. On Linux MADV_DONTNEED on a
// file mapping unmaps the pages from this process at once; they stay in the page cache, and a
// later read faults them back in from it.
func dropResident(b []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), syscall.MADV_DONTNEED)
	if errno != 0 {
		return errno
	}
	return nil
}
