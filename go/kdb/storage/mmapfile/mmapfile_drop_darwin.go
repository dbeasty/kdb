package mmapfile

import (
	"syscall"
	"unsafe"
)

// dropResident takes b's pages out of the process's resident set. On Darwin MADV_DONTNEED only
// deactivates a file mapping's pages and leaves them resident (measured: no change in RSS), while
// msync(MS_INVALIDATE) drops them; a later read faults them back in. Safe here because every
// mapping this package makes is read-only, so there is nothing dirty to lose.
func dropResident(b []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), syscall.MS_INVALIDATE)
	if errno != 0 {
		return errno
	}
	return nil
}
