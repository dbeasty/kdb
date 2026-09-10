package io

import (
	"os"
	"syscall"
	"unsafe"
)

// fstore_t flags and the F_PREALLOCATE command, none of which the syscall
// package declares on darwin.
const (
	fPreallocate     = 42
	fAllocateContig  = 2 // allocate contiguously
	fAllocateAll     = 4 // allocate all or nothing
	fPosModeFileSize = 3 // length is an absolute file size, not an offset
)

type fstore struct {
	flags      uint32
	posmode    int32
	offset     int64
	length     int64
	bytesalloc int64
}

// preallocateFile sizes f to bytes with its extents allocated and initialized.
//
// darwin has no fallocate(2); F_PREALLOCATE reserves space instead. Try for a
// contiguous run first and fall back to a fragmented allocation, which is what
// the fcntl man page recommends and what a fragmented volume will actually be
// able to satisfy. Either way the reservation alone does not move the file
// size or initialize anything, so zeroFill does both.
//
// macOS is not the deployment target for this work (see the plan doc - the
// metadata-commit cost this removes is a Linux fdatasync concern), so this
// path exists to keep behavior identical for developers running the tests on a
// Mac rather than because it is expected to pay off here.
func preallocateFile(f *os.File, bytes int64) error {
	st := fstore{
		flags:   fAllocateContig | fAllocateAll,
		posmode: fPosModeFileSize,
		length:  bytes,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fPreallocate, uintptr(unsafe.Pointer(&st)))
	if errno != 0 {
		st.flags = fAllocateAll
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fPreallocate, uintptr(unsafe.Pointer(&st)))
		// A failed reservation is not fatal: zeroFill still produces a
		// correctly sized, fully written file, just without the contiguity.
	}
	return zeroFill(f, bytes)
}
