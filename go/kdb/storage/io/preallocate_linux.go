package io

import (
	"os"
	"syscall"
)

// preallocateFile sizes f to bytes with its extents allocated *and initialized*,
// so later writes into that range are pure in-place overwrites.
//
// fallocate(2) with mode 0 allocates the extents and updates the file size in
// one call, which is what lets a subsequent fdatasync skip the metadata commit.
// It is not enough on its own on every filesystem: ext4 can hand back extents
// marked "unwritten", and the first write to one still costs a metadata update
// to flip its state. zeroFill afterwards forces every block to a written state,
// which is the whole point - an ftruncate-style sparse file would leave holes
// that allocate on first write and buy nothing.
//
// A filesystem without fallocate support (tmpfs on some kernels, several
// network mounts) returns EOPNOTSUPP/ENOSYS; fall through to the portable
// zero-fill, which is slower to create but produces the same initialized
// extents.
func preallocateFile(f *os.File, bytes int64) error {
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, bytes); err != nil {
		if err != syscall.EOPNOTSUPP && err != syscall.ENOSYS && err != syscall.EINVAL {
			return err
		}
	}
	return zeroFill(f, bytes)
}
