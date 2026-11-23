package io

import (
	"errors"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

// unnamedFilesSupported reports whether this platform can create a file with no name and give
// it one later (createUnnamedFile, linkUnnamedFile). Spare delta segments are built on it.
const unnamedFilesSupported = true

// oTmpfile is O_TMPFILE: __O_TMPFILE (020000000 on every Linux port Go supports) together with
// O_DIRECTORY, which the kernel requires alongside it so that a kernel without O_TMPFILE fails
// the open (EISDIR) instead of silently opening the directory.
const oTmpfile = 0x400000 | syscall.O_DIRECTORY

const (
	atSymlinkFollow = 0x400
	atEmptyPath     = 0x1000
)

// createUnnamedFile creates a regular file in dir that has no name: it takes disk space but no
// directory entry, so nothing listing dir can see it and a crash leaves nothing behind - the
// kernel frees an unnamed file when its last descriptor closes. linkUnnamedFile names it.
//
// A filesystem without O_TMPFILE (some network and FUSE mounts) reports an error, and callers
// fall back to creating the file by name.
func createUnnamedFile(dir string) (*os.File, error) {
	return os.OpenFile(dir, os.O_WRONLY|oTmpfile, 0o644)
}

// linkUnnamedFile gives f, created by createUnnamedFile, the name path. It fails with EEXIST
// when path already exists, and never replaces it.
//
// linkat through /proc/self/fd/N is the documented way to name an O_TMPFILE file without
// privileges (open(2)); AT_EMPTY_PATH on the descriptor itself is tried after it, for a process
// without /proc that holds CAP_DAC_READ_SEARCH.
func linkUnnamedFile(f *os.File, path string) error {
	fd := int(f.Fd())
	err := linkat(atFDCWD, "/proc/self/fd/"+strconv.Itoa(fd), atFDCWD, path, atSymlinkFollow)
	if err == nil || errors.Is(err, syscall.EEXIST) {
		return err
	}
	if err2 := linkat(fd, "", atFDCWD, path, atEmptyPath); err2 == nil || errors.Is(err2, syscall.EEXIST) {
		return err2
	}
	return &os.LinkError{Op: "linkat", Old: f.Name(), New: path, Err: err}
}

// atFDCWD is AT_FDCWD (-100), a variable so it converts to the uintptr linkat takes.
var atFDCWD = -100

func linkat(olddirfd int, oldpath string, newdirfd int, newpath string, flags int) error {
	oldp, err := syscall.BytePtrFromString(oldpath)
	if err != nil {
		return err
	}
	newp, err := syscall.BytePtrFromString(newpath)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_LINKAT, uintptr(olddirfd), uintptr(unsafe.Pointer(oldp)),
		uintptr(newdirfd), uintptr(unsafe.Pointer(newp)), uintptr(flags), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
