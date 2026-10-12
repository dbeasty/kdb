//go:build windows

package fsx

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// OpenFile is os.OpenFile with FILE_SHARE_DELETE added to the share mode, so that a file this
// process holds open can still be removed or renamed - by this process or another - as it can on
// unix. Flag handling mirrors syscall.Open.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if name == "" {
		return nil, &os.PathError{Op: "open", Path: name, Err: syscall.ENOENT}
	}
	namep, err := windows.UTF16PtrFromString(longPath(name))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	var access uint32
	switch flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		access = windows.GENERIC_READ
	case os.O_WRONLY:
		access = windows.GENERIC_WRITE
	case os.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	}
	if flag&os.O_CREATE != 0 {
		access |= windows.GENERIC_WRITE
	}
	if flag&os.O_APPEND != 0 {
		// As syscall.Open: every write right except FILE_WRITE_DATA, which is what makes writes
		// land at the end of the file. O_TRUNC keeps GENERIC_WRITE because truncating needs it.
		if flag&os.O_TRUNC == 0 {
			access &^= windows.GENERIC_WRITE
		}
		access |= windows.FILE_APPEND_DATA | windows.FILE_WRITE_ATTRIBUTES | windows.FILE_WRITE_EA |
			windows.STANDARD_RIGHTS_WRITE | windows.SYNCHRONIZE
	}

	attrs := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	if perm&0o200 == 0 && flag&os.O_CREATE != 0 {
		attrs = windows.FILE_ATTRIBUTE_READONLY
	}
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		// Needed to open a directory at all, and harmless for a file.
		attrs |= windows.FILE_FLAG_BACKUP_SEMANTICS
	}
	if flag&os.O_SYNC != 0 {
		attrs |= windows.FILE_FLAG_WRITE_THROUGH
	}

	var createmode uint32
	switch {
	case flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL:
		createmode = windows.CREATE_NEW
	case flag&os.O_CREATE != 0:
		createmode = windows.OPEN_ALWAYS
	default:
		createmode = windows.OPEN_EXISTING
	}

	// A nil SecurityAttributes leaves the handle non-inheritable: the O_CLOEXEC behaviour os gives.
	h, err := windows.CreateFile(namep, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, createmode, attrs, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	f := os.NewFile(uintptr(h), name)
	// Truncate after opening rather than using CREATE_ALWAYS/TRUNCATE_EXISTING, as syscall.Open
	// does (go.dev/issue/38225).
	if flag&os.O_TRUNC != 0 && flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}

// Rename replaces newpath with oldpath. MOVEFILE_WRITE_THROUGH makes it return only once the move
// is on disk - the guarantee unix callers get from fsyncing the parent directory, which SyncDir
// cannot do here. Transient sharing violations are retried; see retry.
func Rename(oldpath, newpath string) error {
	from, err := windows.UTF16PtrFromString(longPath(oldpath))
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	to, err := windows.UTF16PtrFromString(longPath(newpath))
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	err = retry(func() error {
		return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
	})
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	return nil
}

// Remove is os.Remove, retried across transient sharing violations.
func Remove(name string) error {
	return retry(func() error { return os.Remove(name) })
}

// RemoveAll is os.RemoveAll, retried across transient sharing violations.
func RemoveAll(path string) error {
	return retry(func() error { return os.RemoveAll(path) })
}

// SyncDir is a no-op on Windows: a directory handle cannot be flushed (FlushFileBuffers is refused
// with ERROR_ACCESS_DENIED), and NTFS journals directory entries itself. Rename's
// MOVEFILE_WRITE_THROUGH is what carries the durability unix gets from fsyncing the directory.
func SyncDir(string) error { return nil }

// retryWindow bounds how long a rename or remove keeps trying. Scanners hold a new file for
// milliseconds; anything still blocked after this is a real conflict and is reported.
const retryWindow = 2 * time.Second

// retry runs op until it succeeds, fails with something other than a sharing-style error, or
// retryWindow has passed. ERROR_ACCESS_DENIED is included because that is what DeleteFile and
// MoveFileEx report for a file that is open without FILE_SHARE_DELETE or is delete-pending.
func retry(op func() error) error {
	deadline := time.Now().Add(retryWindow)
	delay := time.Millisecond
	for {
		err := op()
		if err == nil || !transient(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(delay)
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
}

func transient(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case windows.ERROR_ACCESS_DENIED, windows.ERROR_SHARING_VIOLATION, windows.ERROR_LOCK_VIOLATION:
		return true
	}
	return false
}

// longPath adds the \\?\ prefix to an absolute path long enough to hit MAX_PATH, which os does
// internally for its own calls and the raw Win32 calls above would otherwise not get. Nested
// namespace ids can be up to 200 characters on their own, so a data root need not be unusual
// for a segment path to cross 260.
func longPath(p string) string {
	const limit = 248 // CreateDirectory's limit, the tightest of the Win32 path limits
	if len(p) < limit || strings.HasPrefix(p, `\\?\`) || !filepath.IsAbs(p) {
		return p
	}
	p = filepath.Clean(p)
	if strings.HasPrefix(p, `\\`) {
		return `\\?\UNC\` + p[2:]
	}
	return `\\?\` + p
}
