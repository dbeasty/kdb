// Package fsx holds the handful of file operations whose POSIX behaviour KDB relies on and Windows
// does not give by default.
//
// Three differences matter, and each function here exists for one of them:
//
//   - Open files block deletes and renames. Go's os.OpenFile on Windows opens without
//     FILE_SHARE_DELETE, so while any handle is open the file can be neither removed nor renamed
//     over. The storage engine deletes segments that a concurrent reader may still hold; on unix
//     that reader keeps its inode and the delete succeeds. OpenFile/Open here pass
//     FILE_SHARE_DELETE so the same pattern works.
//   - Other processes open files briefly. Antivirus scanners and the search indexer open freshly
//     written files without FILE_SHARE_DELETE, so a rename or remove can fail with a sharing
//     violation for a few milliseconds for reasons outside this process. Rename, Remove and
//     RemoveAll retry those errors for a short while.
//   - Directories cannot be fsynced. FlushFileBuffers on a directory handle is refused. NTFS
//     journals the directory entry itself, and Rename asks for MOVEFILE_WRITE_THROUGH, which is
//     what makes the rename durable there; SyncDir is therefore a no-op on Windows.
//
// On every other platform each function is exactly its os counterpart (SyncDir is
// open-the-directory-and-fsync), so callers can use them unconditionally.
package fsx

import "os"

// Open opens name for reading. See OpenFile.
func Open(name string) (*os.File, error) {
	return OpenFile(name, os.O_RDONLY, 0)
}
