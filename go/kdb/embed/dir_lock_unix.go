//go:build unix

package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// Two lock files, because "who may open this directory" and "who may write to it" are different
// questions and collapsing them into one made read replicas impossible.
//
//	.kdb.lock       - the ATTACH lock. Every runtime, writer or reader, holds it SHARED for as
//	                  long as it has the directory open. Maintenance tooling (kdb-inspect
//	                  verify/repair/restore/backup, via LockDataDir) takes it EXCLUSIVE, which is
//	                  what proves nothing else has the directory open.
//	.kdb.write.lock - the WRITER lock, held EXCLUSIVE by a writable runtime only. This is what
//	                  keeps two writers apart.
//
// Splitting them gives the three relationships that actually matter: many readers coexist; at
// most one writer exists; readers and that one writer coexist; and maintenance excludes
// everyone. Previously a writer held .kdb.lock exclusively, so a reader could only attach to a
// directory whose writer had stopped - a "read replica" that required the thing it was
// replicating to be down.
//
// Mixed versions stay safe by construction: an older binary takes .kdb.lock EXCLUSIVE to write,
// which blocks every new-style shared attach. The failure mode of running old and new together
// is refusing to open, never two writers.
const (
	attachLockName = ".kdb.lock"
	writeLockName  = ".kdb.write.lock"
)

type dirLock struct {
	attach *os.File
	// write is nil for readers and for the exclusive maintenance lock.
	write *os.File
}

// acquireDirLock takes the writer's locks: a shared attach and the exclusive writer lock.
func acquireDirLock(dataRoot string) (*dirLock, error) {
	attach, err := flockFile(dataRoot, attachLockName, syscall.LOCK_SH)
	if err != nil {
		return nil, fmt.Errorf("data directory is locked for maintenance: %s", filepath.Join(dataRoot, attachLockName))
	}
	write, err := flockFile(dataRoot, writeLockName, syscall.LOCK_EX)
	if err != nil {
		_ = unflock(attach)
		return nil, fmt.Errorf("data directory locked: %s", filepath.Join(dataRoot, writeLockName))
	}
	// Best-effort holder hint, written only by the single writer - concurrent shared holders
	// would interleave into the same offsets and name nobody.
	_, _ = write.Seek(0, 0)
	_, _ = write.WriteString(fmt.Sprintf("pid=%d\nruntime=%s\n", os.Getpid(), runtime.Version()))
	_ = write.Sync()
	return &dirLock{attach: attach, write: write}, nil
}

// acquireDirLockShared takes only the shared attach lock, for a read-only runtime. It coexists
// with a live writer and with other readers, and is excluded only by maintenance tooling.
func acquireDirLockShared(dataRoot string) (*dirLock, error) {
	attach, err := flockFile(dataRoot, attachLockName, syscall.LOCK_SH)
	if err != nil {
		return nil, fmt.Errorf("data directory is locked for maintenance: %s", filepath.Join(dataRoot, attachLockName))
	}
	return &dirLock{attach: attach}, nil
}

// acquireDirLockExclusive takes the attach lock EXCLUSIVE, excluding every runtime - reader and
// writer alike. This is the maintenance lock (LockDataDir).
func acquireDirLockExclusive(dataRoot string) (*dirLock, error) {
	attach, err := flockFile(dataRoot, attachLockName, syscall.LOCK_EX)
	if err != nil {
		return nil, fmt.Errorf("data directory is in use: %s", filepath.Join(dataRoot, attachLockName))
	}
	return &dirLock{attach: attach}, nil
}

func flockFile(dataRoot, name string, mode int) (*os.File, error) {
	path := filepath.Join(dataRoot, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func unflock(f *os.File) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}

func (l *dirLock) Release() {
	if l == nil {
		return
	}
	// Writer lock first: a watcher that sees the attach lock free must not still be excluded by
	// a writer lock this same holder has not dropped yet.
	if l.write != nil {
		_ = unflock(l.write)
		l.write = nil
	}
	if l.attach != nil {
		_ = unflock(l.attach)
		l.attach = nil
	}
}
