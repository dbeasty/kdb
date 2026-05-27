package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

type dirLock struct {
	f *os.File
}

func acquireDirLock(dataRoot string) (*dirLock, error) {
	lockPath := filepath.Join(dataRoot, ".kdb.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("data directory locked: %s", lockPath)
	}
	// Best-effort holder hint.
	_, _ = f.Seek(0, 0)
	_, _ = f.WriteString(fmt.Sprintf("pid=%d\nruntime=%s\n", os.Getpid(), runtime.Version()))
	_ = f.Sync()
	return &dirLock{f: f}, nil
}

func (l *dirLock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
}

