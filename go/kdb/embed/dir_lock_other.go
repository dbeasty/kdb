//go:build !unix

package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// dirLock on non-unix platforms (windows, js/wasm, plan9, ...) has no access to flock(2), so it
// falls back to an O_EXCL create-as-lock: the first opener creates the file and holds it, a
// second opener's O_EXCL create fails while it exists. Unlike flock, this does NOT release
// automatically if the process dies without calling Release - a crashed holder leaves a stale
// lock file that must be removed by hand. That tradeoff is acceptable here because the primary
// deployment target (go/cmd/kdb-service) is unix; this exists so the embed package - and
// anything that imports it, including the GOOS=js/wasm build - compiles and has a directionally
// correct single-process guard everywhere else, not to make Windows/WASM a supported multi-
// process deployment target.
type dirLock struct {
	f    *os.File
	path string
}

// acquireDirLockShared has no shared mode to offer on these platforms: the O_EXCL fallback below
// is a single-holder primitive with no reader/writer distinction to express. It returns an error
// rather than silently degrading to an exclusive lock (which would make a "read-only replica"
// exclude the writer it is replicating from) or to no lock at all (which would let a reader
// attach to a directory being mutated underneath it).
func acquireDirLockShared(dataRoot string) (*dirLock, error) {
	return nil, fmt.Errorf("read-only data directory access requires flock(2), unavailable on this platform: %s", dataRoot)
}

// acquireDirLockExclusive is acquireDirLock here: with only one lock mode available, the
// single-holder create-as-lock already excludes everyone.
func acquireDirLockExclusive(dataRoot string) (*dirLock, error) {
	return acquireDirLock(dataRoot)
}

func acquireDirLock(dataRoot string) (*dirLock, error) {
	lockPath := filepath.Join(dataRoot, ".kdb.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("data directory locked: %s", lockPath)
		}
		return nil, err
	}
	// Best-effort holder hint.
	_, _ = f.WriteString(fmt.Sprintf("pid=%d\nruntime=%s\n", os.Getpid(), runtime.Version()))
	_ = f.Sync()
	return &dirLock{f: f, path: lockPath}, nil
}

func (l *dirLock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = l.f.Close()
	_ = os.Remove(l.path)
	l.f = nil
}
