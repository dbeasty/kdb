//go:build unix

package mmapfile

import (
	"fmt"
	"os"
	"syscall"
)

// Open maps path read-only. An empty file maps to an empty Mapping (mmap rejects length zero).
func Open(path string) (*Mapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size == 0 {
		return &Mapping{}, nil
	}
	if int64(int(size)) != size {
		return nil, fmt.Errorf("mmapfile: %s is too large to map (%d bytes)", path, size)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmapfile: map %s: %w", path, err)
	}
	mappedBytes.Add(int64(len(data)))
	m := &Mapping{data: data, unmap: func() error { return syscall.Munmap(data) }}
	register(m)
	return m, nil
}

// Supported reports whether Open can map files on this platform.
const Supported = true
