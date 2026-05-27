package io

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OSByteStore is a filesystem-backed SegmentByteStore rooted at PlatformIOConfig.RootDirectory.
// Segment names are validated upstream (must start with "ns/").
type OSByteStore struct {
	root string
}

func NewOSByteStore(config PlatformIOConfig) (*OSByteStore, error) {
	if config.RootDirectory == nil || *config.RootDirectory == "" {
		return nil, fmt.Errorf("os byte store requires root directory")
	}
	return &OSByteStore{root: *config.RootDirectory}, nil
}

func (s *OSByteStore) pathFor(segmentName string) string {
	return filepath.Join(s.root, filepath.FromSlash(segmentName))
}

func (s *OSByteStore) Append(segmentName string, bytes []byte) (int64, error) {
	p := s.pathFor(segmentName)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Write(bytes); err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *OSByteStore) Read(segmentName string, offset int64, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	p := s.pathFor(segmentName)
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	if offset != 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(f, buf)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (s *OSByteStore) Flush(segmentName string, fsync bool) error {
	if !fsync {
		return nil
	}
	p := s.pathFor(segmentName)
	f, err := os.OpenFile(p, os.O_RDONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s *OSByteStore) MarkSealed(segmentName string) error {
	// v1: sealing is advisory; persisted segments are discovered by listing + scanning.
	return nil
}

func (s *OSByteStore) List(prefix string) ([]string, error) {
	rootPrefix := s.pathFor(prefix)
	var out []string
	err := filepath.WalkDir(rootPrefix, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		out = append(out, name)
		return nil
	})
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	return out, err
}

func (s *OSByteStore) Delete(segmentName string) error {
	p := s.pathFor(segmentName)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *OSByteStore) AvailableBytes() (int64, error) {
	// v1: not used for correctness; return sentinel.
	return 0, nil
}

func (s *OSByteStore) ReadSnapshot(key string) ([]byte, error) {
	p := filepath.Join(s.root, "snapshots", key)
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return b, nil
}

func (s *OSByteStore) WriteSnapshot(key string, data []byte) error {
	p := filepath.Join(s.root, "snapshots", key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (s *OSByteStore) DeleteSnapshot(key string) error {
	p := filepath.Join(s.root, "snapshots", key)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

