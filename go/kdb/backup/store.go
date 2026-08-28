package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// scanSegmentBytes decodes a verified segment prefix's commits, keyed by hex hash. The prefix
// came from integrity.VerifiedSegmentPrefix, so a scan error here is a real bug, not expected
// corruption.
func scanSegmentBytes(raw []byte) (map[string]document.Commit, error) {
	commits, err := delta.ScanSegmentBytes(raw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]document.Commit, len(commits))
	for _, c := range commits {
		out[c.CommitHash.Hex()] = c.Commit
	}
	return out, nil
}

// DirStore is a filesystem ObjectStore - local backups, tests, and network mounts. Keys map to
// file paths under Root.
type DirStore struct {
	Root string
}

func (d *DirStore) path(key string) (string, error) {
	if strings.Contains(key, "..") {
		return "", fmt.Errorf("dir store: key must not contain '..': %q", key)
	}
	return filepath.Join(d.Root, filepath.FromSlash(key)), nil
}

func (d *DirStore) Put(_ context.Context, key string, data []byte) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	// Write-then-rename so a torn write never leaves a half object under the real key - the
	// manifest-written-last invariant only helps if the objects it names are themselves whole.
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (d *DirStore) Get(_ context.Context, key string) ([]byte, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

func (d *DirStore) Delete(_ context.Context, key string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (d *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(d.Root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(d.Root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) && !strings.HasSuffix(key, ".tmp") {
			out = append(out, key)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return out, err
}
