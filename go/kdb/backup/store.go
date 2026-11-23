package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/limidus/kdb/go/kdb/fsx"
)

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
	// Synced before the rename and the directory after it: unsynced, a power cut after a backup
	// finished could keep the manifest's rename and lose the bytes of the objects it names.
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	if err := errors.Join(werr, serr, f.Close()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := fsx.Rename(tmp, p); err != nil {
		return err
	}
	return fsx.SyncDir(filepath.Dir(p))
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
	return fsx.Remove(p)
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
