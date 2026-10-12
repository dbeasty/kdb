package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/fsx"
)

// Files written under `<indexDir>/<indexId>/` by SaveStoreSnapshot.
const (
	ManifestFileName = "manifest.json"
	SnapshotFileName = "snapshot.bin"
	// ManifestFormatVersion is bumped when the manifest shape changes incompatibly.
	ManifestFormatVersion = 1
)

// ErrNoSnapshot is returned by LoadStoreSnapshot when the directory holds no snapshot.
var ErrNoSnapshot = errors.New("index: no snapshot")

// Manifest describes a persisted store snapshot (Layer 16 §6.5): which index it belongs to,
// the DAG head it was taken at (a differing head on open means the snapshot is stale and the
// index is rebuilt by scan), and whatever summary statistics the store exposes (N and per-field
// avglen for full-text).
type Manifest struct {
	FormatVersion int                `json:"formatVersion"`
	Index         DescriptorJSON     `json:"index"`
	HeadCommitHex string             `json:"headCommitHex"`
	Stats         map[string]float64 `json:"stats,omitempty"`
}

// StatsProvider is implemented by stores that publish summary statistics into the manifest.
type StatsProvider interface {
	SnapshotStats() map[string]float64
}

// WriteFileAtomic writes data to a temp file in path's directory and renames it over path,
// so a crash mid-write leaves the previous file intact.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = fsx.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := fsx.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// IndexDir is the per-index directory under a namespace's index directory.
func IndexDir(baseDir string, indexID codec.UUID) string {
	return filepath.Join(baseDir, indexID.String())
}

// SaveStoreSnapshot writes the store's snapshot and manifest into dir (created if needed).
// The snapshot is written first, then the manifest, each atomically: a manifest is only ever
// visible next to a complete snapshot.
func SaveStoreSnapshot(dir string, store Store, head codec.Hash) error {
	data, err := store.Snapshot()
	if err != nil {
		return err
	}
	return writeStoreSnapshot(dir, store.Descriptor(), data, storeStats(store), head)
}

// storeStats is the store's manifest statistics, if it publishes any.
func storeStats(store Store) map[string]float64 {
	if sp, ok := store.(StatsProvider); ok {
		return sp.SnapshotStats()
	}
	return nil
}

// writeStoreSnapshot writes already-serialized snapshot bytes and then the manifest naming head.
// The directory is synced between the two renames, so after a crash the manifest can never be
// the newer of the pair: a manifest that names head always sits next to the snapshot taken at
// head (or a later manifest does). data == nil writes the manifest alone, for a store whose
// snapshot on disk is already its current content - the caller vouches for that.
func writeStoreSnapshot(dir string, desc Descriptor, data []byte, stats map[string]float64, head codec.Hash) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if data != nil {
		if err := WriteFileAtomic(filepath.Join(dir, SnapshotFileName), data); err != nil {
			return err
		}
		if err := fsx.SyncDir(dir); err != nil {
			return err
		}
	}
	m := Manifest{
		FormatVersion: ManifestFormatVersion,
		Index:         DescriptorToJSON(desc),
		HeadCommitHex: head.Hex(),
		Stats:         stats,
	}
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(filepath.Join(dir, ManifestFileName), mb)
}

// InvalidateStoreSnapshot removes dir's manifest, durably, so whatever snapshot is left there
// can no longer be taken for current: a store without a manifest is rebuilt on open. Used when
// an index is (re)created, where an earlier index of the same id may have left a manifest naming
// the very head the new one is created at. A missing manifest is not an error.
func InvalidateStoreSnapshot(dir string) error {
	if err := fsx.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return fsx.SyncDir(dir)
}

// ReadManifest reads dir's manifest without touching the snapshot. Missing → ErrNoSnapshot.
func ReadManifest(dir string) (Manifest, error) {
	mb, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Manifest{}, ErrNoSnapshot
		}
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(mb, &m); err != nil {
		return Manifest{}, fmt.Errorf("index manifest: %w", err)
	}
	if m.FormatVersion != ManifestFormatVersion {
		return Manifest{}, fmt.Errorf("index manifest: unsupported format version %d", m.FormatVersion)
	}
	return m, nil
}

// LoadStoreSnapshot restores the snapshot in dir into store and returns its manifest. The
// manifest must name the store's index; a mismatch is an error rather than a silent restore
// of somebody else's postings.
func LoadStoreSnapshot(dir string, store Store) (Manifest, error) {
	m, err := ReadManifest(dir)
	if err != nil {
		return Manifest{}, err
	}
	want := store.Descriptor()
	if m.Index.IndexID != want.IndexID.String() || m.Index.Type != want.Type.String() {
		return Manifest{}, fmt.Errorf("index manifest: snapshot belongs to index %s (%s), store is %s (%s)",
			m.Index.IndexID, m.Index.Type, want.IndexID, want.Type)
	}
	data, err := os.ReadFile(filepath.Join(dir, SnapshotFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Manifest{}, ErrNoSnapshot
		}
		return Manifest{}, err
	}
	if err := store.RestoreSnapshot(data); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
