package embed

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// OpenFileRuntime opens an embedded runtime backed by a data directory.
func OpenFileRuntime(dataRoot, catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	lock, err := acquireDirLock(dataRoot)
	if err != nil {
		return nil, err
	}
	if err := ensureNamespaceDirs(dataRoot, namespaceID); err != nil {
		lock.Release()
		return nil, err
	}

	io, err := (&storio.FileBackedPlatformIOFactory{
		NewStore: func(config storio.PlatformIOConfig) (storio.SegmentByteStore, error) {
			return storio.NewOSByteStore(config)
		},
	}).Open(storio.PlatformIOConfig{RootDirectory: &dataRoot, FsyncOnFlush: true})
	if err != nil {
		lock.Release()
		return nil, err
	}

	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 64 * 1024 * 1024,
		CompressionCodec:        storage.CompressionZSTD,
		DefaultIndexRetention:   storage.IndexRetentionEvictable,
		IOShim:                  io,
	}
	handle, err := engine.DefaultFactory{EngineTarget: engine.TargetServer}.Open(namespaceID, cfg)
	if err != nil {
		lock.Release()
		return nil, err
	}

	d, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		lock.Release()
		return nil, err
	}

	store := handle.Adapter()
	if store == nil {
		lock.Release()
		return nil, fmt.Errorf("file runtime missing storage adapter")
	}

	if err := replayDeltaNamespace(d, store, handle.DeltaReader()); err != nil {
		lock.Release()
		return nil, err
	}
	dagOut := dag.CommitDAG(d)
	if w := handle.DeltaWriter(); w != nil {
		dagOut = NewPersistingCommitDAG(d, w)
	}

	rt := &EmbeddedKdbRuntime{
		Catalog:          catalog,
		DAG:              dagOut,
		Storage:          store,
		Schema:           sch,
		DefaultNamespace: namespaceID,
		DataRoot:         dataRoot,
	}
	if !sch.IsNone() {
		if err := syncEmbedSchema(rt, namespaceID, sch); err != nil {
			lock.Release()
			return nil, err
		}
	}
	rt.release = lock.Release
	return rt, nil
}

func ensureNamespaceDirs(dataRoot, namespaceID string) error {
	nsDir := filepath.Join(dataRoot, "ns", namespaceID)
	for _, sub := range []string{"", "delta", "meta"} {
		p := nsDir
		if sub != "" {
			p = filepath.Join(nsDir, sub)
		}
		if err := os.MkdirAll(p, 0o755); err != nil {
			return err
		}
	}
	meta := filepath.Join(nsDir, "meta.json")
	if _, err := os.Stat(meta); os.IsNotExist(err) {
		if err := os.WriteFile(meta, []byte(`{"namespaceId":"`+namespaceID+`"}`), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// CatalogFromNamespace returns the catalog segment of a namespace id (before '/').
func CatalogFromNamespace(namespaceID string) string {
	if i := indexByte(namespaceID, '/'); i >= 0 {
		return namespaceID[:i]
	}
	return namespaceID
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
