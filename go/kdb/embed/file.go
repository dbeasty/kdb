package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// OpenFileRuntime opens an embedded runtime backed by a data directory.
// When KDB_S3_BUCKET is set, sealed segments and snapshots are replicated to S3 (see FileRuntimeOptionsFromEnv).
func OpenFileRuntime(dataRoot, catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	return OpenFileRuntimeWithOptions(dataRoot, catalog, namespaceID, sch, FileRuntimeOptionsFromEnv())
}

// OpenFileRuntimeWithOptions opens a file runtime with explicit storage options.
func OpenFileRuntimeWithOptions(dataRoot, catalog, namespaceID string, sch schema.KdbSchema, opts FileRuntimeOptions) (*EmbeddedKdbRuntime, error) {
	lock, err := acquireDirLock(dataRoot)
	if err != nil {
		return nil, err
	}
	if err := ensureNamespaceDirs(dataRoot, namespaceID); err != nil {
		lock.Release()
		return nil, err
	}

	s3Cfg := opts.S3
	if s3Cfg == nil {
		s3Cfg = s3io.ConfigFromEnv()
	}
	policy := opts.ReplicationPolicy

	io, err := (&storio.FileBackedPlatformIOFactory{
		NewStore: func(config storio.PlatformIOConfig) (storio.SegmentByteStore, error) {
			return buildSegmentByteStore(config, s3Cfg, policy)
		},
	}).Open(storio.PlatformIOConfig{
		RootDirectory: &dataRoot,
		FsyncOnFlush:  true,
		SyncMode:      opts.Storage.SyncMode,
	})
	if err != nil {
		lock.Release()
		return nil, err
	}

	compression := storage.CompressionZSTD
	if opts.Storage.Compression != nil {
		compression = *opts.Storage.Compression
	}
	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 64 * 1024 * 1024,
		CompressionCodec:        compression,
		DefaultIndexRetention:   storage.IndexRetentionEvictable,
		IOShim:                  io,
		Durability:              opts.Storage.Durability,
		AsyncSyncIntervalMillis: opts.Storage.AsyncSyncIntervalMillis,
	}
	handle, err := engine.DefaultFactory{EngineTarget: engine.TargetServer}.Open(namespaceID, cfg)
	if err != nil {
		lock.Release()
		return nil, err
	}
	// Every return between here and the success path below used to call lock.Release() alone,
	// leaking handle's open file descriptors and unsealed WAL on any post-open failure (schema
	// sync, delta replay, ...). handleClosed tracks whether the success path already took over
	// responsibility for handle - the deferred cleanup only fires on an early return.
	handleClosed := false
	defer func() {
		if !handleClosed {
			_ = handle.Close()
			lock.Release()
		}
	}()

	d, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		return nil, err
	}

	store := handle.Adapter()
	if store == nil {
		return nil, fmt.Errorf("file runtime missing storage adapter")
	}

	if err := replayDeltaNamespace(d, store, handle.DeltaReader()); err != nil {
		return nil, err
	}
	dagOut := dag.CommitDAG(d)
	var persisting *PersistingCommitDAG
	if w := handle.DeltaWriter(); w != nil {
		persisting = NewPersistingCommitDAGWithAsyncInterval(
			d, w, cfg.Durability,
			time.Duration(cfg.AsyncSyncIntervalMillis)*time.Millisecond,
		)
		dagOut = persisting
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
			return nil, err
		}
	}
	handleClosed = true
	rt.release = lock.Release
	// storageClose is what makes Close() an orderly shutdown rather than a no-op that only
	// releases the directory lock (kdb-spec-layer13 Component 47 §2.4/§4.5) - previously nothing
	// retained handle past this function returning, so even a clean process exit never flushed
	// the delta writer, sealed its segment, or reached ServerEngine.Close()'s final WAL sync.
	rt.storageClose = func() error {
		var firstErr error
		// Drain the commit log first: under DurabilityAsync there can be
		// records queued but not yet written, and sealing the segment out from
		// under them would drop acknowledged commits on a clean shutdown.
		if persisting != nil {
			if err := persisting.Close(); err != nil {
				firstErr = err
			}
		}
		if w := handle.DeltaWriter(); w != nil {
			if err := w.Flush(); err != nil && firstErr == nil {
				firstErr = err
			}
			if !w.IsSealed() {
				if _, err := w.Seal(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
		if err := handle.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return rt, nil
}

// LockDataDir takes dataRoot's exclusive directory lock - the same lock OpenFileRuntime holds -
// and returns its release func. For maintenance tooling (kdb-inspect verify/repair/restore/
// backup) that must not run concurrently with a live writer: holding this proves no service or
// embedded runtime has the directory open, and blocks one from starting mid-operation.
func LockDataDir(dataRoot string) (release func(), err error) {
	lock, err := acquireDirLock(dataRoot)
	if err != nil {
		return nil, err
	}
	return lock.Release, nil
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
