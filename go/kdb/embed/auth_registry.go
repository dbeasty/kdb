package embed

import (
	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// FileAuthRegistry is a durable RBAC registry (kdb-finish-up-plan Phase 2.7): users and roles
// persisted through the same delta-log machinery as any other namespace, under the reserved
// auth.UsersNamespace/auth.RolesNamespace directories of a data root. Close flushes and seals
// both namespaces' segments.
type FileAuthRegistry struct {
	Store   *auth.RegistryAuthStore
	closers []func() error
	lock    *dirLock
}

// Close seals both registry namespaces (and releases the directory lock, for registries opened
// via OpenFileAuthRegistryExclusive). Safe against partial construction.
func (r *FileAuthRegistry) Close() error {
	var first error
	for _, c := range r.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	if r.lock != nil {
		r.lock.Release()
	}
	return first
}

// OpenFileAuthRegistry opens (creating on first use) the durable RBAC registry under dataRoot.
// The caller must already hold dataRoot's directory lock - in practice, kdb-service after
// OpenFileRuntime. Standalone tooling (the user-bootstrap CLI) should use
// OpenFileAuthRegistryExclusive instead, which takes the lock itself.
func OpenFileAuthRegistry(dataRoot string) (*FileAuthRegistry, error) {
	return openFileAuthRegistry(dataRoot, nil)
}

// OpenFileAuthRegistryExclusive is OpenFileAuthRegistry for standalone use: it acquires
// dataRoot's directory lock first (so it cannot race a running service - stop the service
// before bootstrapping users) and releases it on Close.
func OpenFileAuthRegistryExclusive(dataRoot string) (*FileAuthRegistry, error) {
	lock, err := acquireDirLock(dataRoot)
	if err != nil {
		return nil, err
	}
	reg, err := openFileAuthRegistry(dataRoot, lock)
	if err != nil {
		lock.Release()
		return nil, err
	}
	return reg, nil
}

func openFileAuthRegistry(dataRoot string, lock *dirLock) (*FileAuthRegistry, error) {
	reg := &FileAuthRegistry{lock: lock}
	usersDag, usersAdapter, err := openAuthNamespace(reg, dataRoot, auth.UsersNamespace)
	if err != nil {
		_ = reg.closeWithoutLock()
		return nil, err
	}
	rolesDag, rolesAdapter, err := openAuthNamespace(reg, dataRoot, auth.RolesNamespace)
	if err != nil {
		_ = reg.closeWithoutLock()
		return nil, err
	}
	// RegistryAuthStore takes one Adapter, but ServerEngine is single-namespace, so the
	// registry's two namespaces need two engines behind it. That is exactly what
	// engine.MultiplexAdapter is for - it used to be a private copy of it right here, which
	// panicked on an unknown namespace and misrouted delta segments; see its doc comment.
	routes := engine.NewMultiplexAdapter()
	routes.Register(auth.UsersNamespace, usersAdapter)
	routes.Register(auth.RolesNamespace, rolesAdapter)
	store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, routes)
	if err != nil {
		_ = reg.closeWithoutLock()
		return nil, err
	}
	reg.Store = store
	return reg, nil
}

// closeWithoutLock runs the namespace closers but leaves the dir lock alone - construction
// error paths release (or not) the lock themselves depending on which entry point took it.
func (r *FileAuthRegistry) closeWithoutLock() error {
	var first error
	for _, c := range r.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	r.closers = nil
	return first
}

// openAuthNamespace opens one registry namespace's engine + persisting DAG, replaying its delta
// log - the same sequence OpenFileRuntimeWithOptions runs for a data namespace, minus the dir
// lock (handled by the caller) and schema sync (registry documents are schemaless).
func openAuthNamespace(reg *FileAuthRegistry, dataRoot, namespaceID string) (dag.CommitDAG, storage.Adapter, error) {
	if err := ensureNamespaceDirs(dataRoot, namespaceID); err != nil {
		return nil, nil, err
	}
	io, err := (&storio.FileBackedPlatformIOFactory{
		// Local disk only - no S3 replication for the auth registry (its writes are rare and
		// tiny; back it up with the data dir as a whole).
		NewStore: func(config storio.PlatformIOConfig) (storio.SegmentByteStore, error) {
			return buildSegmentByteStore(config, nil, storio.ReplicationPolicy{})
		},
	}).Open(storio.PlatformIOConfig{RootDirectory: &dataRoot, FsyncOnFlush: true})
	if err != nil {
		return nil, nil, err
	}
	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 16 * 1024 * 1024,
		CompressionCodec:        storage.CompressionZSTD,
		DefaultIndexRetention:   storage.IndexRetentionEvictable,
		IOShim:                  io,
	}
	handle, err := engine.DefaultFactory{EngineTarget: engine.TargetServer}.Open(namespaceID, cfg)
	if err != nil {
		return nil, nil, err
	}
	reg.closers = append(reg.closers, func() error {
		var first error
		if w := handle.DeltaWriter(); w != nil {
			if err := w.Flush(); err != nil && first == nil {
				first = err
			}
			if !w.IsSealed() {
				if _, err := w.Seal(); err != nil && first == nil {
					first = err
				}
			}
		}
		if err := handle.Close(); err != nil && first == nil {
			first = err
		}
		return first
	})

	d, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		return nil, nil, err
	}
	adapter := handle.Adapter()
	if err := replayDeltaNamespace(d, adapter, handle.DeltaReader()); err != nil {
		return nil, nil, err
	}
	var dagOut dag.CommitDAG = d
	if w := handle.DeltaWriter(); w != nil {
		dagOut = NewPersistingCommitDAG(d, w)
	}
	return dagOut, adapter, nil
}
