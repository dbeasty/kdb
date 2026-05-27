package manager

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// Manager coordinates realized store handles across enlistments.
type Manager interface {
	Config() storage.StorageEngineConfig
	RealizedBytesInUse() int64
	RequestRealized(enlistmentID codec.UUID, commitHash codec.Hash, blockingPolicy storage.RebuildBlockingPolicy) (storage.RealizedStoreHandle, error)
}

var (
	installed     Manager
	installedOnce sync.Once
)

// Install sets the process-wide storage manager (once).
func Install(m Manager) {
	installedOnce.Do(func() {
		if installed != nil {
			panic("StorageManager already installed")
		}
		installed = m
	})
}

// Get returns the installed manager.
func Get() (Manager, error) {
	if installed == nil {
		return nil, &NotInstalledError{Message: "StorageManager not installed"}
	}
	return installed, nil
}

// NotInstalledError indicates no manager was installed.
type NotInstalledError struct{ Message string }

func (e *NotInstalledError) Error() string { return e.Message }

// DefaultManager is the default pool-based storage manager.
type DefaultManager struct {
	config storage.StorageEngineConfig
	target engine.Target
	pool   *RealizedStorePool
}

// NewDefaultManager returns a manager for the given engine target.
func NewDefaultManager(config storage.StorageEngineConfig, target engine.Target) *DefaultManager {
	return &DefaultManager{
		config: config,
		target: target,
		pool:   NewRealizedStorePool(config, target),
	}
}

func (m *DefaultManager) Config() storage.StorageEngineConfig { return m.config }

func (m *DefaultManager) RealizedBytesInUse() int64 { return m.pool.RealizedBytesInUse() }

func (m *DefaultManager) RequestRealized(
	enlistmentID codec.UUID,
	commitHash codec.Hash,
	blockingPolicy storage.RebuildBlockingPolicy,
) (storage.RealizedStoreHandle, error) {
	return m.pool.Acquire(enlistmentID, commitHash, blockingPolicy)
}

// RealizedStorePool pools handles keyed by enlistment and commit.
type RealizedStorePool struct {
	config storage.StorageEngineConfig
	target engine.Target

	mu            sync.Mutex
	handles       map[poolKey]*PooledRealizedHandle
	defaultEngine engine.Handle
	bytes         atomic.Int64
}

type poolKey struct {
	enlistment codec.UUID
	commit     codec.Hash
}

// NewRealizedStorePool returns an empty pool.
func NewRealizedStorePool(config storage.StorageEngineConfig, target engine.Target) *RealizedStorePool {
	return &RealizedStorePool{
		config:  config,
		target:  target,
		handles: make(map[poolKey]*PooledRealizedHandle),
	}
}

func (p *RealizedStorePool) RealizedBytesInUse() int64 { return p.bytes.Load() }

// Acquire returns or creates a pooled realized handle.
func (p *RealizedStorePool) Acquire(
	enlistmentID codec.UUID,
	commitHash codec.Hash,
	_ storage.RebuildBlockingPolicy,
) (storage.RealizedStoreHandle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := poolKey{enlistment: enlistmentID, commit: commitHash}
	h, ok := p.handles[key]
	if !ok {
		if p.defaultEngine == nil {
			eng, err := engine.DefaultFactory{EngineTarget: p.target}.Open("default", p.config)
			if err != nil {
				return nil, err
			}
			p.defaultEngine = eng
		}
		h = &PooledRealizedHandle{
			enlistmentID: enlistmentID,
			commitHash:   commitHash,
			storage:      p.defaultEngine.Adapter(),
			namespaceID:  "default",
			onRelease:    func() {},
		}
		p.handles[key] = h
	}
	h.refCount++
	return h, nil
}

// PooledRealizedHandle is a reference-counted realized store.
type PooledRealizedHandle struct {
	enlistmentID codec.UUID
	commitHash   codec.Hash
	namespaceID  string
	storage      storage.Adapter
	onRelease    func()
	refCount     int
}

func (h *PooledRealizedHandle) NamespaceID() string   { return h.namespaceID }
func (h *PooledRealizedHandle) CommitHash() codec.Hash  { return h.commitHash }
func (h *PooledRealizedHandle) EnlistmentID() codec.UUID { return h.enlistmentID }
func (h *PooledRealizedHandle) IsReady() bool           { return true }
func (h *PooledRealizedHandle) AwaitReady(storage.RebuildBlockingPolicy) error { return nil }
func (h *PooledRealizedHandle) Storage() storage.Adapter { return h.storage }

func (h *PooledRealizedHandle) Close() {
	h.refCount--
	if h.refCount <= 0 {
		h.onRelease()
	}
}

// String for debugging.
func (h *PooledRealizedHandle) String() string {
	return fmt.Sprintf("realized{%s@%s}", h.enlistmentID.String(), h.commitHash.Hex())
}
