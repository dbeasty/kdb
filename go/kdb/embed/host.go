package embed

import (
	"fmt"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// Host owns everything a data root has exactly one of, and lends it to every namespace opened
// under it: the directory lock, and the platform I/O shim (and with it the object store and,
// when configured, the S3 replica tier and its client).
//
// This exists because the open path used to take all of that per *namespace*, which made "one
// process, several namespaces" impossible rather than merely expensive. OpenFileRuntimeWithOptions
// takes dataRoot's writer lock exclusively, and flock(2) is scoped to the open file description,
// so a second namespace under one root was refused even from inside the same process - see
// TestTwoNamespacesShareOneHost, which is the regression test for exactly that. A consumer that
// wanted nine namespaces therefore needed nine data roots, nine locks, nine I/O shims, nine S3
// clients, and nine memory budgets that could not see each other. See
// docs/kdb-spec-layer17-multi-namespace-runtime.md §1.
//
// A Host owns the runtimes it opens. Namespace is idempotent - asking twice for the same
// namespace returns the same runtime, because two independent engines over one namespace's files
// is precisely what the writer lock exists to prevent. Deliberately *not* reference-counted at
// this phase: nothing yet has two independent holders of one namespace, and a refcount whose
// decrement hangs off a shared pointer's Close cannot be balanced correctly (the second Close on
// the same pointer is a no-op by design, so the count would leak). When a second holder appears,
// give Namespace its own handle type and count on that, the way ServerRuntimeRegistry counts on
// *KdbServerRuntime.
type Host struct {
	dataRoot string
	opts     FileRuntimeOptions
	lock     *dirLock
	io       storage.PlatformIOShim

	mu     sync.Mutex
	nss    map[string]*EmbeddedKdbRuntime
	closes map[string]func() error
	closed bool
}

// OpenFileHost takes dataRoot's directory lock and builds the shared platform I/O shim, and
// opens no namespace at all - call Namespace for that. A writable host takes the exclusive
// writer lock; opts.ReadOnly takes only the shared attach lock, so several read-only hosts may
// attach alongside a single writer (see dir_lock_unix.go).
//
// The host-scoped half of opts is used here (S3, ReplicationPolicy, ReadOnly, and
// Storage.SyncMode, which configures the shared shim); the rest is the per-namespace default
// that Namespace applies, and that NamespaceWithOptions overrides.
//
// Because the shim is shared, SyncMode, the S3 replica tier and the replication policy are
// properties of the host rather than of each namespace. Nothing observable changes today: the
// only way to have set two of any of them under one root before was to open two runtimes over
// it, which the lock refused.
//
// It does bound one future consolidation, so it is worth naming. FileAuthRegistry already opens
// several namespaces under one root with the caller holding the lock (see openAuthNamespace) -
// the same shape as a Host, arrived at independently - but deliberately builds a *local-only*
// shim, because the auth registry's writes should not go to S3 while the data namespaces beside
// it do. Folding it onto a Host therefore needs the shim, not just the budget, to become
// per-namespace. Until something actually needs that, one shim per host is the simpler and
// cheaper arrangement, and it is what makes one S3 client serve nine namespaces instead of
// nine.
func OpenFileHost(dataRoot string, opts FileRuntimeOptions) (*Host, error) {
	acquire := acquireDirLock
	if opts.ReadOnly {
		acquire = acquireDirLockShared
	}
	if opts.alreadyLocked {
		// The caller holds the directory exclusively and is excluding
		// everyone else on this host's behalf; releasing is theirs too.
		acquire = func(string) (*dirLock, error) { return &dirLock{}, nil }
	}
	lock, err := acquire(dataRoot)
	if err != nil {
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
		FsyncOnFlush:  !opts.ReadOnly,
		SyncMode:      opts.Storage.SyncMode,
	})
	if err != nil {
		lock.Release()
		return nil, err
	}

	return &Host{
		dataRoot: dataRoot,
		opts:     opts,
		lock:     lock,
		io:       io,
		nss:      make(map[string]*EmbeddedKdbRuntime),
		closes:   make(map[string]func() error),
	}, nil
}

// DataRoot is the directory this host holds the lock on.
func (h *Host) DataRoot() string { return h.dataRoot }

// ReadOnly reports whether this host attached without the writer lock, which every namespace
// under it inherits.
func (h *Host) ReadOnly() bool { return h.opts.ReadOnly }

// Namespace opens namespaceID under this host with the host's default storage options, or
// returns the already-open runtime for it.
func (h *Host) Namespace(catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	return h.NamespaceWithOptions(catalog, namespaceID, sch, h.opts.Storage)
}

// NamespaceWithOptions is Namespace with per-namespace storage tuning - a hot namespace and a
// nearly-empty one under the same root rarely want the same budgets or the same durability.
//
// The options are only consulted when this namespace is actually opened: on a repeat call for an
// already-open namespace the existing runtime is returned as-is, because re-tuning a live engine
// is not something this can do behind its callers' backs.
func (h *Host) NamespaceWithOptions(catalog, namespaceID string, sch schema.KdbSchema, sopts StorageOptions) (*EmbeddedKdbRuntime, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, fmt.Errorf("kdb: host for %s is closed", h.dataRoot)
	}
	if rt, ok := h.nss[namespaceID]; ok {
		return rt, nil
	}
	opts := h.opts
	opts.Storage = sopts
	// The host settled these two; a per-namespace override of either would mean a namespace
	// disagreeing with the lock actually held over it.
	opts.ReadOnly = h.opts.ReadOnly
	opts.Storage.SyncMode = h.opts.Storage.SyncMode

	rt, storageClose, err := h.openNamespace(catalog, namespaceID, sch, opts)
	if err != nil {
		return nil, err
	}
	h.nss[namespaceID] = rt
	h.closes[namespaceID] = storageClose
	return rt, nil
}

// Namespaces lists the namespaces currently open under this host, sorted.
func (h *Host) Namespaces() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.nss))
	for id := range h.nss {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// CloseNamespace shuts one namespace down - flushing and sealing its delta segment and closing
// its engine handle - and leaves the host, its lock, and every other namespace alone. A later
// Namespace call for the same id opens it fresh. Unknown or already-closed ids are a no-op, so
// this is safe to call more than once.
func (h *Host) CloseNamespace(namespaceID string) error {
	h.mu.Lock()
	closeFn, ok := h.closes[namespaceID]
	delete(h.closes, namespaceID)
	delete(h.nss, namespaceID)
	h.mu.Unlock()
	if !ok || closeFn == nil {
		return nil
	}
	return closeFn()
}

// Close shuts every namespace down and then releases the directory lock, in that order - a
// namespace still flushing must not do so after another process has been told the directory is
// free. Returns the first error any namespace's shutdown reported, having still attempted all of
// them; per EmbeddedKdbRuntime.storageClose none of it is load-bearing for correctness.
//
// Safe to call more than once.
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	ids := make([]string, 0, len(h.closes))
	for id := range h.closes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	closes := h.closes
	h.closes = make(map[string]func() error)
	h.nss = make(map[string]*EmbeddedKdbRuntime)
	lock := h.lock
	h.lock = nil
	h.mu.Unlock()

	var firstErr error
	for _, id := range ids {
		if fn := closes[id]; fn != nil {
			if err := fn(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if lock != nil {
		lock.Release()
	}
	return firstErr
}
