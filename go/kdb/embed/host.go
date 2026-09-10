package embed

import (
	"fmt"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
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

	arbiter *storage.BudgetArbiter
	// adapter presents every open namespace under this host as one storage.Adapter, kept in step
	// with nss as namespaces open and close.
	adapter *engine.MultiplexAdapter

	mu     sync.Mutex
	nss    map[string]*namespaceEntry
	closed bool
}

// namespaceEntry is everything the host holds on behalf of one open namespace.
type namespaceEntry struct {
	rt *EmbeddedKdbRuntime
	// close flushes and seals this namespace's storage. Held here rather than on the runtime
	// because on the multi-namespace path the directory lock outlives any one namespace.
	close func() error
	// storage is what this namespace was opened with, kept so a reopen can start from the
	// options actually in force rather than from a set assembled elsewhere - which would
	// silently revert every setting the reopening caller did not happen to name. See
	// Host.StorageOptionsOf.
	storage StorageOptions
	// catalog and schema are the other two things reopening needs and nothing else records.
	catalog string
	schema  schema.KdbSchema
}

// DefaultHostMemoryBudgetBytes is the pool a host divides across its namespaces when the caller
// names no total. Exactly the per-runtime default the single-namespace open path has always
// used, so a host with one namespace under it is proportioned byte for byte the way that
// namespace would have been on its own.
//
// It does mean a host with nine namespaces divides the same 64 MiB nine ways rather than handing
// out 64 MiB nine times. That is the point - see docs/kdb-spec-layer17-multi-namespace-runtime.md
// §1.1 - but it also means a multi-namespace deployment should set a real total rather than
// inherit a default sized for one namespace.
const DefaultHostMemoryBudgetBytes int64 = 64 << 20

// namespaceBudget is one namespace's side of the host's memory pool: the storage engine holds
// document versions, historical trees and the memtable, and the commit DAG holds operations, so
// a share has to be split across both and demand summed from both.
type namespaceBudget struct {
	eng *engine.ServerEngine
	dag *dag.InMemoryCommitDag
	// pinnedOps records that the caller set StorageOptions.CommitOpsBytes explicitly, so the
	// share must not be re-derived over it. The engine makes the same check for its own three
	// sub-budgets from its config; the DAG has no config to consult, so the fact is carried
	// here instead. See ServerEngine.SetMemoryBudgetBytes.
	pinnedOps bool
}

func (n namespaceBudget) SetBudgetBytes(b int64) {
	n.eng.SetMemoryBudgetBytes(b)
	if n.pinnedOps {
		return
	}
	// The same fraction ResolvedCommitOpsBytes applies at open, so an arbitrated namespace and a
	// standalone one are proportioned identically and only the total differs.
	n.dag.SetOperationsBudget(int64(float64(b) * storage.DefaultCommitOpsFraction))
}

func (n namespaceBudget) DemandBytes() int64 {
	return n.eng.MemoryDemandBytes() + n.dag.OperationsResidentBytes()
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
	// The archive is a separate destination from the replica, and separately configured: they
	// are different jobs (see buildSegmentByteStore), and a deployment that wants a bounded
	// local footprint with the commits still recoverable needs the archive specifically.
	archiveCfg := opts.S3Archive
	if archiveCfg == nil {
		archiveCfg = s3io.ArchiveConfigFromEnv()
	}
	policy := opts.ReplicationPolicy

	io, err := (&storio.FileBackedPlatformIOFactory{
		NewStore: func(config storio.PlatformIOConfig) (storio.SegmentByteStore, error) {
			return buildSegmentByteStore(config, s3Cfg, archiveCfg, opts.ArchiveBlobs, policy)
		},
	}).Open(storio.PlatformIOConfig{
		RootDirectory:    &dataRoot,
		FsyncOnFlush:     !opts.ReadOnly,
		SyncMode:         opts.Storage.SyncMode,
		PreallocateBytes: opts.Storage.PreallocateBytes,
	})
	if err != nil {
		lock.Release()
		return nil, err
	}

	pool := opts.Storage.MemoryBudgetBytes
	if pool <= 0 {
		pool = DefaultHostMemoryBudgetBytes
	}
	return &Host{
		dataRoot: dataRoot,
		opts:     opts,
		lock:     lock,
		io:       io,
		arbiter:  storage.NewBudgetArbiter(pool, storage.DefaultBudgetRebalanceInterval),
		adapter:  engine.NewMultiplexAdapter(),
		nss:      make(map[string]*namespaceEntry),
	}, nil
}

// Adapter presents every namespace open under this host as a single storage.Adapter, dispatching
// on the namespaceID each of its methods already carries.
//
// For a caller that spans namespaces - a query over several spaces, or a transaction touching
// more than one. A caller working within one namespace should use that runtime's own Storage,
// which is the same engine without the indirection. The returned adapter tracks the host: a
// namespace opened later becomes routable through it, and one closed stops being.
func (h *Host) Adapter() storage.Adapter { return h.adapter }

// MemoryArbiter is the pool this host divides across its namespaces. Exposed so a caller can
// change the floor, pin one namespace's share with Reserve, or read the current allocation for
// metrics.
func (h *Host) MemoryArbiter() *storage.BudgetArbiter { return h.arbiter }

// DataRoot is the directory this host holds the lock on.
func (h *Host) DataRoot() string { return h.dataRoot }

// ReadOnly reports whether this host attached without the writer lock, which every namespace
// under it inherits.
func (h *Host) ReadOnly() bool { return h.opts.ReadOnly }

// Namespace opens namespaceID under this host with the host's default storage options, or
// returns the already-open runtime for it.
func (h *Host) Namespace(catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error) {
	sopts := h.opts.Storage
	// StorageOptions.MemoryBudgetBytes means two different things at the two levels, and passing
	// the host's through unchanged conflates them: on the host it is the *pool* every namespace
	// draws from, while on a namespace it is that namespace's fixed, non-arbitrated share. Left
	// in, every namespace opened here would be pinned at the size of the whole pool - so a host
	// given an explicit 256 MiB total handed 256 MiB to each of nine namespaces, which is the
	// exact arithmetic this component exists to remove. Zeroed here so the default is what the
	// caller meant: draw from the pool.
	sopts.MemoryBudgetBytes = 0
	return h.NamespaceWithOptions(catalog, namespaceID, sch, sopts)
}

// NamespaceWithOptions is Namespace with per-namespace storage tuning - a hot namespace and a
// nearly-empty one under the same root rarely want the same budgets or the same durability.
//
// The options are only consulted when this namespace is actually opened: on a repeat call for an
// already-open namespace the existing runtime is returned as-is, because re-tuning a live engine
// is not something this can do behind its callers' backs.
func (h *Host) NamespaceWithOptions(catalog, namespaceID string, sch schema.KdbSchema, sopts StorageOptions) (*EmbeddedKdbRuntime, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, fmt.Errorf("kdb: host for %s is closed", h.dataRoot)
	}
	if e, ok := h.nss[namespaceID]; ok {
		h.mu.Unlock()
		return e.rt, nil
	}
	opts := h.opts
	opts.Storage = sopts
	// The host settled these two; a per-namespace override of either would mean a namespace
	// disagreeing with the lock actually held over it.
	opts.ReadOnly = h.opts.ReadOnly
	opts.Storage.SyncMode = h.opts.Storage.SyncMode
	// Open at roughly the share this namespace is about to be given, not at the whole pool.
	// Replay happens inside openNamespace, so a namespace that replays against the pool and is
	// only cut down to its share afterwards has already held the difference - which is precisely
	// the transient spike the pool exists to prevent. The arbiter refines this a moment later.
	fixed := sopts.MemoryBudgetBytes > 0
	if !fixed {
		opts.Storage.MemoryBudgetBytes = h.arbiter.TotalBytes() / int64(len(h.nss)+1)
	}
	h.mu.Unlock()

	entry, budget, err := h.openNamespace(catalog, namespaceID, sch, opts)
	if err != nil {
		return nil, err
	}
	// Recorded so a later reopen can start from what this namespace is
	// actually running on. See Host.StorageOptionsOf.
	entry.storage, entry.catalog, entry.schema = opts.Storage, catalog, sch

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = entry.close()
		return nil, fmt.Errorf("kdb: host for %s is closed", h.dataRoot)
	}
	// Lost a race to another goroutine opening the same namespace: keep theirs, close ours.
	// Two engines over one namespace's files is what the writer lock exists to prevent, and the
	// lock cannot see this one because both are inside the process that holds it.
	if e, ok := h.nss[namespaceID]; ok {
		h.mu.Unlock()
		_ = entry.close()
		return e.rt, nil
	}
	h.nss[namespaceID] = entry
	h.mu.Unlock()

	if entry.rt.Storage != nil {
		h.adapter.Register(namespaceID, entry.rt.Storage)
	}

	// Outside the host lock: registering rebalances, which evicts inside the engines' own locks,
	// and none of that should block another namespace from being opened or closed.
	if budget != nil {
		if fixed {
			// An explicit per-namespace budget opts out of the pool: carved off the top and left
			// alone, so a caller who has decided what one namespace needs keeps that decision.
			h.arbiter.Reserve(namespaceID, sopts.MemoryBudgetBytes)
		}
		h.arbiter.Register(namespaceID, budget)
	}
	return entry.rt, nil
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
	entry, ok := h.nss[namespaceID]
	delete(h.nss, namespaceID)
	h.mu.Unlock()
	// Unrouted before anything is torn down: a call naming this namespace must fail with
	// ErrUnknownNamespace rather than reach an engine that is being closed underneath it.
	h.adapter.Unregister(namespaceID)
	if !ok || entry == nil || entry.close == nil {
		return nil
	}
	// Before the storage close, so the namespaces that are staying get the room back at the
	// moment it stops being used rather than at the next tick.
	h.arbiter.Unregister(namespaceID)
	return entry.close()
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
	ids := make([]string, 0, len(h.nss))
	for id := range h.nss {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	entries := h.nss
	h.nss = make(map[string]*namespaceEntry)
	for _, id := range ids {
		h.adapter.Unregister(id)
	}
	lock := h.lock
	h.lock = nil
	h.mu.Unlock()

	// Stop rebalancing before anything is torn down, so no tick lands on a namespace that is
	// mid-close. Namespaces keep whatever ceiling they last had; see BudgetArbiter.Close.
	h.arbiter.Close()

	var firstErr error
	for _, id := range ids {
		if e := entries[id]; e != nil && e.close != nil {
			if err := e.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if lock != nil {
		lock.Release()
	}
	return firstErr
}
