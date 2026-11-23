package embed

import (
	"runtime"
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
)

// The memory budget an embedded store gets when StorageOptions.MemoryBudgetBytes is zero: a
// sixteenth of the memory the process may use (the cgroup limit, else physical RAM), at least
// 64 MiB and at most 256 MiB, and one pool for the whole process rather than one per store.
//
// This is Cassandra's shape for a cache nobody sized: a fraction of what the process has, with a
// fixed cap. Its key cache defaults to min(5% of the heap, 100 MB) and its chunk cache to
// min(1/4 of the heap, 512 MB), so a small node is not starved and a large one does not hand a
// cache memory it never asked for. kdb-service owns its container and takes an eighth of it
// (server.CachePoolBytes); an embedded store shares its process with an application that owns
// the memory, so it takes half that fraction and stops at 256 MiB. The floor is the fixed 64 MiB
// every embedded store had before, so no host gets less than it used to.
//
// Measured (docs/kdb-basic-rw-performance-plan.md, E2): with 100k 1 KB documents in a 4 CPU /
// 3 GB container, the old fixed 64 MiB served point reads 3.3-4.8x slower than a cache sized to
// hold the hot set.
const (
	DefaultMemoryBudgetFraction       = 1.0 / 16
	MinDefaultMemoryBudgetBytes int64 = 64 << 20
	MaxDefaultMemoryBudgetBytes int64 = 256 << 20
)

// DefaultHostMemoryBudgetBytes is the fixed pool an embedded store took before the default was
// sized from the machine, and is now that default's floor.
//
// Deprecated: the default is DefaultMemoryBudgetBytes, shared across the process.
const DefaultHostMemoryBudgetBytes = MinDefaultMemoryBudgetBytes

// DefaultMemoryBudgetBytes is the pool every default-sized store in this process shares: those
// opened through OpenFileRuntime, OpenFileRuntimeWithOptions, OpenFileHost or the database/sql
// driver with StorageOptions.MemoryBudgetBytes left at zero. It is read when the first such store
// opens and holds until the last one closes.
//
// On Android and iOS it stays at the floor: a phone app's memory is the operating system's to
// reclaim, and a library that sized itself from the device would be the first thing killed.
func DefaultMemoryBudgetBytes() int64 {
	return defaultMemoryBudgetFor(runtime.GOOS, machineMemoryBytes())
}

func defaultMemoryBudgetFor(goos string, machine int64) int64 {
	if goos == "android" || goos == "ios" || machine <= 0 {
		return MinDefaultMemoryBudgetBytes
	}
	b := int64(float64(machine) * DefaultMemoryBudgetFraction)
	switch {
	case b < MinDefaultMemoryBudgetBytes:
		return MinDefaultMemoryBudgetBytes
	case b > MaxDefaultMemoryBudgetBytes:
		return MaxDefaultMemoryBudgetBytes
	}
	return b
}

// machineMemoryBytes is the memory this process may use: its cgroup limit, else the host's
// physical RAM, else 0. A variable so tests can size a pretend machine.
var machineMemoryBytes = func() int64 {
	if v, ok := storage.CgroupMemoryLimitBytes(); ok {
		return int64(v)
	}
	if total, err := storage.TotalSystemMemoryBytes(); err == nil && total > 0 {
		return total
	}
	return 0
}

// sharedPool is the memory pool every default-sized host in the process divides, so N embedded
// stores share one DefaultMemoryBudgetBytes instead of each taking it. Before, each store took a
// fixed 64 MiB and two stores meant 128 MiB of ceilings that could not see each other; with a
// default sized from the machine, that multiplication would be four times worse.
//
// Reference-counted by the hosts holding it. The arbiter is built when the first one opens and
// closed when the last one closes, so no rebalancing goroutine outlives the stores it serves, and
// a process that closes everything and opens again re-reads the machine's size.
var sharedPool processPool

type processPool struct {
	mu      sync.Mutex
	arbiter *storage.BudgetArbiter
	users   int
}

func (p *processPool) acquire() *storage.BudgetArbiter {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.arbiter == nil {
		p.arbiter = storage.NewBudgetArbiter(DefaultMemoryBudgetBytes(), sharedPoolRebalanceInterval)
	}
	p.users++
	return p.arbiter
}

// release drops one host's hold on a, closing it if that was the last. A host whose arbiter is
// not the current shared one (it cannot be, but a double release must not close a newer pool)
// is ignored.
func (p *processPool) release(a *storage.BudgetArbiter) {
	p.mu.Lock()
	if a == nil || a != p.arbiter || p.users == 0 {
		p.mu.Unlock()
		return
	}
	p.users--
	var last *storage.BudgetArbiter
	if p.users == 0 {
		last, p.arbiter = p.arbiter, nil
	}
	p.mu.Unlock()
	if last != nil {
		last.Close()
	}
}

// sharedPoolRebalanceInterval is how often the shared pool re-cuts shares; a variable so tests
// can drive rebalancing by hand.
var sharedPoolRebalanceInterval = storage.DefaultBudgetRebalanceInterval
