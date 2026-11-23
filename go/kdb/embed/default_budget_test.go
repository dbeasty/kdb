package embed

import (
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func TestDefaultMemoryBudgetIsASixteenthBetween64And256MiB(t *testing.T) {
	const mib, gib = int64(1 << 20), int64(1 << 30)
	cases := []struct {
		name    string
		goos    string
		machine int64
		want    int64
	}{
		{"unknown machine size", "linux", 0, 64 * mib},
		{"tiny host keeps the old 64 MiB", "linux", 512 * mib, 64 * mib},
		{"1 GiB host is at the floor", "linux", 1 * gib, 64 * mib},
		{"3 GiB container", "linux", 3 * gib, 192 * mib},
		{"4 GiB laptop", "darwin", 4 * gib, 256 * mib},
		{"16 GiB host hits the cap", "linux", 16 * gib, 256 * mib},
		{"64 GiB host stays at the cap", "windows", 64 * gib, 256 * mib},
		{"Android stays at the floor", "android", 8 * gib, 64 * mib},
		{"iOS stays at the floor", "ios", 8 * gib, 64 * mib},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultMemoryBudgetFor(c.goos, c.machine); got != c.want {
				t.Fatalf("defaultMemoryBudgetFor(%s, %d) = %d MiB, want %d MiB", c.goos, c.machine, got>>20, c.want>>20)
			}
		})
	}
}

// pretendMachine sizes the machine the default pool is cut from, and fails the test if a store
// from an earlier test is still holding the pool - its share would make every number here wrong.
func pretendMachine(t *testing.T, bytes int64) {
	t.Helper()
	sharedPool.mu.Lock()
	users := sharedPool.users
	sharedPool.mu.Unlock()
	if users != 0 {
		t.Fatalf("%d default-sized stores are still open from an earlier test", users)
	}
	was := machineMemoryBytes
	machineMemoryBytes = func() int64 { return bytes }
	t.Cleanup(func() { machineMemoryBytes = was })
}

func openDefault(t *testing.T, ns string) *EmbeddedKdbRuntime {
	t.Helper()
	rt, err := OpenFileRuntimeWithOptions(t.TempDir(), "app", ns, schema.None(), FileRuntimeOptions{})
	if err != nil {
		t.Fatalf("open %s: %v", ns, err)
	}
	return rt
}

func currentSharedPool() (*storage.BudgetArbiter, int) {
	sharedPool.mu.Lock()
	defer sharedPool.mu.Unlock()
	return sharedPool.arbiter, sharedPool.users
}

func sumShares(a *storage.BudgetArbiter) int64 {
	var sum int64
	for _, s := range a.Shares() {
		sum += s
	}
	return sum
}

// Two default-sized stores in one process divide one pool. Before, each took its own fixed
// 64 MiB; with a default sized from the machine, each taking it would multiply a 256 MiB ceiling.
func TestDefaultStoresInOneProcessShareOnePool(t *testing.T) {
	pretendMachine(t, 3<<30)
	a := openDefault(t, "app/a")
	defer a.Close()
	pool, users := currentSharedPool()
	if users != 1 || pool == nil {
		t.Fatalf("after one open: %d users, pool %v", users, pool)
	}
	if got := pool.TotalBytes(); got != 192<<20 {
		t.Fatalf("pool is %d MiB, want 192 MiB (a sixteenth of 3 GiB)", got>>20)
	}
	if got := sumShares(pool); got != pool.TotalBytes() {
		t.Fatalf("a lone store holds %d of the %d pool, want all of it", got, pool.TotalBytes())
	}

	b := openDefault(t, "app/b")
	defer b.Close()
	again, users := currentSharedPool()
	if again != pool || users != 2 {
		t.Fatalf("second store got its own pool (%v vs %v, %d users)", again, pool, users)
	}
	shares := pool.Shares()
	if len(shares) != 2 {
		t.Fatalf("%d participants, want 2: %v", len(shares), shares)
	}
	if sum := sumShares(pool); sum > pool.TotalBytes() {
		t.Fatalf("shares sum to %d, over the %d pool", sum, pool.TotalBytes())
	}
	for key, s := range shares {
		if s >= pool.TotalBytes() || s < storage.DefaultNamespaceFloorBytes {
			t.Fatalf("%q got %d of a %d pool", key, s, pool.TotalBytes())
		}
	}

	// Closing one store hands its room to the other.
	b.Close()
	if _, users := currentSharedPool(); users != 1 {
		t.Fatalf("%d users after one close, want 1", users)
	}
	if got := sumShares(pool); got != pool.TotalBytes() || len(pool.Shares()) != 1 {
		t.Fatalf("survivor holds %d of the %d pool (%v), want all of it", got, pool.TotalBytes(), pool.Shares())
	}
}

// A Host the caller opens and a single-namespace runtime draw from the same pool, and a host's
// namespaces are told apart from another store's namespace of the same id.
func TestHostsAndRuntimesShareThePool(t *testing.T) {
	pretendMachine(t, 16<<30)
	rt := openDefault(t, "app/same")
	defer rt.Close()
	host, err := OpenFileHost(t.TempDir(), FileRuntimeOptions{})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()
	for _, ns := range []string{"app/same", "app/other"} {
		if _, err := host.Namespace("app", ns, schema.None()); err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
	}
	pool, users := currentSharedPool()
	if host.MemoryArbiter() != pool || users != 2 {
		t.Fatalf("host is not on the process pool (%d users)", users)
	}
	if got := pool.TotalBytes(); got != MaxDefaultMemoryBudgetBytes {
		t.Fatalf("pool is %d MiB, want the 256 MiB cap", got>>20)
	}
	if n := pool.Participants(); n != 3 {
		t.Fatalf("%d participants, want 3 (one runtime + two host namespaces): %v", n, pool.Shares())
	}
	if sum := sumShares(pool); sum > pool.TotalBytes() {
		t.Fatalf("shares sum to %d, over the %d pool", sum, pool.TotalBytes())
	}
	mine := host.MemoryShares()
	if len(mine) != 2 || mine["app/same"] == 0 || mine["app/other"] == 0 {
		t.Fatalf("host's own shares = %v, want both of its namespaces", mine)
	}
}

// An explicit budget is the caller's alone: it neither draws from nor joins the process pool.
func TestAnExplicitBudgetStaysOutOfTheProcessPool(t *testing.T) {
	pretendMachine(t, 3<<30)
	shared := openDefault(t, "app/shared")
	defer shared.Close()
	pool, _ := currentSharedPool()

	opts := FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 32 << 20
	own, err := OpenFileRuntimeWithOptions(t.TempDir(), "app", "app/own", schema.None(), opts)
	if err != nil {
		t.Fatalf("open with an explicit budget: %v", err)
	}
	defer own.Close()
	host, err := OpenFileHost(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("open host with an explicit pool: %v", err)
	}
	defer host.Close()
	if _, err := host.Namespace("app", "app/hosted", schema.None()); err != nil {
		t.Fatalf("open hosted namespace: %v", err)
	}

	if _, users := currentSharedPool(); users != 1 {
		t.Fatalf("%d users of the process pool, want only the default-sized store", users)
	}
	if n := pool.Participants(); n != 1 {
		t.Fatalf("process pool has %d participants, want 1: %v", n, pool.Shares())
	}
	if host.MemoryArbiter() == pool || host.MemoryArbiter().TotalBytes() != 32<<20 {
		t.Fatal("a host given its own total was put on the process pool")
	}
	if got := host.MemoryShares()["app/hosted"]; got != 32<<20 {
		t.Fatalf("hosted namespace got %d, want its host's whole 32 MiB", got)
	}
}

// The pool's rebalancing goroutine lives exactly as long as some default-sized store is open,
// and a process that closes everything and opens again sizes the pool afresh.
func TestTheLastCloseStopsThePool(t *testing.T) {
	pretendMachine(t, 3<<30)
	loopsBefore := arbiterLoops()
	a := openDefault(t, "app/a")
	b := openDefault(t, "app/b")
	if got := arbiterLoops() - loopsBefore; got != 1 {
		t.Fatalf("%d pool goroutines for two default stores, want 1", got)
	}
	a.Close()
	if got := arbiterLoops() - loopsBefore; got != 1 {
		t.Fatalf("pool stopped while a store still uses it (%d goroutines)", got)
	}
	b.Close()
	if pool, users := currentSharedPool(); pool != nil || users != 0 {
		t.Fatalf("after the last close: pool %v, %d users", pool, users)
	}
	if got := arbiterLoops() - loopsBefore; got != 0 {
		t.Fatalf("%d pool goroutines outlived the last store", got)
	}

	machineMemoryBytes = func() int64 { return 16 << 30 }
	c := openDefault(t, "app/c")
	defer c.Close()
	if pool, _ := currentSharedPool(); pool.TotalBytes() != MaxDefaultMemoryBudgetBytes {
		t.Fatalf("reopened pool is %d MiB, want it re-sized to 256 MiB", pool.TotalBytes()>>20)
	}
}

// arbiterLoops counts the BudgetArbiter rebalancing goroutines running in this process.
func arbiterLoops() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "(*BudgetArbiter).loop(")
		}
		buf = make([]byte, 2*len(buf))
	}
}
