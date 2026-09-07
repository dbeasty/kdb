package embed_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// fill writes n documents of roughly size bytes each, so a namespace ends up actually holding
// something the arbiter can measure.
func fill(t *testing.T, rt *embed.EmbeddedKdbRuntime, ns string, n, size int) {
	t.Helper()
	body := strings.Repeat("x", size)
	for i := 0; i < n; i++ {
		put(t, rt, ns, fmt.Sprintf(`{"i":%d,"pad":%q}`, i, body))
	}
}

// The compatibility case. A host with one namespace under it must hand that namespace the whole
// pool, because that is exactly what the single-namespace open path gave it before any of this
// existed - 64 MiB, from which the engine derives the same four sub-budgets it always did.
func TestSingleNamespaceGetsTheWholePool(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()
	if _, err := host.Namespace("app", "app/only", schema.None()); err != nil {
		t.Fatalf("open namespace: %v", err)
	}
	shares := host.MemoryArbiter().Shares()
	if got := shares["app/only"]; got != embed.DefaultHostMemoryBudgetBytes {
		t.Fatalf("lone namespace got %d, want the whole %d pool", got, embed.DefaultHostMemoryBudgetBytes)
	}
}

// Nine namespaces divide one pool instead of claiming nine independent ceilings. This is the
// arithmetic the whole component exists to fix: before it, nine namespaces meant nine 64 MiB hot
// tiers - 720 MiB of ceilings - that could not see each other.
func TestNineNamespacesDivideOnePool(t *testing.T) {
	names := []string{
		"matches", "users", "sessions", "scoring_sessions", "match_results",
		"player_stats", "identities", "login_codes", "oauth_flows",
	}
	root := t.TempDir()
	opts := hostOpts()
	opts.Storage.MemoryBudgetBytes = 256 << 20
	host, err := embed.OpenFileHost(root, opts)
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	for _, n := range names {
		if _, err := host.Namespace("zolik", "zolik/"+n, schema.None()); err != nil {
			t.Fatalf("open %s: %v", n, err)
		}
	}
	shares := host.MemoryArbiter().Shares()
	if len(shares) != len(names) {
		t.Fatalf("%d namespaces have shares, want %d", len(shares), len(names))
	}
	var total int64
	for _, s := range shares {
		total += s
	}
	if total > host.MemoryArbiter().TotalBytes() {
		t.Fatalf("shares total %d, over-committing the %d pool", total, host.MemoryArbiter().TotalBytes())
	}
	// Every namespace keeps something it can serve reads out of.
	for ns, s := range shares {
		if s < storage.DefaultNamespaceFloorBytes {
			t.Fatalf("%s got %d, below the %d floor", ns, s, storage.DefaultNamespaceFloorBytes)
		}
	}
}

// The behaviour a static split cannot produce: a namespace that is actually holding data ends up
// with more of the pool than one that is idle, without the idle one losing its floor.
func TestBusyNamespaceBorrowsFromTheIdleOne(t *testing.T) {
	root := t.TempDir()
	opts := hostOpts()
	opts.Storage.MemoryBudgetBytes = 64 << 20
	host, err := embed.OpenFileHost(root, opts)
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	busy, err := host.Namespace("app", "app/busy", schema.None())
	if err != nil {
		t.Fatalf("open app/busy: %v", err)
	}
	idle, err := host.Namespace("app", "app/idle", schema.None())
	if err != nil {
		t.Fatalf("open app/idle: %v", err)
	}

	// Both start even, holding nothing.
	if before := host.MemoryArbiter().Shares(); before["app/busy"] != before["app/idle"] {
		t.Fatalf("shares started uneven: %v", before)
	}

	fill(t, busy, "app/busy", 200, 4096)
	put(t, idle, "app/idle", `{"one":"document"}`)
	host.MemoryArbiter().Rebalance()

	after := host.MemoryArbiter().Shares()
	gotBusy, gotIdle := after["app/busy"], after["app/idle"]
	if gotBusy <= gotIdle {
		t.Fatalf("busy namespace got %d and idle got %d - demand did not move the split", gotBusy, gotIdle)
	}
	if gotIdle < storage.DefaultNamespaceFloorBytes {
		t.Fatalf("idle namespace fell to %d, below the %d floor", gotIdle, storage.DefaultNamespaceFloorBytes)
	}
	if sum := gotBusy + gotIdle; sum > host.MemoryArbiter().TotalBytes() {
		t.Fatalf("shares sum to %d, over the %d pool", sum, host.MemoryArbiter().TotalBytes())
	}
	t.Logf("busy=%d idle=%d pool=%d", gotBusy, gotIdle, host.MemoryArbiter().TotalBytes())

	// And the data is all still readable at the smaller budgets - a share is a cache ceiling,
	// never a limit on what the namespace holds.
	head, err := busy.DAG.Head()
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	commit, err := busy.DAG.GetCommitOrThrow(head)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	n := 0
	err = busy.Storage.ScanDocuments("app/busy", commit.DocumentTreeHash, 64, func(batch []document.Document) error {
		n += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 200 {
		t.Fatalf("scanned %d documents, want all 200 back", n)
	}
}

// Closing a namespace hands its share back to the ones that are staying, rather than leaving the
// pool permanently fragmented by namespaces that are no longer open.
func TestClosingANamespaceReturnsItsShare(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	if _, err := host.Namespace("app", "app/stay", schema.None()); err != nil {
		t.Fatalf("open app/stay: %v", err)
	}
	if _, err := host.Namespace("app", "app/going", schema.None()); err != nil {
		t.Fatalf("open app/going: %v", err)
	}
	if err := host.CloseNamespace("app/going"); err != nil {
		t.Fatalf("close app/going: %v", err)
	}
	if got := host.MemoryArbiter().Shares()["app/stay"]; got != embed.DefaultHostMemoryBudgetBytes {
		t.Fatalf("survivor got %d, want the whole %d pool back", got, embed.DefaultHostMemoryBudgetBytes)
	}
	if _, still := host.MemoryArbiter().Shares()["app/going"]; still {
		t.Fatal("a closed namespace still holds a share of the pool")
	}
}

// An explicit per-namespace budget opts that namespace out of arbitration entirely - the escape
// hatch for a caller who has already decided what one namespace needs.
func TestExplicitNamespaceBudgetIsNotArbitrated(t *testing.T) {
	root := t.TempDir()
	opts := hostOpts()
	opts.Storage.MemoryBudgetBytes = 100 << 20
	host, err := embed.OpenFileHost(root, opts)
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	pinned := embed.StorageOptions{MemoryBudgetBytes: 40 << 20}
	if _, err := host.NamespaceWithOptions("app", "app/pinned", schema.None(), pinned); err != nil {
		t.Fatalf("open app/pinned: %v", err)
	}
	other, err := host.Namespace("app", "app/other", schema.None())
	if err != nil {
		t.Fatalf("open app/other: %v", err)
	}
	fill(t, other, "app/other", 200, 4096)
	host.MemoryArbiter().Rebalance()

	shares := host.MemoryArbiter().Shares()
	if got := shares["app/pinned"]; got != 40<<20 {
		t.Fatalf("pinned namespace got %d, want its reserved %d however busy the others are", got, int64(40<<20))
	}
	if got := shares["app/other"]; got != 60<<20 {
		t.Fatalf("other namespace got %d, want the remaining %d", got, int64(60<<20))
	}
}

// An explicitly configured sub-budget survives arbitration.
//
// The arbiter hands a namespace a share and the engine splits it across the three things it
// holds - but a caller who named DocumentCacheBytes or CommitOpsBytes outright meant that
// number, and re-deriving it from the share would be the same defect the Resolved* helpers
// already avoid. Honouring it at open and overwriting it on the first rebalance is worse than
// never honouring it: it looks correct for as long as anyone watches.
//
// TestExplicitRetentionBudgetsAreHonoured has guarded this since before there was an arbiter,
// and caught it when Phase C broke it. This is the same property asserted deliberately, through
// a host, where the interaction actually lives.
func TestArbitrationDoesNotOverrideExplicitSubBudgets(t *testing.T) {
	root := t.TempDir()
	opts := hostOpts()
	opts.Storage.MemoryBudgetBytes = 512 << 20
	host, err := embed.OpenFileHost(root, opts)
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	defer host.Close()

	// A pool large enough that a derived document budget would keep everything, with an explicit
	// one small enough that it cannot.
	pinned := embed.StorageOptions{
		DocumentCacheBytes: 1 << 20,
		CommitOpsBytes:     1 << 20,
	}
	rt, err := host.NamespaceWithOptions("bench", "bench/matches", schema.None(), pinned)
	if err != nil {
		t.Fatalf("open namespace: %v", err)
	}
	// A second namespace, so rebalancing actually has something to do.
	if _, err := host.Namespace("bench", "bench/other", schema.None()); err != nil {
		t.Fatalf("open second namespace: %v", err)
	}

	// 120 versions of one document, then a rebalance, then read the oldest back. The read is
	// what turns eviction into an observable cold load - writing alone evicts but never proves
	// it, which is what the first draft of this test got wrong.
	commits, texts := writeVersions(t, rt, 120)
	host.MemoryArbiter().Rebalance()

	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skip("not the server engine")
	}
	docID, _, err := document.ResolveID(texts[0])
	if err != nil {
		t.Fatal(err)
	}
	commit, err := rt.DAG.GetCommitOrThrow(commits[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := rt.Storage.GetDocument("bench/matches", docID, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != texts[0] {
		t.Fatal("the oldest version was not readable after arbitration")
	}
	if eng.ColdDocumentLoads() == 0 {
		t.Fatal("nothing evicted under an explicit 1MiB document budget - arbitration overrode it")
	}
}
