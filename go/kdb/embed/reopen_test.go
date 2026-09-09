package embed_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Reopening a namespace under new storage options, which is what turns
// config.MutabilityNamespaceReopen from a documented class into one that
// actually works.

func reopenHost(t *testing.T, root string) *embed.Host {
	t.Helper()
	h, err := embed.OpenFileHost(root, embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// The data has to survive, which is the only thing that makes a reopen an
// acceptable way to change a setting.
func TestReopenPreservesEverythingDurable(t *testing.T) {
	root := t.TempDir()
	h := reopenHost(t, root)

	sopts := embed.StorageOptions{MemoryBudgetBytes: 4 << 20}
	rt, err := h.NamespaceWithOptions("app", "app/docs", schema.None(), sopts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}

	sopts.DocumentCacheBytes = 1 << 20
	res, err := h.ReopenNamespace("app", "app/docs", schema.None(), sopts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Runtime == nil {
		t.Fatal("a successful reopen returned no runtime")
	}
	if res.Unavailable <= 0 {
		t.Error("a reopen reported no unavailability; that number is the price of the operation")
	}
	for i := 0; i < 30; i++ {
		if _, ok := readDoc(t, res.Runtime, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d did not survive a reopen", i)
		}
	}
	// And the reopened namespace still takes writes.
	writeRotationDoc(t, res.Runtime, "after", 1)
	if _, ok := readDoc(t, res.Runtime, "after"); !ok {
		t.Fatal("the reopened namespace did not accept a write")
	}
}

// A reopen must start from the options the namespace is actually running on,
// or it silently reverts every setting the caller did not happen to name.
func TestStorageOptionsOfReportsWhatIsInForce(t *testing.T) {
	root := t.TempDir()
	h := reopenHost(t, root)

	sopts := embed.StorageOptions{
		MemoryBudgetBytes:  4 << 20,
		DocumentCacheBytes: 1 << 20,
		DisableCheckpoints: true,
	}
	if _, err := h.NamespaceWithOptions("app", "app/docs", schema.None(), sopts); err != nil {
		t.Fatal(err)
	}
	got, ok := h.StorageOptionsOf("app/docs")
	if !ok {
		t.Fatal("the host does not report the options of a namespace it has open")
	}
	if got.DocumentCacheBytes != sopts.DocumentCacheBytes {
		t.Errorf("document cache reported as %d, want %d", got.DocumentCacheBytes, sopts.DocumentCacheBytes)
	}
	if !got.DisableCheckpoints {
		t.Error("a setting that was on came back off, so a reopen from these would silently revert it")
	}
	if _, ok := h.StorageOptionsOf("app/never-opened"); ok {
		t.Error("the host reported options for a namespace it does not have open")
	}
}

// The new options have to actually take effect, or the whole operation is
// theatre.
func TestReopenAppliesTheNewOptions(t *testing.T) {
	root := t.TempDir()
	h := reopenHost(t, root)

	sopts := embed.StorageOptions{MemoryBudgetBytes: 4 << 20}
	if _, err := h.NamespaceWithOptions("app", "app/docs", schema.None(), sopts); err != nil {
		t.Fatal(err)
	}
	sopts.HistoryMode = storage.HistoryModeUnset
	sopts.DeltaMaxSegmentBytes = 4096
	res, err := h.ReopenNamespace("app", "app/docs", schema.None(), sopts)
	if err != nil {
		t.Fatal(err)
	}
	// The rotation cap is the one that is observable from outside: writing
	// past it has to produce more than one segment.
	for i := 0; i < 40; i++ {
		writeRotationDoc(t, res.Runtime, fmt.Sprintf("doc-%d", i), i)
	}
	if n := deltaSegmentCount(t, root); n < 2 {
		t.Fatalf("after reopening with a 4KB segment cap there are %d segments; the new option did not take", n)
	}
}

// Reopening something that is not open is an error rather than a quiet open,
// because it almost always means the caller has the wrong namespace.
func TestReopenRefusesAnUnopenedNamespace(t *testing.T) {
	root := t.TempDir()
	h := reopenHost(t, root)
	if _, err := h.ReopenNamespace("app", "app/missing", schema.None(), embed.StorageOptions{}); err == nil {
		t.Fatal("reopening a namespace that was never open succeeded")
	}
}
