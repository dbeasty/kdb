package embed_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func modeRuntime(tb testing.TB, root string, m storage.HistoryMode, s storage.HistoryStrategy) (*embed.EmbeddedKdbRuntime, error) {
	tb.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.HistoryMode = m
	opts.Storage.HistoryStrategy = s
	return embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
}

func readMeta(t *testing.T, root string, namespacePath ...string) (mode, strategy string) {
	t.Helper()
	parts := append([]string{root, "ns"}, defaultNamespacePath(namespacePath)...)
	raw, err := os.ReadFile(filepath.Join(append(parts, "meta.json")...))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		HistoryMode     string `json:"historyMode"`
		HistoryStrategy string `json:"historyStrategy"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m.HistoryMode, m.HistoryStrategy
}

// defaultNamespacePath keeps readMeta callable as readMeta(t, root) for
// the bench/matches namespace these tests mostly use, while letting a
// caller name another one.
func defaultNamespacePath(given []string) []string {
	if len(given) == 0 {
		return []string{"bench", "matches"}
	}
	return given
}

func TestNewNamespaceRecordsFullByDefault(t *testing.T) {
	root := t.TempDir()
	rt, err := modeRuntime(t, root, storage.HistoryModeUnset, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	mode, strategy := readMeta(t, root)
	if mode != "full" {
		t.Fatalf("a new namespace recorded history mode %q, want full", mode)
	}
	if strategy != storage.DefaultHistoryStrategy.String() {
		t.Fatalf("the strategy default changed: %q", strategy)
	}
}

func TestNoneModeIsRecordedAndForcesReplay(t *testing.T) {
	root := t.TempDir()
	rt, err := modeRuntime(t, root, storage.HistoryModeNone, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	mode, strategy := readMeta(t, root)
	if mode != "none" {
		t.Fatalf("history mode recorded as %q, want none", mode)
	}
	// The coupling: tree objects serve reads at commits this mode
	// reclaims, so none must never record objects.
	if strategy != "replay" {
		t.Fatalf("history=none recorded strategy %q, want replay", strategy)
	}
}

func TestAskingForNoneAndObjectsIsRefused(t *testing.T) {
	root := t.TempDir()
	_, err := modeRuntime(t, root, storage.HistoryModeNone, storage.HistoryStrategyObjects)
	var conflict *embed.HistoryModeStrategyConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want HistoryModeStrategyConflictError, got %v", err)
	}
}

func TestReopeningUnderTheOtherModeIsRefused(t *testing.T) {
	root := t.TempDir()
	rt, err := modeRuntime(t, root, storage.HistoryModeNone, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()

	_, err = modeRuntime(t, root, storage.HistoryModeFull, storage.HistoryStrategyUnset)
	var mismatch *embed.HistoryModeMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("want HistoryModeMismatchError, got %v", err)
	}
	if mismatch.Recorded != storage.HistoryModeNone || mismatch.Requested != storage.HistoryModeFull {
		t.Fatal("the error should name both modes")
	}

	// And the reverse direction, which is the dangerous one: opening a
	// full-history namespace as none would licence deleting segments the
	// operator believes are the record.
	root2 := t.TempDir()
	rt2, err := modeRuntime(t, root2, storage.HistoryModeFull, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt2.Close()
	_, err = modeRuntime(t, root2, storage.HistoryModeNone, storage.HistoryStrategyUnset)
	if !errors.As(err, &mismatch) {
		t.Fatalf("want HistoryModeMismatchError opening full as none, got %v", err)
	}
}

func TestAnUnmarkedNamespaceIsFull(t *testing.T) {
	root := t.TempDir()
	rt, err := modeRuntime(t, root, storage.HistoryModeUnset, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	// Strip the mode the way a namespace written before the setting
	// existed would have it: strategy recorded, mode absent.
	path := filepath.Join(root, "ns", "bench", "matches", "meta.json")
	if err := os.WriteFile(path, []byte(`{"namespaceId":"bench/matches","historyStrategy":"objects"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	rt, err = modeRuntime(t, root, storage.HistoryModeUnset, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
	mode, _ := readMeta(t, root)
	if mode != "full" {
		t.Fatalf("an unmarked namespace resolved to %q; nothing has ever reclaimed a commit from it, so it is full", mode)
	}
	// ...and asking for none on it is refused rather than silently
	// starting to delete its segments.
	if _, err := modeRuntime(t, root, storage.HistoryModeNone, storage.HistoryStrategyUnset); err == nil {
		t.Fatal("an unmarked namespace should not be openable as none")
	}
}

func TestReopeningUnderTheSameModeSucceeds(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 3; i++ {
		rt, err := modeRuntime(t, root, storage.HistoryModeNone, storage.HistoryStrategyUnset)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		rt.Close()
	}
	// And with the mode left unset, which is what a caller that does not
	// care passes.
	rt, err := modeRuntime(t, root, storage.HistoryModeUnset, storage.HistoryStrategyUnset)
	if err != nil {
		t.Fatal(err)
	}
	rt.Close()
}
