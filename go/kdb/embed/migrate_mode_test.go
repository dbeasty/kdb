package embed_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func TestMigratingFullToNoneAndBack(t *testing.T) {
	root := t.TempDir()
	rt := fullRuntime(t, root)
	for i := 0; i < 3; i++ {
		if _, err := embed.PutJSONDocument(rt, "app/docs", fmt.Sprintf(`{"id":"a","n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	// Opening as none is refused until the namespace is actually converted.
	if _, err := noneRuntimeOrError(root, storage.RetentionWindow{}); err == nil {
		t.Fatal("a full namespace should not open as none without a migration")
	}

	if err := embed.MigrateHistoryMode(root, "app/docs", storage.HistoryModeNone); err != nil {
		t.Fatal(err)
	}
	mode, strategy := readMeta(t, root, "app", "docs")
	if mode != "none" {
		t.Fatalf("after migration the mode is %q", mode)
	}
	if strategy != "replay" {
		t.Fatalf("migrating to none must force replay, got %q", strategy)
	}

	// The data survives the conversion, and is readable under the new mode.
	rt = noneRuntime(t, root, storage.RetentionWindow{})
	body, ok := readDoc(t, rt, "a")
	if !ok || !strings.Contains(body, `"n":2`) {
		t.Fatalf("after migrating to none, the document reads %q", body)
	}
	// And the invariant truncation depends on already holds, without any
	// session having written under the new mode.
	type durability interface{ LiveBodiesDurable() bool }
	if eng, ok := rt.Storage.(durability); ok && !eng.LiveBodiesDurable() {
		t.Fatal("migration to none left live bodies with no home outside the delta log")
	}
	rt.Close()

	if err := embed.MigrateHistoryMode(root, "app/docs", storage.HistoryModeFull); err != nil {
		t.Fatal(err)
	}
	mode, _ = readMeta(t, root, "app", "docs")
	if mode != "full" {
		t.Fatalf("after migrating back the mode is %q", mode)
	}
	rt = fullRuntime(t, root)
	defer rt.Close()
	if body, ok := readDoc(t, rt, "a"); !ok || !strings.Contains(body, `"n":2`) {
		t.Fatalf("after migrating back, the document reads %q", body)
	}
}

func TestMigrateIsIdempotentAndRejectsNonsense(t *testing.T) {
	root := t.TempDir()
	rt := fullRuntime(t, root)
	if _, err := embed.PutJSONDocument(rt, "app/docs", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	for i := 0; i < 2; i++ {
		if err := embed.MigrateHistoryMode(root, "app/docs", storage.HistoryModeNone); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if err := embed.MigrateHistoryMode(root, "app/docs", storage.HistoryModeUnset); err == nil {
		t.Fatal("migrating to the unset mode should be refused")
	}
	if err := embed.MigrateHistoryMode(root, "no/such", storage.HistoryModeNone); err == nil {
		t.Fatal("migrating a namespace that does not exist should be refused")
	}
}

func noneRuntimeOrError(root string, window storage.RetentionWindow) (*embed.EmbeddedKdbRuntime, error) {
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = window
	return embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
}

// The conversion must not be reachable while the directory is open, which
// is what keeps the marker and the bytes from disagreeing.
func TestMigrateRefusesWhileTheDirectoryIsOpen(t *testing.T) {
	root := t.TempDir()
	rt := fullRuntime(t, root)
	defer rt.Close()
	if _, err := embed.PutJSONDocument(rt, "app/docs", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	err := embed.MigrateHistoryMode(root, "app/docs", storage.HistoryModeNone)
	if err == nil {
		t.Fatal("migration ran while a runtime held the data directory")
	}
	var mismatch *embed.HistoryModeMismatchError
	if errors.As(err, &mismatch) {
		t.Fatal("the failure should be the lock, not a mode mismatch")
	}
}
