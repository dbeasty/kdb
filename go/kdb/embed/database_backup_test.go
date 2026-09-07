package embed_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func dirShim(t *testing.T, dir string) storage.PlatformIOShim {
	t.Helper()
	root := dir
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

// ListNamespaces is what makes whole-database tooling possible: without it, maintenance can only
// act on whichever namespace it was told about, which was fine when a root held exactly one.
func TestListNamespacesFindsEveryNamespaceUnderARoot(t *testing.T) {
	root := t.TempDir()
	if got, err := embed.ListNamespaces(root); err != nil || len(got) != 0 {
		t.Fatalf("an empty root reported %v, %v - want no namespaces and no error", got, err)
	}

	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"zolik/matches", "zolik/users", "other/audit"} {
		if _, err := host.Namespace(embed.CatalogFromNamespace(ns), ns, schema.None()); err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
	}
	host.Close()

	got, err := embed.ListNamespaces(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"other/audit", "zolik/matches", "zolik/users"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ListNamespaces = %v, want %v", got, want)
	}
	// Namespace ids keep their catalog segment, which is what groups them.
	cats := embed.CatalogsOf(got)
	if len(cats["zolik"]) != 2 || len(cats["other"]) != 1 {
		t.Fatalf("CatalogsOf = %v", cats)
	}
}

// The Phase D guarantee, end to end: back up every namespace under one lock, restore them into a
// fresh root, and find all of them intact.
//
// Before namespaces could share a root, "the database" and "the namespace" were the same thing,
// so there was nothing to group. Now that nine can sit under one lock, backing them up one
// command at a time would give N manifests from N different instants - each internally
// consistent, none consistent with the others, and no record of which belong together.
func TestDatabaseBackupAndRestoreRoundTripsEveryNamespace(t *testing.T) {
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatal(err)
	}
	namespaces := []string{"zolik/matches", "zolik/users", "zolik/sessions"}
	ids := make(map[string]codec.UUID, len(namespaces))
	for _, ns := range namespaces {
		rt, err := host.Namespace("zolik", ns, schema.None())
		if err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
		ids[ns] = put(t, rt, ns, `{"ns":"`+ns+`"}`)
	}
	// Closed so every namespace's delta segment is sealed before the backup reads it.
	if err := host.Close(); err != nil {
		t.Fatalf("close host: %v", err)
	}

	found, err := embed.ListNamespaces(root)
	if err != nil {
		t.Fatal(err)
	}
	store := &backup.DirStore{Root: filepath.Join(t.TempDir(), "backups")}

	release, err := embed.LockDataDir(root)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	dm, err := backup.CreateDatabase(dirShim(t, root), found, store, root, nil)
	release()
	if err != nil {
		t.Fatalf("database backup: %v", err)
	}
	if len(dm.Entries) != len(namespaces) {
		t.Fatalf("database manifest has %d entries, want %d", len(dm.Entries), len(namespaces))
	}

	// The manifest is discoverable and reloadable as a unit - that is what makes it a database
	// backup rather than three unrelated namespace backups.
	listed, err := backup.ListDatabaseBackups(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0] != dm.BackupID {
		t.Fatalf("ListDatabaseBackups = %v, want [%s]", listed, dm.BackupID)
	}
	reloaded, err := backup.LoadDatabaseManifest(store, dm.BackupID)
	if err != nil {
		t.Fatalf("reload manifest: %v", err)
	}
	if len(reloaded.Entries) != len(namespaces) {
		t.Fatalf("reloaded manifest has %d entries", len(reloaded.Entries))
	}

	// Every namespace's backup verifies.
	results, err := backup.VerifyDatabase(store, dm.BackupID)
	if err != nil {
		t.Fatalf("verify database: %v", err)
	}
	for ns, res := range results {
		if !res.Clean() {
			t.Fatalf("%s backup is not clean: %+v", ns, res.Problems)
		}
	}

	// Restore all three into a fresh root, the way restoreDatabase does.
	out := t.TempDir()
	outShim := dirShim(t, out)
	for _, e := range reloaded.Entries {
		fetchDir := t.TempDir()
		if _, err := backup.FetchToDir(store, e.NamespaceID, e.BackupID, fetchDir); err != nil {
			t.Fatalf("fetch %s: %v", e.NamespaceID, err)
		}
		sources := []recovery.Source{{Label: "backup", Shim: dirShim(t, fetchDir)}}
		if _, err := recovery.HybridRestore(sources, e.NamespaceID, storage.CompressionZSTD, outShim); err != nil {
			t.Fatalf("restore %s: %v", e.NamespaceID, err)
		}
	}

	// And the restored root really is the database: one host, every namespace, every document.
	restored, err := embed.OpenFileHost(out, hostOpts())
	if err != nil {
		t.Fatalf("open restored root: %v", err)
	}
	defer restored.Close()
	for _, ns := range namespaces {
		rt, err := restored.Namespace("zolik", ns, schema.None())
		if err != nil {
			t.Fatalf("open restored %s: %v", ns, err)
		}
		if got := readBack(t, rt, ns, ids[ns]); !strings.Contains(got, ns) {
			t.Fatalf("restored %s read back %q, want it to contain %q", ns, got, ns)
		}
	}
}
