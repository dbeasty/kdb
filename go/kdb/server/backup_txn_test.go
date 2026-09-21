package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func osShim(t *testing.T, dir string) storage.PlatformIOShim {
	t.Helper()
	root := dir
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

// A database backup has to carry the cross-namespace decision log. The data root here was cut
// off mid-transaction - the group's parts are in both namespaces' logs, its decision is not - and
// only the dead epoch's decision file says the group never committed. Restored without it, the
// group would come back as committed: sealed-epoch semantics, "no file means everything in it
// committed".
func TestDatabaseBackupCarriesCrossNamespaceDecisions(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "bank/accounts", "bank/ledger")

	decidedA, decidedB := mustUUID(t), mustUUID(t)
	if _, err := set.CommitAcross([]NamespaceTransaction{
		{Namespace: "bank/accounts", Tx: writeTx(codec.Hash{}, decidedA, `{"decided":true}`)},
		{Namespace: "bank/ledger", Tx: writeTx(codec.Hash{}, decidedB, `{"decided":true}`)},
	}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	crashed := filepath.Join(t.TempDir(), "crashed")
	release := make(chan struct{})
	set.Coordinator().SetBeforeDecisionHookForTest(func(codec.UUID) {
		copyDir(t, root, crashed)
		<-release
	})
	cutA, cutB := mustUUID(t), mustUUID(t)
	done := make(chan error, 1)
	go func() {
		_, err := set.CommitAcross([]NamespaceTransaction{
			{Namespace: "bank/accounts", Tx: writeTx(codec.Hash{}, cutA, `{"cut":true}`)},
			{Namespace: "bank/ledger", Tx: writeTx(codec.Hash{}, cutB, `{"cut":true}`)},
		}, auth.Principal{})
		done <- err
	}()
	for {
		if _, err := os.Stat(crashed); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	host.Close()

	// Back up the crashed data root, as kdb-inspect backup does: under its lock.
	store := &backup.DirStore{Root: filepath.Join(t.TempDir(), "backups")}
	nss, err := embed.ListNamespaces(crashed)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := embed.LockDataDir(crashed)
	if err != nil {
		t.Fatal(err)
	}
	dm, err := backup.CreateDatabase(osShim(t, crashed), nss, store, crashed, nil)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(dm.Txn) == 0 {
		t.Fatal("the database backup carries no transaction state")
	}
	results, err := backup.VerifyDatabase(store, dm.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	for key, res := range results {
		if !res.Clean() {
			t.Fatalf("%s: %v", key, res.Problems)
		}
	}

	restore := func(t *testing.T, withTxn bool) string {
		out := t.TempDir()
		unlock, err := embed.LockDataDir(out)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		if withTxn {
			if err := backup.RestoreTxnState(store, dm, out); err != nil {
				t.Fatal(err)
			}
		}
		outShim := osShim(t, out)
		for _, e := range dm.Entries {
			fetch := t.TempDir()
			if _, err := backup.FetchToDir(store, e.NamespaceID, e.BackupID, fetch); err != nil {
				t.Fatal(err)
			}
			if _, err := recovery.HybridRestore([]recovery.Source{{Label: "backup", Shim: osShim(t, fetch)}},
				e.NamespaceID, storage.CompressionZSTD, outShim); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	open := func(t *testing.T, dir string) (map[string]*embed.EmbeddedKdbRuntime, func()) {
		h, err := embed.OpenFileHost(dir, embed.FileRuntimeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]*embed.EmbeddedKdbRuntime{}
		for _, ns := range []string{"bank/accounts", "bank/ledger"} {
			rt, err := h.Namespace("bank", ns, schema.None())
			if err != nil {
				t.Fatal(err)
			}
			out[ns] = rt
		}
		return out, func() { h.Close() }
	}

	rts, closeRestored := open(t, restore(t, true))
	defer closeRestored()
	if _, ok := docAtRuntime(t, rts["bank/accounts"], decidedA); !ok {
		t.Error("restored: a committed group's part is missing")
	}
	if _, ok := docAtRuntime(t, rts["bank/ledger"], decidedB); !ok {
		t.Error("restored: a committed group's part is missing in the other namespace")
	}
	if _, ok := docAtRuntime(t, rts["bank/accounts"], cutA); ok {
		t.Error("restored: a group that never committed came back in bank/accounts")
	}
	if _, ok := docAtRuntime(t, rts["bank/ledger"], cutB); ok {
		t.Error("restored: a group that never committed came back in bank/ledger")
	}

	// The failure this exists to prevent, made visible: restored without its transaction state,
	// the same backup resurrects the group that never committed.
	bare, closeBare := open(t, restore(t, false))
	defer closeBare()
	if _, ok := docAtRuntime(t, bare["bank/accounts"], cutA); !ok {
		t.Log("note: without the transaction state the cut group did not reappear; the test above is then not load-bearing")
		t.Fail()
	}

	// A damaged decision file fails verification.
	key := "database-backups/" + dm.BackupID + "/txn/" + dm.Txn[len(dm.Txn)-1].Name
	if err := os.WriteFile(filepath.Join(store.Root, filepath.FromSlash(key)), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	results, err = backup.VerifyDatabase(store, dm.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if res, ok := results["txn/"]; !ok || res.Clean() {
		t.Error("a damaged transaction state file verified clean")
	}
	if err := backup.RestoreTxnState(store, dm, t.TempDir()); err == nil {
		t.Error("a damaged transaction state file was restored")
	}
}
