package client_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// startRoutedServer is kdb-service's shape: one listener on bank/accounts, every other namespace
// under the same data root reachable through it, opened (and, for writes, created) on first use.
func startRoutedServer(t *testing.T) (string, *server.NamespaceSet) {
	t.Helper()
	root := t.TempDir()
	host, err := embed.OpenFileHost(root, embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Close() })
	set := server.NewNamespaceSet(host.Transactions())
	open := func(ns string) (*server.KdbServerRuntime, error) {
		rt, err := host.Namespace(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			return nil, err
		}
		srv := server.NewKdbServerRuntime(rt)
		srv.Namespaces = set
		return srv, nil
	}
	set.SetOpener(func(ns string, create bool) (*server.KdbServerRuntime, error) {
		if err := embed.ValidateNamespaceID(ns); err != nil {
			return nil, err
		}
		if !create && !embed.NamespaceExists(root, ns) {
			return nil, fmt.Errorf("%w: %s", server.ErrUnknownNamespace, ns)
		}
		return open(ns)
	})
	primary, err := open("bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Add(primary); err != nil {
		t.Fatal(err)
	}
	if _, err := set.Resolve("bank/ledger", true); err != nil {
		t.Fatal(err)
	}
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", primary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return fmt.Sprintf("tcp://%s", ln.Addr().String()), set
}

// serverHas reads a document straight off the runtime serving ns - the ground truth for which
// namespace a frame actually wrote into.
func serverHas(t *testing.T, set *server.NamespaceSet, ns, docID string) bool {
	t.Helper()
	rt, ok := set.Get(ns)
	if !ok {
		return false
	}
	id, err := codec.UUIDFromString(docID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, found, err := rt.GetDocument(ns, id)
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestSessionFramesReachTheNamespaceTheyName(t *testing.T) {
	addr, set := startRoutedServer(t)
	c := connectTestClient(t, addr)
	ctx := txCtx(t)

	// PutJSON: SESSION_BEGIN + TX_COMMIT with client-built bytes.
	doc, _ := randomUUID()
	head, err := c.PutJSON(ctx, "bank/ledger", doc, []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if !serverHas(t, set, "bank/ledger", doc) {
		t.Error("PutJSON into bank/ledger did not land in bank/ledger")
	}
	if serverHas(t, set, "bank/accounts", doc) {
		t.Error("PutJSON into bank/ledger landed in the listener's own namespace")
	}

	// Commit against the base PutJSON returned: optimistic concurrency, in the right namespace.
	if _, err := c.Commit(ctx, client.Transaction{
		Namespace: "bank/ledger", BaseVersion: head,
		Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{"v":2}`)}},
	}); err != nil {
		t.Fatal(err)
	}
	_, err = c.Commit(ctx, client.Transaction{
		Namespace: "bank/ledger", BaseVersion: head,
		Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{"v":3}`)}},
	})
	if !errors.Is(err, client.ErrConflict) {
		t.Errorf("a stale commit into bank/ledger: want a conflict, got %v", err)
	}

	// Upsert.
	up, _ := randomUUID()
	if _, err := c.Upsert(ctx, "bank/ledger", up, []byte(`{"u":1}`)); err != nil {
		t.Fatal(err)
	}
	if !serverHas(t, set, "bank/ledger", up) || serverHas(t, set, "bank/accounts", up) {
		t.Error("Upsert into bank/ledger landed in the wrong namespace")
	}

	// History.
	hist, err := c.History(ctx, client.HistoryRequest{Namespace: "bank/ledger"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hist.Commits) < 3 {
		t.Errorf("bank/ledger history has %d commits, want the 3 written to it", len(hist.Commits))
	}
	acctHist, err := c.History(ctx, client.HistoryRequest{Namespace: "bank/accounts"})
	if err != nil {
		t.Fatal(err)
	}
	if len(acctHist.Commits) >= len(hist.Commits) {
		t.Errorf("bank/accounts history (%d) looks like it holds bank/ledger's writes (%d)", len(acctHist.Commits), len(hist.Commits))
	}
}

// A lease is held in the namespace it names: it blocks writers there, and only there.
func TestLeasesAreHeldInTheirOwnNamespace(t *testing.T) {
	addr, set := startRoutedServer(t)
	holder := connectTestClient(t, addr)
	other := connectTestClient(t, addr)
	ctx := txCtx(t)
	doc, _ := randomUUID()

	lease, err := holder.AcquireLock(ctx, "bank/ledger", doc, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Namespace != "bank/ledger" {
		t.Errorf("lease namespace %q", lease.Namespace)
	}
	// The holder writes the leased document.
	if _, err := holder.Upsert(ctx, "bank/ledger", doc, []byte(`{"by":"holder"}`)); err != nil {
		t.Fatalf("the lease holder was refused: %v", err)
	}
	// Anyone else is refused in bank/ledger...
	if _, err := other.Upsert(ctx, "bank/ledger", doc, []byte(`{"by":"other"}`)); err == nil {
		t.Error("a document leased in bank/ledger was written by another client")
	}
	// ...and a second lease there is refused too...
	if _, err := other.AcquireLock(ctx, "bank/ledger", doc, time.Minute); !errors.Is(err, client.ErrLockUnavailable) {
		t.Errorf("a second lease on a leased document: %v", err)
	}
	// ...but the same document id in another namespace is a different document.
	if _, err := other.Upsert(ctx, "bank/accounts", doc, []byte(`{"by":"other"}`)); err != nil {
		t.Errorf("a lease in bank/ledger blocked bank/accounts: %v", err)
	}
	if _, err := other.AcquireLock(ctx, "bank/accounts", doc, time.Minute); err != nil {
		t.Errorf("a lease in bank/ledger blocked a lease in bank/accounts: %v", err)
	}

	if err := holder.ReleaseLock(ctx, "bank/ledger", doc); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Upsert(ctx, "bank/ledger", doc, []byte(`{"by":"other"}`)); err != nil {
		t.Errorf("released lease still blocks: %v", err)
	}
	if !serverHas(t, set, "bank/ledger", doc) {
		t.Error("document missing from bank/ledger")
	}
}

func TestSessionBeginOpensNewNamespacesAndRefusesUnsafeNames(t *testing.T) {
	addr, set := startRoutedServer(t)
	c := connectTestClient(t, addr)
	ctx := txCtx(t)

	doc, _ := randomUUID()
	if _, err := c.PutJSON(ctx, "bank/audit", doc, []byte(`{"new":true}`)); err != nil {
		t.Fatal(err)
	}
	if !serverHas(t, set, "bank/audit", doc) {
		t.Error("a write to a new namespace did not create it")
	}
	body, _, err := c.GetJSON(ctx, "bank/audit", doc)
	if err != nil || !strings.Contains(string(body), "new") {
		t.Errorf("reading the new namespace back: %s %v", body, err)
	}

	for _, bad := range []string{"../escape", "_system/users", "bank//x"} {
		if _, err := c.PutJSON(ctx, bad, doc, []byte(`{}`)); err == nil {
			t.Errorf("namespace %q was accepted", bad)
		}
	}
	// A read never creates a namespace.
	if _, _, err := c.GetJSON(ctx, "bank/never", doc); err == nil {
		t.Error("reading a missing namespace succeeded")
	}
	if _, ok := set.Get("bank/never"); ok {
		t.Error("a read created a namespace")
	}
}

// SQL on a session opened on another namespace runs there: its unqualified tables mean that
// namespace, and a transaction started there can still reach back into others.
func TestSQLSessionOnAnotherNamespace(t *testing.T) {
	addr, set := startRoutedServer(t)
	c := connectTestClient(t, addr)
	ctx := txCtx(t)

	tx, err := c.Begin(ctx, "bank/ledger")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO entries (owner) VALUES ('ada')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO bank.accounts (owner, balance) VALUES ('ada', 5)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, ctx, c, "bank/ledger", "SELECT owner FROM entries"); len(rows) != 1 {
		t.Errorf("bank/ledger: %v", rows)
	}
	if rows := rowsOf(t, ctx, c, "bank/accounts", "SELECT balance FROM whatever"); len(rows) != 1 || rows[0][0] != "5" {
		t.Errorf("bank/accounts: %v", rows)
	}
	ledger, _ := set.Get("bank/ledger")
	accounts, _ := set.Get("bank/accounts")
	lh, _ := ledger.Runtime.DAG.Head()
	ah, _ := accounts.Runtime.DAG.Head()
	lc, _ := ledger.Runtime.DAG.GetCommit(lh)
	ac, _ := accounts.Runtime.DAG.GetCommit(ah)
	if lc.TransactionID != ac.TransactionID {
		t.Error("the two namespaces' commits are not one cross-namespace transaction")
	}
}
