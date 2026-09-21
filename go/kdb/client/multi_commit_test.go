package client_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// startMultiNamespaceServer serves "bank/accounts" on the wire, with "bank/ledger" reachable
// through the same listener by way of a NamespaceSet over one file-backed host - the shape
// kdb-service runs in.
func startMultiNamespaceServer(t *testing.T) string {
	t.Helper()
	host, err := embed.OpenFileHost(t.TempDir(), embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Close() })
	set := server.NewNamespaceSet(host.Transactions())
	var primary *server.KdbServerRuntime
	for _, ns := range []string{"bank/accounts", "bank/ledger"} {
		rt, err := host.Namespace(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		srv := server.NewKdbServerRuntime(rt)
		srv.Namespaces = set
		if err := set.Add(srv); err != nil {
			t.Fatal(err)
		}
		if primary == nil {
			primary = srv
		}
	}
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", primary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return fmt.Sprintf("tcp://%s", ln.Addr().String())
}

func TestCommitAcrossOverTheWire(t *testing.T) {
	addr := startMultiNamespaceServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	acct, _ := randomUUID()
	entry, _ := randomUUID()
	res, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/accounts", Writes: []client.DocWrite{{DocID: acct, JSON: []byte(`{"balance":100}`)}}},
		{Namespace: "bank/ledger", Writes: []client.DocWrite{{DocID: entry, JSON: []byte(`{"opened":100}`)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.GroupID == "" || len(res.Commits) != 2 {
		t.Fatalf("result: %+v", res)
	}

	// Both halves readable, each from its own namespace through the one listener.
	body, acctHead, err := c.GetJSON(ctx, "bank/accounts", acct)
	if err != nil || !strings.Contains(string(body), `"balance":100`) {
		t.Fatalf("account: %s %v", body, err)
	}
	body, ledgerHead, err := c.GetJSON(ctx, "bank/ledger", entry)
	if err != nil || !strings.Contains(string(body), `"opened":100`) {
		t.Fatalf("ledger entry: %s %v", body, err)
	}
	if acctHead != res.Commits["bank/accounts"] || ledgerHead != res.Commits["bank/ledger"] {
		t.Errorf("heads %s/%s are not the transaction's commits %v", acctHead, ledgerHead, res.Commits)
	}

	// A transfer anchored on what was read.
	entry2, _ := randomUUID()
	res2, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/accounts", BaseVersion: acctHead, Writes: []client.DocWrite{{DocID: acct, JSON: []byte(`{"balance":70}`)}}},
		{Namespace: "bank/ledger", BaseVersion: ledgerHead, Writes: []client.DocWrite{{DocID: entry2, JSON: []byte(`{"debit":30}`)}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Replaying the same transfer against the old account version conflicts - and the ledger,
	// which on its own would have accepted its half, is left untouched.
	entry3, _ := randomUUID()
	_, err = c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/accounts", BaseVersion: acctHead, Writes: []client.DocWrite{{DocID: acct, JSON: []byte(`{"balance":40}`)}}},
		{Namespace: "bank/ledger", Writes: []client.DocWrite{{DocID: entry3, JSON: []byte(`{"debit":30}`)}}},
	})
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("want a conflict, got %v", err)
	}
	var cne *client.CrossNamespaceError
	if !errors.As(err, &cne) || cne.Namespace != "bank/accounts" {
		t.Fatalf("want a CrossNamespaceError naming bank/accounts, got %v", err)
	}
	if _, _, err := c.GetJSON(ctx, "bank/ledger", entry3); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("the ledger half of a refused transaction was written: %v", err)
	}
	body, head, err := c.GetJSON(ctx, "bank/accounts", acct)
	if err != nil || !strings.Contains(string(body), `"balance":70`) || head != res2.Commits["bank/accounts"] {
		t.Errorf("account after the refused transfer: %s at %s, %v", body, head, err)
	}

	// Deletes ride along too.
	if _, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/ledger", Deletes: []string{entry2}},
		{Namespace: "bank/accounts", Writes: []client.DocWrite{{DocID: acct, JSON: []byte(`{"balance":100}`)}}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.GetJSON(ctx, "bank/ledger", entry2); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("deleted ledger entry still readable: %v", err)
	}
}

func TestCommitAcrossRefusesUnknownNamespacesAndBadInput(t *testing.T) {
	addr, _ := startTestServer(t) // no NamespaceSet: only app/data is served
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc, _ := randomUUID()

	// The one namespace this server has works on its own.
	if _, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "app/data", Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{}`)}}},
	}); err != nil {
		t.Fatalf("single-namespace CommitAcross: %v", err)
	}
	_, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "app/data", Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{}`)}}},
		{Namespace: "elsewhere", Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{}`)}}},
	})
	var cne *client.CrossNamespaceError
	if !errors.As(err, &cne) || cne.Namespace != "elsewhere" {
		t.Fatalf("want a refusal naming the unknown namespace, got %v", err)
	}
	if _, err := c.CommitAcross(ctx, nil); err == nil {
		t.Error("an empty transaction was accepted")
	}
	if _, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "app/data", Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{}`)}}},
		{Namespace: "app/data", Writes: []client.DocWrite{{DocID: doc, JSON: []byte(`{}`)}}},
	}); err == nil {
		t.Error("a namespace named twice was accepted")
	}
}
