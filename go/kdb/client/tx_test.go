package client_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

func txCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// rowsOf returns the rows a query in ns yields, read outside any transaction.
func rowsOf(t *testing.T, ctx context.Context, c *client.Client, ns, sqlText string) [][]string {
	t.Helper()
	_, rows, err := c.QueryRaw(ctx, ns, sqlText, nil)
	if err != nil {
		t.Fatalf("%s: %v", sqlText, err)
	}
	return rows
}

func seedAccount(t *testing.T, ctx context.Context, c *client.Client, owner string, balance int) {
	t.Helper()
	if err := c.Exec(ctx, "bank/accounts", "INSERT INTO accounts (owner, balance) VALUES (?, ?)", []any{owner, balance}); err != nil {
		t.Fatal(err)
	}
}

func balanceOf(t *testing.T, ctx context.Context, c *client.Client, owner string) string {
	t.Helper()
	rows := rowsOf(t, ctx, c, "bank/accounts", "SELECT balance FROM accounts WHERE owner = '"+owner+"'")
	if len(rows) != 1 {
		t.Fatalf("account %s: %d rows", owner, len(rows))
	}
	return rows[0][0]
}

func TestSQLTransactionCommitsAcrossNamespaces(t *testing.T) {
	c := connectTestClient(t, startMultiNamespaceServer(t))
	ctx := txCtx(t)
	seedAccount(t, ctx, c, "ada", 100)

	tx, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if n, err := tx.Exec(ctx, "UPDATE accounts SET balance = 70 WHERE owner = 'ada'"); err != nil || n != 1 {
		t.Fatalf("update: %d %v", n, err)
	}
	// Qualified: always another namespace.
	if _, err := tx.Exec(ctx, "INSERT INTO bank.ledger (owner, debit) VALUES (?, ?)", "ada", 30); err != nil {
		t.Fatal(err)
	}
	// Unqualified, but bank/ledger exists: routed there too.
	if _, err := tx.Exec(ctx, "INSERT INTO ledger (owner, note) VALUES ('ada', 'unqualified')"); err != nil {
		t.Fatal(err)
	}
	// Inside the transaction nothing is visible from outside it, in either namespace.
	if got := balanceOf(t, ctx, c, "ada"); got != "100" {
		t.Errorf("uncommitted update visible: balance %s", got)
	}
	if rows := rowsOf(t, ctx, c, "bank/ledger", "SELECT owner FROM ledger"); len(rows) != 0 {
		t.Errorf("uncommitted ledger rows visible: %v", rows)
	}

	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := balanceOf(t, ctx, c, "ada"); got != "70" {
		t.Errorf("balance after commit: %s", got)
	}
	if rows := rowsOf(t, ctx, c, "bank/ledger", "SELECT owner FROM ledger"); len(rows) != 2 {
		t.Errorf("ledger after commit: %v", rows)
	}
	// A finished transaction refuses further use.
	if _, err := tx.Exec(ctx, "DELETE FROM accounts"); !errors.Is(err, client.ErrTxDone) {
		t.Errorf("exec after commit: %v", err)
	}
}

func TestSQLTransactionRollbackDiscardsEveryNamespace(t *testing.T) {
	c := connectTestClient(t, startMultiNamespaceServer(t))
	ctx := txCtx(t)
	seedAccount(t, ctx, c, "ada", 100)

	tx, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = 0 WHERE owner = 'ada'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO bank.ledger (owner) VALUES ('ada')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := balanceOf(t, ctx, c, "ada"); got != "100" {
		t.Errorf("rolled-back update landed: %s", got)
	}
	if rows := rowsOf(t, ctx, c, "bank/ledger", "SELECT owner FROM ledger"); len(rows) != 0 {
		t.Errorf("rolled-back insert landed: %v", rows)
	}

	// The session is reused by the next transaction, and starts it clean.
	tx2, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(ctx, "INSERT INTO bank.ledger (owner) VALUES ('grace')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, ctx, c, "bank/ledger", "SELECT owner FROM ledger"); len(rows) != 1 || rows[0][0] != "grace" {
		t.Errorf("after reuse: %v", rows)
	}
}

// Two transactions change the same ledger row. The second to commit conflicts in the ledger - and
// its write to its own namespace, which on its own would have been fine, is not applied either.
func TestSQLTransactionConflictElsewhereWritesNothing(t *testing.T) {
	c := connectTestClient(t, startMultiNamespaceServer(t))
	ctx := txCtx(t)
	seedAccount(t, ctx, c, "ada", 100)
	if err := c.Exec(ctx, "bank/ledger", "INSERT INTO ledger (owner, total) VALUES ('ada', 0)", nil); err != nil {
		t.Fatal(err)
	}

	first, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	for _, tx := range []*client.Tx{first, second} {
		if _, err := tx.Exec(ctx, "UPDATE bank.ledger SET total = 10 WHERE owner = 'ada'"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := second.Exec(ctx, "UPDATE accounts SET balance = 90 WHERE owner = 'ada'"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = second.Commit(ctx)
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("want a conflict, got %v", err)
	}
	var cne *client.CrossNamespaceError
	if !errors.As(err, &cne) || cne.Namespace != "bank/ledger" {
		t.Fatalf("want the conflict attributed to bank/ledger, got %v", err)
	}
	if got := balanceOf(t, ctx, c, "ada"); got != "100" {
		t.Errorf("the conflicting transaction's own-namespace write landed: balance %s", got)
	}
}

// A snapshot transaction reads every namespace as of Begin, whatever commits after.
func TestSQLSnapshotTransactionReadsEveryNamespaceAtOneInstant(t *testing.T) {
	c := connectTestClient(t, startMultiNamespaceServer(t))
	ctx := txCtx(t)
	if err := c.Exec(ctx, "bank/ledger", "INSERT INTO ledger (owner) VALUES ('ada')", nil); err != nil {
		t.Fatal(err)
	}
	tx, err := c.BeginTx(ctx, "bank/accounts", client.TxOptions{Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// Written after Begin, before the transaction first reads the ledger.
	if err := c.Exec(ctx, "bank/ledger", "INSERT INTO ledger (owner) VALUES ('grace')", nil); err != nil {
		t.Fatal(err)
	}
	_, rows, err := tx.QueryRaw(ctx, "SELECT owner FROM bank.ledger")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("snapshot transaction saw %d ledger rows, want the 1 there at Begin: %v", len(rows), rows)
	}

	// Read-committed does see it.
	rc, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Rollback(ctx)
	_, rows, err = rc.QueryRaw(ctx, "SELECT owner FROM ledger")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Errorf("read-committed transaction saw %d ledger rows, want 2", len(rows))
	}
}

func TestSQLTableRouting(t *testing.T) {
	c := connectTestClient(t, startMultiNamespaceServer(t))
	ctx := txCtx(t)

	// An unqualified table that is not a namespace keeps meaning the session's own.
	if err := c.Exec(ctx, "bank/accounts", "INSERT INTO whatever (owner) VALUES ('ada')", nil); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, ctx, c, "bank/accounts", "SELECT owner FROM accounts"); len(rows) != 1 {
		t.Errorf("unqualified unknown table did not land in the session namespace: %v", rows)
	}
	// A qualified read of a namespace that does not exist is an error, not an empty result.
	if _, _, err := c.QueryRaw(ctx, "bank/accounts", "SELECT owner FROM bank.nowhere", nil); err == nil ||
		!strings.Contains(err.Error(), "unknown namespace") {
		t.Errorf("qualified read of a missing namespace: %v", err)
	}
	// BEGIN with writes pending is refused rather than silently committing or dropping them.
	tx, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO bank.ledger (owner) VALUES ('x')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "BEGIN"); err == nil {
		t.Error("BEGIN inside a transaction with pending writes was accepted")
	}
}

// Without a NamespaceSet the server behaves as it always has: one namespace, table names
// ignored - and SQL BEGIN / COMMIT / ROLLBACK now work on it.
func TestSQLTransactionOnSingleNamespaceServer(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := txCtx(t)
	tx, err := c.Begin(ctx, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO anything (v) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO other.ns (v) VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, ctx, c, "app/data", "SELECT v FROM t"); len(rows) != 2 {
		t.Errorf("single-namespace server: %v", rows)
	}
}
