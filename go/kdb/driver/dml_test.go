package driver_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	kdbdriver "github.com/limidus/kdb/go/kdb/driver"
)

// memDB opens a shared in-memory database private to this test. isolate= (not unique=) so every
// pooled connection sees the same data.
func memDB(t *testing.T, namespace string) *sql.DB {
	t.Helper()
	db, err := kdbdriver.Open(fmt.Sprintf("kdb://memory:///%s?isolate=%s&dropOnClose=true", namespace, strings.ReplaceAll(t.Name(), "/", "_")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func fileDB(t *testing.T, root, namespace string) *sql.DB {
	t.Helper()
	db, err := kdbdriver.Open("kdb://file://" + root + "/" + namespace)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func count(t *testing.T, db interface {
	QueryRow(string, ...any) *sql.Row
}, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func mustExec(t *testing.T, db interface {
	Exec(string, ...any) (sql.Result, error)
}, query string, args ...any) sql.Result {
	t.Helper()
	res, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return res
}

func TestAutocommitCRUDAndTypedValues(t *testing.T) {
	db := memDB(t, "shop/items")
	res := mustExec(t, db, "INSERT INTO items (name, price, stock, active, note) VALUES (?, ?, ?, ?, ?)",
		"lamp", 19.5, int64(3), true, nil)
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("insert affected %d", n)
	}
	mustExec(t, db, "INSERT INTO items (name, price, stock, active) VALUES ('desk', 120.0, 1, false)")

	var name string
	var price float64
	var stock int64
	var active bool
	var note sql.NullString
	if err := db.QueryRow("SELECT name, price, stock, active, note FROM items WHERE name = ?", "lamp").
		Scan(&name, &price, &stock, &active, &note); err != nil {
		t.Fatal(err)
	}
	if name != "lamp" || price != 19.5 || stock != 3 || !active || note.Valid {
		t.Errorf("scanned %q %v %d %v %+v", name, price, stock, active, note)
	}

	res = mustExec(t, db, "UPDATE items SET stock = 0 WHERE active = true")
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("update affected %d", n)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items WHERE stock = 0"); n != 1 {
		t.Errorf("after update: %d", n)
	}
	res = mustExec(t, db, "DELETE FROM items WHERE name = 'desk'")
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("delete affected %d", n)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 1 {
		t.Errorf("after delete: %d", n)
	}
	// SELECT 1 still works, and a prepared statement binds positionally.
	if n := count(t, db, "SELECT 1"); n != 1 {
		t.Errorf("SELECT 1 = %d", n)
	}
	st, err := db.Prepare("SELECT COUNT(*) FROM items WHERE name = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var n int
	if err := st.QueryRow("lamp").Scan(&n); err != nil || n != 1 {
		t.Errorf("prepared: %d %v", n, err)
	}
}

func TestTransactionCommitAndRollback(t *testing.T) {
	db := memDB(t, "shop/items")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO items (name) VALUES ('a')")
	mustExec(t, tx, "INSERT INTO items (name) VALUES ('b')")
	// Not visible outside the transaction until it commits.
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 0 {
		t.Errorf("uncommitted rows visible: %d", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 2 {
		t.Errorf("after commit: %d", n)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "DELETE FROM items")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 2 {
		t.Errorf("rolled-back delete applied: %d", n)
	}
}

// Statements name other namespaces by table; the transaction commits all of them or none.
func TestTransactionAcrossNamespaces(t *testing.T) {
	db := memDB(t, "bank/accounts")
	mustExec(t, db, "INSERT INTO accounts (owner, balance) VALUES ('ada', 100)")
	mustExec(t, db, "INSERT INTO bank.ledger (owner, total) VALUES ('ada', 0)") // creates bank/ledger

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "UPDATE accounts SET balance = 70 WHERE owner = 'ada'")
	mustExec(t, tx, "UPDATE ledger SET total = 30 WHERE owner = 'ada'") // unqualified: bank/ledger exists
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "SELECT balance FROM accounts WHERE owner = 'ada'"); n != 70 {
		t.Errorf("balance %d", n)
	}
	if n := count(t, db, "SELECT total FROM bank.ledger WHERE owner = 'ada'"); n != 30 {
		t.Errorf("ledger total %d", n)
	}

	// Two transactions change the same ledger row. The second conflicts there, and its update to
	// accounts - fine on its own - does not land either.
	first, _ := db.Begin()
	second, _ := db.Begin()
	mustExec(t, first, "UPDATE bank.ledger SET total = 40 WHERE owner = 'ada'")
	mustExec(t, second, "UPDATE bank.ledger SET total = 50 WHERE owner = 'ada'")
	mustExec(t, second, "UPDATE accounts SET balance = 0 WHERE owner = 'ada'")
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	err = second.Commit()
	if !errors.Is(err, kdbdriver.ErrConflict) {
		t.Fatalf("want a conflict, got %v", err)
	}
	var ce *kdbdriver.ConflictError
	if !errors.As(err, &ce) || ce.Namespace != "bank/ledger" {
		t.Errorf("conflict attributed to %+v", ce)
	}
	if n := count(t, db, "SELECT balance FROM accounts WHERE owner = 'ada'"); n != 70 {
		t.Errorf("the conflicting transaction's accounts write landed: %d", n)
	}
	// A read of a namespace that does not exist is an error, not an empty result.
	if _, err := db.Query("SELECT owner FROM bank.nowhere"); !errors.Is(err, kdbdriver.ErrUnknownNamespace) {
		t.Errorf("reading a missing namespace: %v", err)
	}
}

func TestSnapshotTransactionReadsEveryNamespaceAtOneInstant(t *testing.T) {
	db := memDB(t, "bank/accounts")
	mustExec(t, db, "INSERT INTO bank.ledger (owner) VALUES ('ada')")
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, db, "INSERT INTO bank.ledger (owner) VALUES ('grace')")
	if n := count(t, tx, "SELECT COUNT(*) FROM bank.ledger"); n != 1 {
		t.Errorf("snapshot transaction saw %d ledger rows, want the 1 there at begin", n)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM bank.ledger"); n != 2 {
		t.Errorf("outside the transaction: %d", n)
	}
	if _, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable}); err == nil {
		t.Error("serializable was accepted, which this driver cannot honour")
	}
	ro, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Rollback()
	if _, err := ro.Exec("INSERT INTO accounts (x) VALUES (1)"); err == nil {
		t.Error("a read-only transaction wrote")
	}
}

func TestBeginCommitAsStatements(t *testing.T) {
	db := memDB(t, "shop/items")
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	for _, q := range []string{"BEGIN", "INSERT INTO items (n) VALUES (1)", "INSERT INTO items (n) VALUES (2)"} {
		if _, err := c.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 0 {
		t.Errorf("visible before COMMIT: %d", n)
	}
	if _, err := c.ExecContext(ctx, "BEGIN"); err == nil {
		t.Error("BEGIN with pending writes was accepted")
	}
	if _, err := c.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 2 {
		t.Errorf("after COMMIT: %d", n)
	}
	if _, err := c.ExecContext(ctx, "BEGIN WORK"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "DELETE FROM items"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM items"); n != 2 {
		t.Errorf("after ROLLBACK: %d", n)
	}
}

func TestUniqueConstraintAndParameterRules(t *testing.T) {
	db := memDB(t, "app/users")
	mustExec(t, db, "CREATE TABLE users (email VARCHAR NOT NULL UNIQUE)")
	mustExec(t, db, "INSERT INTO users (email) VALUES ('a@x')")
	_, err := db.Exec("INSERT INTO users (email) VALUES ('a@x')")
	var se *kdbdriver.SchemaError
	if !errors.As(err, &se) {
		t.Errorf("duplicate unique value: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (email) VALUES (@e)", sql.Named("e", "b@x")); err == nil {
		t.Error("a named parameter was accepted")
	}

	ro, err := kdbdriver.Open(fmt.Sprintf("kdb://memory:///app/users?isolate=%s&readOnly=true", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Exec("INSERT INTO users (email) VALUES ('c@x')"); err == nil {
		t.Error("a read-only connection wrote")
	}
	if n := count(t, ro, "SELECT COUNT(*) FROM users"); n != 1 {
		t.Errorf("read-only connection reads %d", n)
	}
}

// File mode: every pooled connection shares the data root's one host - before, the second
// connection to open failed on the directory lock - and writes from many of them at once all land
// and survive a reopen. Run under -race.
func TestFileDatabaseSharedAcrossPooledConnections(t *testing.T) {
	root := t.TempDir()
	db := fileDB(t, root, "shop/orders")
	db.SetMaxOpenConns(8)
	const writers, each = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := db.Exec("INSERT INTO orders (w, i) VALUES (?, ?)", int64(w), int64(i)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// A transaction across namespaces on the file database too.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO shop.audit (what) VALUES ('bulk')")
	mustExec(t, tx, "INSERT INTO orders (w, i) VALUES (-1, -1)")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := fileDB(t, root, "shop/orders")
	defer db2.Close()
	if n := count(t, db2, "SELECT COUNT(*) FROM orders"); n != writers*each+1 {
		t.Errorf("after reopen: %d orders, want %d", n, writers*each+1)
	}
	if n := count(t, db2, "SELECT COUNT(*) FROM shop.audit"); n != 1 {
		t.Errorf("after reopen: %d audit rows", n)
	}
}

// Concurrent read-modify-write transactions on one counter, at snapshot isolation: every commit
// either lands on the value it read or conflicts, so no increment is lost. (At read committed a
// transaction's conflict base is fixed by its first write, not its first read - the wire
// server's rule too - so a SELECT-then-UPDATE there can lose an update, as SQL permits.)
func TestConcurrentTransactionsNeverLoseAnUpdate(t *testing.T) {
	db := memDB(t, "app/counters")
	db.SetMaxOpenConns(8)
	mustExec(t, db, "INSERT INTO counters (name, n) VALUES ('c', 0)")
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := 0
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for done := 0; done < 10; {
				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
				if err != nil {
					t.Error(err)
					return
				}
				var n int
				if err := tx.QueryRow("SELECT n FROM counters WHERE name = 'c'").Scan(&n); err != nil {
					tx.Rollback()
					t.Error(err)
					return
				}
				if _, err := tx.Exec("UPDATE counters SET n = ? WHERE name = 'c'", int64(n+1)); err != nil {
					tx.Rollback()
					t.Error(err)
					return
				}
				err = tx.Commit()
				switch {
				case err == nil:
					mu.Lock()
					committed++
					mu.Unlock()
					done++
				case errors.Is(err, kdbdriver.ErrConflict):
				default:
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n := count(t, db, "SELECT n FROM counters WHERE name = 'c'"); n != committed || committed != 80 {
		t.Errorf("counter %d after %d committed increments", n, committed)
	}
}

// sql.Open with the full kdb:// URL - the form the user guide shows - works as well as the bare
// DSN kdbdriver.Open passes through.
func TestSQLOpenAcceptsTheFullURL(t *testing.T) {
	db, err := sql.Open("kdb", "kdb://memory:///demo/users?unique=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}
