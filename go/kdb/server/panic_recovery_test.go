package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// TestMalformedStatementIsAnErrorNotAnOutage covers the bug that motivated all of this:
// `SELECT 1` - a projection that is a literal rather than an identifier, and a standard
// connectivity probe - used to panic the SQL parser, and with nothing recovering on the
// frame-handling path it killed the entire server process, taking every other connection and
// namespace with it.
//
// Two properties are asserted, and they are different claims:
//
//  1. The client gets a descriptive parse error - not "internal server error", which is what
//     the panic backstop produced while the parser was still panicking, and not silence, which
//     leaves a caller hanging until its own deadline.
//  2. Everyone else's connection is unaffected. That is the part that made this severe rather
//     than merely annoying.
func TestMalformedStatementIsAnErrorNotAnOutage(t *testing.T) {
	runtime, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(runtime)
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	victim, err := client.Connect(ctx, addr, "")
	if err != nil {
		t.Fatalf("connect victim: %v", err)
	}
	defer victim.Close()

	bystander, err := client.Connect(ctx, addr, "")
	if err != nil {
		t.Fatalf("connect bystander: %v", err)
	}
	defer bystander.Close()

	_, _, err = victim.QueryRaw(ctx, "app/data", "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected an error from a malformed statement")
	}
	// The parser's own message, naming what it found - not the backstop's generic reply.
	if !strings.Contains(err.Error(), "expected identifier") {
		t.Fatalf("expected a descriptive parse error, got: %v", err)
	}
	if strings.Contains(err.Error(), "internal server error") {
		t.Fatalf("statement reached the panic backstop instead of being parsed cleanly: %v", err)
	}

	// The connection that sent it is still usable...
	if _, _, err := victim.QueryRaw(ctx, "app/data", "SELECT 1", nil); err == nil {
		t.Fatal("expected the same error on a second attempt")
	}

	// ...and so is everyone else's. Had the process died, this would fail on a closed
	// connection rather than returning a clean per-statement error.
	if _, _, err := bystander.QueryRaw(ctx, "app/data", "SELECT 1", nil); err == nil {
		t.Fatal("expected an error for the bystander too")
	} else if !strings.Contains(err.Error(), "expected identifier") {
		t.Fatalf("bystander connection did not survive: %v", err)
	}

	// A well-formed statement still works, proving the server is serving rather than merely
	// still running.
	if err := bystander.Exec(ctx, "app/data",
		`CREATE TABLE players (name VARCHAR NOT NULL, level VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatalf("server no longer serving valid statements: %v", err)
	}
}

// TestMalformedStatementVariantsAllReturnErrors sweeps the shapes most likely to reach a
// parser edge from a real client - a probe, a truncated statement, a typo - and asserts every
// one comes back as an error on a connection that stays usable.
func TestMalformedStatementVariantsAllReturnErrors(t *testing.T) {
	runtime, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", server.NewKdbServerRuntime(runtime))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, fmt.Sprintf("tcp://%s", ln.Addr().String()), "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	for _, sql := range []string{
		"SELECT 1",
		"SELECT",
		"SELECT * FROM",
		"SELECT a FROM 1",
		"",
		"DROP TABLE players",
		"INSERT INTO",
		"CREATE TABLE",
		"SELECT a FROM t WHERE b = 1.2.3",
	} {
		if _, _, err := c.QueryRaw(ctx, "app/data", sql, nil); err == nil {
			t.Errorf("QueryRaw(%q) unexpectedly succeeded", sql)
		}
	}

	// One connection, every malformed statement above, still serving afterwards.
	if _, _, err := c.QueryRaw(ctx, "app/data", "SELECT name FROM players", nil); err != nil &&
		strings.Contains(err.Error(), "connection") {
		t.Fatalf("connection did not survive the sweep: %v", err)
	}
}
