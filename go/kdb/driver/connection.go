package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/embed"
)

type conn struct {
	parsed  ParsedURL
	runtime *embed.EmbeddedKdbRuntime
	release func()
	closed  bool
	mu      sync.Mutex
}

func openConn(parsed ParsedURL) (*conn, error) {
	rt, release, err := openRuntime(parsed)
	if err != nil {
		return nil, err
	}
	return &conn{parsed: parsed, runtime: rt, release: release}, nil
}

func (c *conn) checkOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.release != nil {
		c.release()
	}
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// Ping implements driver.Pinger.
func (c *conn) Ping(ctx context.Context) error {
	return c.checkOpen()
}

// Catalog returns the JDBC-style catalog from the connection URL.
func (c *conn) Catalog() string {
	return c.parsed.Catalog
}

// NamespaceID returns the namespace id from the connection URL.
func (c *conn) NamespaceID() string {
	return c.parsed.NamespaceID
}

// Runtime returns the embedded runtime (for advanced callers).
func (c *conn) Runtime() *embed.EmbeddedKdbRuntime {
	return c.runtime
}

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.conn.checkOpen(); err != nil {
		return nil, err
	}
	if s.conn.parsed.ReadOnly {
		return nil, fmt.Errorf("connection is read-only")
	}
	q := strings.TrimSpace(strings.ToUpper(s.query))
	if strings.HasPrefix(q, "UPDATE ") || strings.HasPrefix(q, "INSERT ") || strings.HasPrefix(q, "DELETE ") {
		return nil, fmt.Errorf("SQL DML not yet ported to Go driver")
	}
	return driver.RowsAffected(0), nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.conn.checkOpen(); err != nil {
		return nil, err
	}
	q := strings.TrimSpace(strings.ToUpper(s.query))
	switch {
	case q == "SELECT 1" || q == "SELECT 1;":
		return &rows{
			columns: []string{"1"},
			data:    [][]driver.Value{{int64(1)}},
		}, nil
	case strings.HasPrefix(q, "SELECT"):
		return nil, fmt.Errorf("SQL query engine not yet ported to Go driver")
	default:
		return nil, fmt.Errorf("unsupported statement: %s", s.query)
	}
}

type rows struct {
	columns []string
	data    [][]driver.Value
	idx     int
}

func (r *rows) Columns() []string { return r.columns }

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.idx])
	r.idx++
	return nil
}
