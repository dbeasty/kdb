package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/sql"
)

// conn is one database/sql connection: a view onto a shared database, anchored on the namespace
// its URL names. Statements run against that namespace unless their table names another (see
// route), and writes outside a transaction commit immediately.
type conn struct {
	parsed ParsedURL
	db     *database
	ns     *namespace

	mu     sync.Mutex
	closed bool
	// tx is the open transaction - begun through database/sql's BeginTx or by a BEGIN statement
	// - or nil in autocommit.
	tx *connTx
}

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
)

func openConn(parsed ParsedURL) (*conn, error) {
	db, err := databases.acquire(parsed)
	if err != nil {
		return nil, err
	}
	ns, err := db.namespace(parsed.NamespaceID, true)
	if err != nil {
		databases.release(db)
		return nil, err
	}
	return &conn{parsed: parsed, db: db, ns: ns}, nil
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
	if c.tx != nil {
		c.tx.release()
		c.tx = nil
	}
	databases.release(c.db)
	return nil
}

// Ping implements driver.Pinger.
func (c *conn) Ping(ctx context.Context) error { return c.checkOpen() }

// IsValid implements driver.Validator.
func (c *conn) IsValid() bool { return c.checkOpen() == nil }

// ResetSession implements driver.SessionResetter. A transaction a caller opened with a BEGIN
// statement and never finished must not be inherited by whoever takes this connection from the
// pool next: it is rolled back.
func (c *conn) ResetSession(ctx context.Context) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx != nil {
		c.tx.release()
		c.tx = nil
	}
	return nil
}

// Catalog returns the JDBC-style catalog from the connection URL.
func (c *conn) Catalog() string { return c.parsed.Catalog }

// NamespaceID returns the namespace id from the connection URL.
func (c *conn) NamespaceID() string { return c.parsed.NamespaceID }

// Runtime returns the embedded runtime of the connection's own namespace (for advanced callers).
func (c *conn) Runtime() *embed.EmbeddedKdbRuntime { return c.ns.rt }

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return &stmt{conn: c, query: query}, nil
}

// Begin implements driver.Conn.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction. Read-committed (the default) reads each statement at the current
// head; LevelSnapshot and LevelRepeatableRead read every namespace as of one instant taken here.
// Other isolation levels are refused rather than silently weakened.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var snapshot bool
	switch isolationLevel(opts.Isolation) {
	case levelDefault, levelReadCommitted:
	case levelRepeatableRead, levelSnapshot:
		snapshot = true
	default:
		return nil, fmt.Errorf("kdb driver: isolation level %d is not supported (use read committed or snapshot)", opts.Isolation)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx != nil {
		return nil, errors.New("kdb driver: a transaction is already open on this connection")
	}
	c.tx = newConnTx(c.db, snapshot, opts.ReadOnly)
	return &txHandle{c: c, tx: c.tx}, nil
}

// isolation levels, mirroring database/sql's IsolationLevel values without importing
// database/sql into a driver.
const (
	levelDefault        = 0
	levelReadCommitted  = 2
	levelRepeatableRead = 4
	levelSnapshot       = 5
)

type isolationLevel int

// txHandle is the driver.Tx database/sql holds. It ends the transaction it was created for and
// nothing else: once that has ended (by a COMMIT statement, say) Commit and Rollback say so.
type txHandle struct {
	c  *conn
	tx *connTx
}

func (h *txHandle) Commit() error   { return h.c.endTx(h.tx, true) }
func (h *txHandle) Rollback() error { return h.c.endTx(h.tx, false) }

var errTxDone = errors.New("kdb driver: transaction has already been committed or rolled back")

// endTx commits or rolls back tx if it is still the connection's open transaction.
func (c *conn) endTx(tx *connTx, commit bool) error {
	c.mu.Lock()
	if c.tx != tx || tx == nil {
		c.mu.Unlock()
		return errTxDone
	}
	c.tx = nil
	c.mu.Unlock()
	defer tx.release()
	if !commit {
		return nil
	}
	return c.db.commit(tx.parts())
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	params, err := toParameters(args)
	if err != nil {
		return nil, err
	}
	stmt, err := sql.DefaultParser{}.Parse(strings.TrimSpace(query))
	if err != nil {
		return nil, err
	}
	if sql.IsTransactionControl(stmt) {
		return driver.RowsAffected(0), c.execControl(stmt)
	}
	switch stmt.(type) {
	case sql.StmtSelect:
		// Exec of a SELECT runs it and discards the rows, as database/sql callers expect.
		if _, _, err := c.query(stmt, query, params); err != nil {
			return nil, err
		}
		return driver.RowsAffected(0), nil
	case sql.StmtCreateTable, sql.StmtCreateIndex, sql.StmtDropIndex:
		return c.execDDL(stmt, query, params)
	default:
		return c.execDML(stmt, query, params)
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	params, err := toParameters(args)
	if err != nil {
		return nil, err
	}
	stmt, err := sql.DefaultParser{}.Parse(strings.TrimSpace(query))
	if err != nil {
		return nil, err
	}
	if _, ok := stmt.(sql.StmtSelect); !ok {
		return nil, fmt.Errorf("kdb driver: Query runs SELECT only; use Exec for %q", query)
	}
	cols, data, err := c.query(stmt, query, params)
	if err != nil {
		return nil, err
	}
	return &rows{columns: cols, data: data}, nil
}

// execControl runs BEGIN, COMMIT and ROLLBACK sent as statements.
func (c *conn) execControl(stmt sql.Statement) error {
	switch stmt.(type) {
	case sql.StmtBegin:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.tx != nil {
			if c.tx.hasWrites() {
				return errors.New("kdb driver: a transaction with pending writes is already open; COMMIT or ROLLBACK it first")
			}
			c.tx.release()
		}
		c.tx = newConnTx(c.db, false, false)
		return nil
	case sql.StmtCommit:
		c.mu.Lock()
		tx := c.tx
		c.mu.Unlock()
		if tx == nil {
			return nil // COMMIT outside a transaction commits nothing, as in PostgreSQL
		}
		return c.endTx(tx, true)
	default:
		c.mu.Lock()
		tx := c.tx
		c.mu.Unlock()
		if tx == nil {
			return nil
		}
		return c.endTx(tx, false)
	}
}

// route resolves the namespace a statement's table names - the rule the wire server applies
// (docs/kdb-cross-namespace-transactions-plan.md §7.1): a qualified name (bank.ledger) always
// names that namespace; an unqualified table names <catalog>/<table> when that namespace exists
// and the connection's own namespace otherwise.
func (c *conn) route(stmt sql.Statement, forWrite bool) (*namespace, error) {
	ref, named := sql.TargetTable(stmt)
	if !named {
		return c.ns, nil
	}
	if strings.Contains(ref.Name, ".") {
		id := strings.ReplaceAll(ref.Name, ".", "/")
		if id == c.ns.id {
			return c.ns, nil
		}
		if err := embed.ValidateNamespaceID(id); err != nil {
			return nil, err
		}
		return c.db.namespace(id, forWrite)
	}
	id := embed.CatalogFromNamespace(c.ns.id) + "/" + ref.Name
	if id == c.ns.id || embed.ValidateNamespaceID(id) != nil || !c.db.exists(id) {
		return c.ns, nil
	}
	return c.db.namespace(id, false)
}

func (c *conn) currentTx() *connTx {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tx
}

func (c *conn) query(stmt sql.Statement, query string, params []sql.Parameter) ([]string, [][]driver.Value, error) {
	ns, err := c.route(stmt, false)
	if err != nil {
		return nil, nil, err
	}
	head, err := c.readHead(ns)
	if err != nil {
		return nil, nil, err
	}
	result, err := ns.sqlEngine.Execute(query, sql.QueryContext{
		NamespaceID: ns.id, Schema: ns.schema(), AtCommit: &head, Parameters: params,
	})
	if err != nil {
		return nil, nil, err
	}
	cols := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		cols[i] = col.Name
	}
	data := make([][]driver.Value, len(result.Rows))
	for i, row := range result.Rows {
		values := make([]driver.Value, len(row.Values))
		for j, cell := range row.Values {
			values[j] = cellValue(cell)
		}
		data[i] = values
	}
	return cols, data, nil
}

// readHead is where a statement reads ns: the transaction's read point for a snapshot
// transaction, the live head otherwise.
func (c *conn) readHead(ns *namespace) (codec.Hash, error) {
	if tx := c.currentTx(); tx != nil && tx.snapshot {
		return tx.readHead(ns)
	}
	return ns.head()
}

func (c *conn) execDDL(stmt sql.Statement, query string, params []sql.Parameter) (driver.Result, error) {
	if c.parsed.ReadOnly {
		return nil, errors.New("connection is read-only")
	}
	ns, err := c.route(stmt, true)
	if err != nil {
		return nil, err
	}
	head, err := ns.head()
	if err != nil {
		return nil, err
	}
	result, err := ns.sqlEngine.Execute(query, sql.QueryContext{
		NamespaceID: ns.id, Schema: ns.schema(), AtCommit: &head, Parameters: params,
	})
	if err != nil {
		return nil, err
	}
	if result.AppliedSchema != nil {
		if err := ns.setSchemaChecked(*result.AppliedSchema); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(0), nil
}

func (c *conn) execDML(stmt sql.Statement, query string, params []sql.Parameter) (driver.Result, error) {
	if c.parsed.ReadOnly {
		return nil, errors.New("connection is read-only")
	}
	tx := c.currentTx()
	if tx != nil && tx.readOnly {
		return nil, errors.New("kdb driver: transaction is read-only")
	}
	ns, err := c.route(stmt, true)
	if err != nil {
		return nil, err
	}
	if err := ns.fenceErr(); err != nil {
		return nil, err
	}
	head, err := c.readHead(ns)
	if err != nil {
		return nil, err
	}
	result, err := ns.sqlEngine.ExecuteDML(query, sql.QueryContext{
		NamespaceID: ns.id, Schema: ns.schema(), AtCommit: &head, Parameters: params,
	})
	if err != nil {
		return nil, err
	}
	if tx != nil {
		// Buffered until the transaction ends. The part's base version is the head its first
		// write read at, so conflict detection covers everything the transaction read there.
		tx.add(ns, head, result.Operations)
		return driver.RowsAffected(result.RowsAffected), nil
	}
	if len(result.Operations) == 0 {
		return driver.RowsAffected(0), nil
	}
	err = commitOne(ns, document.Transaction{BaseVersion: head, Operations: result.Operations})
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(result.RowsAffected), nil
}

// cellValue converts a SQL cell to a driver.Value. JSON is returned as its text.
func cellValue(cell sql.Cell) driver.Value {
	switch v := cell.(type) {
	case sql.CellNull, nil:
		return nil
	case sql.CellString:
		return v.Value
	case sql.CellLong:
		return v.Value
	case sql.CellDouble:
		return v.Value
	case sql.CellBool:
		return v.Value
	case sql.CellJSON:
		return v.JSON
	default:
		return fmt.Sprint(v)
	}
}

// toParameters converts positional arguments. KDB-SQL parameters are positional (?), so named
// arguments are refused rather than silently bound by position.
func toParameters(args []driver.NamedValue) ([]sql.Parameter, error) {
	out := make([]sql.Parameter, len(args))
	for i, a := range args {
		if a.Name != "" {
			return nil, fmt.Errorf("kdb driver: named parameter %q is not supported; use positional ?", a.Name)
		}
		switch v := a.Value.(type) {
		case nil:
			out[i] = sql.ParamNull{}
		case int64:
			out[i] = sql.ParamInt{Value: v}
		case float64:
			out[i] = sql.ParamDouble{Value: v}
		case bool:
			out[i] = sql.ParamBool{Value: v}
		case string:
			out[i] = sql.ParamString{Value: v}
		case []byte:
			out[i] = sql.ParamString{Value: string(v)}
		case time.Time:
			out[i] = sql.ParamString{Value: v.UTC().Format(time.RFC3339Nano)}
		default:
			return nil, fmt.Errorf("kdb driver: unsupported parameter type %T", a.Value)
		}
	}
	return out, nil
}

type stmt struct {
	conn  *conn
	query string
}

var (
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), named(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), named(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
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
