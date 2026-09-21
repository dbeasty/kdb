package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/wire"
)

// ErrTxDone is returned by any call on a Tx after Commit or Rollback.
var ErrTxDone = errors.New("kdb: transaction has already been committed or rolled back")

// TxOptions configures a SQL transaction.
type TxOptions struct {
	// Snapshot makes every read in the transaction see every namespace as of one instant, taken
	// at Begin. The default, read-committed, reads each statement at the current head.
	Snapshot bool
}

// Tx is a SQL transaction: statements buffer their writes until Commit, which applies all of them
// or none. A statement whose table names another namespace - qualified (`bank.ledger`), or an
// unqualified table that is the name of another namespace in the same catalog - writes there, and
// Commit then commits every namespace the transaction touched atomically.
//
// A Tx runs on a server session of its own, so it neither sees nor commits work done through the
// Client's other calls. It is not safe for concurrent use; run one statement at a time.
type Tx struct {
	c         *Client
	ns        string
	sessionID string
	poolKey   string

	mu   sync.Mutex
	done bool
}

// Begin starts a read-committed SQL transaction on namespace ns.
func (c *Client) Begin(ctx context.Context, ns string) (*Tx, error) {
	return c.BeginTx(ctx, ns, TxOptions{})
}

// BeginTx starts a SQL transaction on namespace ns - the namespace unqualified tables refer to.
func (c *Client) BeginTx(ctx context.Context, ns string, opts TxOptions) (*Tx, error) {
	consistency := "READ_COMMITTED"
	if opts.Snapshot {
		consistency = "SNAPSHOT"
	}
	key := ns + "\x00" + consistency
	sessionID := c.takeIdleTxSession(key)
	if sessionID == "" {
		var err error
		if sessionID, _, err = c.beginSession(ctx, ns, consistency); err != nil {
			return nil, err
		}
	}
	tx := &Tx{c: c, ns: ns, sessionID: sessionID, poolKey: key}
	// BEGIN, not merely a fresh session: a reused session must start its transaction at the
	// current head, and a snapshot transaction takes its multi-namespace snapshot here.
	if _, err := c.execSqlOnSession(ctx, ns, sessionID, "BEGIN", nil); err != nil {
		return nil, err
	}
	return tx, nil
}

func (c *Client) takeIdleTxSession(key string) string {
	c.nsMu.Lock()
	defer c.nsMu.Unlock()
	idle := c.idleTxSessions[key]
	if len(idle) == 0 {
		return ""
	}
	id := idle[len(idle)-1]
	c.idleTxSessions[key] = idle[:len(idle)-1]
	return id
}

func (c *Client) releaseTxSession(key, sessionID string) {
	c.nsMu.Lock()
	defer c.nsMu.Unlock()
	if c.idleTxSessions == nil {
		c.idleTxSessions = make(map[string][]string)
	}
	c.idleTxSessions[key] = append(c.idleTxSessions[key], sessionID)
}

func (t *Tx) checkOpen() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return ErrTxDone
	}
	return nil
}

// finish marks the transaction over and returns its session for reuse. Only a session whose
// transaction definitely ended - committed, refused, or rolled back - goes back: one lost to a
// transport error may still hold buffered writes.
func (t *Tx) finish(reusable bool) {
	t.mu.Lock()
	t.done = true
	t.mu.Unlock()
	if reusable {
		t.c.releaseTxSession(t.poolKey, t.sessionID)
	}
}

// Exec runs one statement - INSERT, UPDATE, DELETE, or DDL - and returns the number of rows it
// affected. DML is buffered until Commit; DDL applies immediately.
func (t *Tx) Exec(ctx context.Context, sqlText string, args ...any) (int, error) {
	if err := t.checkOpen(); err != nil {
		return 0, err
	}
	r, err := t.c.execSqlOnSession(ctx, t.ns, t.sessionID, sqlText, args)
	if err != nil {
		return 0, err
	}
	return r.rowsAffected, nil
}

// Query runs a SELECT inside the transaction and decodes its rows into dest (see Client.Query).
func (t *Tx) Query(ctx context.Context, sqlText string, dest any, args ...any) error {
	cols, rows, err := t.QueryRaw(ctx, sqlText, args...)
	if err != nil {
		return err
	}
	return decodeRows(cols, rows, dest)
}

// QueryRaw runs a SELECT inside the transaction and returns its raw columns and rows.
func (t *Tx) QueryRaw(ctx context.Context, sqlText string, args ...any) ([]string, [][]string, error) {
	if err := t.checkOpen(); err != nil {
		return nil, nil, err
	}
	r, err := t.c.execSqlOnSession(ctx, t.ns, t.sessionID, sqlText, args)
	if err != nil {
		return nil, nil, err
	}
	return r.Columns, r.Rows, nil
}

// Commit applies every buffered write, in every namespace the transaction touched, or none. It
// returns the new commit in the transaction's own namespace (its current head, if the
// transaction wrote only elsewhere).
//
// A conflict satisfies errors.Is(err, ErrConflict); when the refusing namespace is not the
// transaction's own, the error is a *CrossNamespaceError naming it. Either way nothing was written
// anywhere, and the transaction is over.
func (t *Tx) Commit(ctx context.Context) (string, error) {
	if err := t.checkOpen(); err != nil {
		return "", err
	}
	reply, err := t.c.request(ctx, wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: t.c.nextCorrelation()},
		Namespace: t.ns,
		SessionID: t.sessionID,
	})
	if err != nil {
		t.finish(false)
		return "", err
	}
	t.finish(true)
	switch r := reply.(type) {
	case wire.ConflictReportMessage:
		conflict := decodeConflictError(r.ReportBytes, r.RetryAfterMs)
		if r.Namespace != "" && r.Namespace != t.ns {
			return "", &CrossNamespaceError{Namespace: r.Namespace, Err: conflict}
		}
		return "", conflict
	case wire.SqlResultMessage:
		if r.Error != nil {
			return "", classifiedError(*r.Error, r.ErrorCode, r.RetryAfterMs)
		}
		t.c.advanceHead(t.ns, r.ResolvedCommitHex)
		return r.ResolvedCommitHex, nil
	default:
		return "", fmt.Errorf("kdb: unexpected commit response %T", reply)
	}
}

// Rollback discards every buffered write. Calling it after Commit is a no-op returning ErrTxDone,
// so `defer tx.Rollback(ctx)` is safe.
func (t *Tx) Rollback(ctx context.Context) error {
	if err := t.checkOpen(); err != nil {
		return err
	}
	reply, err := t.c.request(ctx, wire.TxRollbackMessage{
		H:         wire.Header{MessageType: wire.MsgTxRollback, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: t.c.nextCorrelation()},
		Namespace: t.ns,
		SessionID: t.sessionID,
	})
	if err != nil {
		t.finish(false)
		return err
	}
	t.finish(true)
	if r, ok := reply.(wire.SqlResultMessage); ok && r.Error != nil {
		return classifiedError(*r.Error, r.ErrorCode, r.RetryAfterMs)
	}
	return nil
}
