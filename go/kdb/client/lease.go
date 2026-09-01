package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/wire"
)

// ErrLockUnavailable is returned when a document is held by another session, or when a lease
// this client thought it held has already lapsed and cannot be renewed.
var ErrLockUnavailable = errors.New("kdb: document lock unavailable")

// LockError names the current holder when the server knew it.
type LockError struct {
	DocumentID string
	Holder     string
	Detail     string
}

func (e *LockError) Error() string {
	if e.Holder != "" {
		return fmt.Sprintf("kdb: document %s is locked by session %s", e.DocumentID, e.Holder)
	}
	return fmt.Sprintf("kdb: document %s lock unavailable: %s", e.DocumentID, e.Detail)
}

func (e *LockError) Unwrap() error { return ErrLockUnavailable }

// Lease is a granted document lock. Fence is the server's monotonic token for this grant: it is
// what the server validates a later commit against, and a Fence that changed across a Renew
// means the lease was lost and re-taken rather than extended - any work done in between was
// based on a document this client no longer owned.
type Lease struct {
	Namespace  string
	DocumentID string
	Fence      uint64
	ExpiresAt  time.Time
}

// Expired reports whether the lease deadline has passed on the local clock. Advisory only - the
// server's clock is the one that decides, and it is the commit-time fence check, not this, that
// makes the answer safe.
func (l Lease) Expired() bool {
	return !l.ExpiresAt.IsZero() && !time.Now().Before(l.ExpiresAt)
}

// AcquireLock takes an exclusive lease on docID for ttl, for work that spans round trips -
// holding a document while a user edits it, for instance. Pass ttl <= 0 to accept the server's
// default.
//
// A lease is not a substitute for the optimistic path: it expires, and a holder that stalls past
// its deadline will be refused at commit rather than silently overwriting whoever took the
// document next. Callers that only need "don't lose my update" should prefer CompareAndSwap.
func (c *Client) AcquireLock(ctx context.Context, ns string, docID string, ttl time.Duration) (Lease, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return Lease{}, err
	}
	msg := wire.LockAcquireMessage{
		H:         wire.Header{MessageType: wire.MsgLockAcquire, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		SessionID: st.sessionID,
		DocID:     docID,
		TTLMillis: int(ttl.Milliseconds()),
	}
	return c.lockRequest(ctx, ns, docID, msg)
}

// RenewLock extends a lease. The returned Lease carries the same Fence when the renewal simply
// extended what this client already held.
func (c *Client) RenewLock(ctx context.Context, ns string, docID string, ttl time.Duration) (Lease, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return Lease{}, err
	}
	msg := wire.LockRenewMessage{
		H:         wire.Header{MessageType: wire.MsgLockRenew, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		SessionID: st.sessionID,
		DocID:     docID,
		TTLMillis: int(ttl.Milliseconds()),
	}
	return c.lockRequest(ctx, ns, docID, msg)
}

// ReleaseLock drops a lease early. Releasing one this client no longer holds is not an error.
func (c *Client) ReleaseLock(ctx context.Context, ns string, docID string) error {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	msg := wire.LockReleaseMessage{
		H:         wire.Header{MessageType: wire.MsgLockRelease, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		SessionID: st.sessionID,
		DocID:     docID,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return err
	}
	result, ok := reply.(wire.LockResultMessage)
	if !ok {
		return fmt.Errorf("kdb: unexpected lock response %T", reply)
	}
	if result.Error != nil {
		return &LockError{DocumentID: docID, Detail: *result.Error}
	}
	return nil
}

func (c *Client) lockRequest(ctx context.Context, ns, docID string, msg wire.Message) (Lease, error) {
	reply, err := c.request(ctx, msg)
	if err != nil {
		return Lease{}, err
	}
	result, ok := reply.(wire.LockResultMessage)
	if !ok {
		return Lease{}, fmt.Errorf("kdb: unexpected lock response %T", reply)
	}
	if !result.Granted {
		le := &LockError{DocumentID: docID}
		if result.HolderSessionID != nil {
			le.Holder = *result.HolderSessionID
		}
		if result.Error != nil {
			le.Detail = *result.Error
		}
		return Lease{}, le
	}
	lease := Lease{Namespace: ns, DocumentID: docID, Fence: result.Fence}
	if result.ExpiresAtMillis != 0 {
		lease.ExpiresAt = time.UnixMilli(result.ExpiresAtMillis)
	}
	return lease, nil
}
