package server

import (
	"context"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// DefaultLeaseTTL is the lease length a LockAcquire gets when it names none. Short enough that a
// client that dies mid-edit frees the document on a human timescale, long enough that a client
// doing real work between renewals is not fighting the clock.
const DefaultLeaseTTL = 30 * time.Second

// MaxLeaseTTL caps what a client may ask for. Without a cap the whole point of leases is
// negotiable away by the one party that benefits from holding forever.
const MaxLeaseTTL = 5 * time.Minute

// handleLockAcquire grants sess an exclusive, expiring, fenced lease on one document.
func (h *sqlWireConnHandler) handleLockAcquire(msg wire.LockAcquireMessage) wire.Message {
	sess, docID, fail := h.lockPreamble(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID)
	if fail != nil {
		return fail
	}
	lease, err := h.runtime.DocumentLocks.TryAcquireLease(
		sess.NamespaceID, docID, sess.ID.Value, clampLeaseTTL(msg.TTLMillis),
	)
	if err != nil {
		return lockDenied(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID, err)
	}
	sess.TrackLease(lease)
	return lockGranted(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID, lease.Fence, lease.ExpiresAt)
}

// handleLockRenew extends an existing lease, keeping its fence. A renew that arrives after the
// lease already lapsed fails rather than silently re-acquiring: the client has to learn it lost
// the document, because anything it did while believing otherwise may now be stale.
func (h *sqlWireConnHandler) handleLockRenew(msg wire.LockRenewMessage) wire.Message {
	sess, docID, fail := h.lockPreamble(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID)
	if fail != nil {
		return fail
	}
	lease, err := h.runtime.DocumentLocks.Renew(
		sess.NamespaceID, docID, sess.ID.Value, clampLeaseTTL(msg.TTLMillis),
	)
	if err != nil {
		sess.UntrackLease(docID)
		return lockDenied(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID, err)
	}
	sess.TrackLease(lease)
	return lockGranted(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID, lease.Fence, lease.ExpiresAt)
}

// handleLockRelease drops a lease early. Releasing something the session does not hold is not an
// error - the caller's intent (not to hold it) is satisfied either way, and a client whose lease
// just expired should not have to distinguish the two.
func (h *sqlWireConnHandler) handleLockRelease(msg wire.LockReleaseMessage) wire.Message {
	sess, docID, fail := h.lockPreamble(msg.H.CorrelationID, msg.Namespace, msg.SessionID, msg.DocID)
	if fail != nil {
		return fail
	}
	h.runtime.DocumentLocks.Release(sess.NamespaceID, docID, sess.ID.Value)
	sess.UntrackLease(docID)
	return wire.LockResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgLockResult),
		Namespace: msg.Namespace,
		SessionID: msg.SessionID,
		DocID:     msg.DocID,
		Granted:   false,
	}
}

// lockPreamble resolves and authorizes a lock request, returning a ready-to-send failure message
// instead of the session when anything is wrong. Locking a document is a write-side capability -
// it exists to exclude other writers - so it is authorized as one rather than as a read.
func (h *sqlWireConnHandler) lockPreamble(
	correlationID int, namespace, sessionID, docIDHex string,
) (*KdbSession, codec.UUID, wire.Message) {
	if !h.isAuthenticated() {
		return nil, codec.UUID{}, lockError(correlationID, namespace, sessionID, docIDHex, "not authenticated", nil)
	}
	sess, ok := h.sessions.Get(sessionID)
	if !ok {
		return nil, codec.UUID{}, lockError(correlationID, namespace, sessionID, docIDHex, "unknown session: "+sessionID, nil)
	}
	action := auth.TxCommitAction{Namespace: sess.NamespaceID}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), sess.Principal, action); err != nil {
		return nil, codec.UUID{}, lockError(correlationID, namespace, sessionID, docIDHex, err.Error(), nil)
	}
	docID, err := codec.UUIDFromString(docIDHex)
	if err != nil {
		return nil, codec.UUID{}, lockError(correlationID, namespace, sessionID, docIDHex, "invalid document id: "+docIDHex, nil)
	}
	return sess, docID, nil
}

// clampLeaseTTL turns a client-requested millisecond TTL into a duration within policy. Zero (or
// negative) means "server default"; anything above MaxLeaseTTL is capped rather than refused, so
// an over-eager client still gets a working lease instead of an error it has no way to act on.
func clampLeaseTTL(ttlMillis int) time.Duration {
	if ttlMillis <= 0 {
		return DefaultLeaseTTL
	}
	ttl := time.Duration(ttlMillis) * time.Millisecond
	if ttl > MaxLeaseTTL {
		return MaxLeaseTTL
	}
	return ttl
}

func lockGranted(correlationID int, namespace, sessionID, docID string, fence uint64, expiresAt time.Time) wire.Message {
	var expiresMillis int64
	if !expiresAt.IsZero() {
		expiresMillis = expiresAt.UnixMilli()
	}
	return wire.LockResultMessage{
		H:               header(correlationID, wire.MsgLockResult),
		Namespace:       namespace,
		SessionID:       sessionID,
		DocID:           docID,
		Granted:         true,
		Fence:           fence,
		ExpiresAtMillis: expiresMillis,
	}
}

// lockDenied reports a refusal, naming the current holder when the error knows it - "locked by
// session X" is actionable in a way that "locked" is not.
func lockDenied(correlationID int, namespace, sessionID, docID string, err error) wire.Message {
	var holder *string
	var locked *kdberr.DocumentLockedError
	if asError(err, &locked) && locked.Owner != "" {
		h := locked.Owner
		holder = &h
	}
	return lockError(correlationID, namespace, sessionID, docID, err.Error(), holder)
}

func lockError(correlationID int, namespace, sessionID, docID, message string, holder *string) wire.Message {
	return wire.LockResultMessage{
		H:               header(correlationID, wire.MsgLockResult),
		Namespace:       namespace,
		SessionID:       sessionID,
		DocID:           docID,
		Granted:         false,
		HolderSessionID: holder,
		Error:           &message,
	}
}
