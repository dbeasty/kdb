package server

import (
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// opClassForMessage maps a wire message type to the admission class its work belongs to, from the
// frame header alone. ok=false means "not classifiable, or not shedding-eligible" - control
// traffic (handshake, session begin, rollback) is never shed, because refusing the messages a
// client needs to establish or unwind a session turns a recoverable overload into a stuck one.
//
// MsgSqlExec is classified as ClassScan even though it may carry an INSERT: which one it is
// cannot be known without decoding the body, which is exactly the cost this check exists to
// avoid paying. It makes no practical difference - ClassScan and ClassWrite are admitted and shed
// in precisely the same zones - and the alternative, decoding first, would defeat the point.
func opClassForMessage(t wire.MessageType) (OpClass, bool) {
	switch t {
	case wire.MsgDocumentGet:
		return ClassPointRead, true
	case wire.MsgSqlExec:
		return ClassScan, true
	case wire.MsgTxCommit, wire.MsgUpsert, wire.MsgTransactionReplay:
		return ClassWrite, true
	case wire.MsgCommitPush, wire.MsgDeltaCommit:
		return ClassReplication, true
	default:
		return 0, false
	}
}

// frameAdmitter returns the core.FrameAdmitter this runtime uses to shed work at the frame
// boundary - kdb-spec-layer13 Component 48 §5.4, "admit early".
//
// Scope, stated precisely because it is narrower than §5.4's full description: this consults the
// *zone policy* only. It does not acquire the operation's memory grant, which is still taken in
// runTransaction, where the grant's lifetime is bounded by the operation it belongs to. Moving
// grant ownership into the transport would mean the transport had to know when each request
// finished in order to release it, which is not a relationship the current layering expresses -
// and a grant that leaks because the release path was missed is worse than one taken slightly
// later.
//
// What it does buy is the cost asymmetry §2.7 identifies: when the server has already decided it
// is shedding a class of work, a request of that class is refused after a 12-byte header read,
// rather than after its body has been read off the socket, assembled into a frame, and
// JSON-decoded. Under exactly the load where shedding matters, that is the difference between
// refusals being nearly free and refusals being the most expensive thing the server does.
//
// A request is only ever shed when a typed response can be built for it. If the message type has
// no result shape this can populate, the frame is admitted and left to the normal path, which
// will produce a proper error of its own - dropping a request with no reply would leave the
// client blocked until its own timeout, which is precisely the "connection just stops
// responding" failure mode Component 51 exists to eliminate.
func (s *KdbServerRuntime) frameAdmitter(codec wire.Codec) core.FrameAdmitter {
	return func(h wire.Header) ([]byte, error) {
		class, ok := opClassForMessage(h.MessageType)
		if !ok {
			return nil, nil
		}
		zone := s.memGuard.CurrentZone()
		if admitInZone(zone, class) {
			return nil, nil
		}
		shed := &MemoryPressureError{Zone: zone, Class: class, RetryAfterMs: retryAfterMsForZone(zone)}
		reply, ok := rejectionMessage(h, shed)
		if !ok {
			return nil, nil // no typed reply available - admit rather than drop silently
		}
		frame, err := codec.Encode(reply)
		if err != nil {
			return nil, nil // could not build the reply - same reasoning
		}
		if s.admission != nil {
			s.admission.stats.DeniedZone[classIndex(class)].Add(1)
		}
		return frame, shed
	}
}

// rejectionMessage builds the typed refusal for a shed request. Namespace and session id are left
// empty: they live in the body, which by design has not been read.
func rejectionMessage(h wire.Header, err error) (wire.Message, bool) {
	switch h.MessageType {
	case wire.MsgSqlExec, wire.MsgTxCommit, wire.MsgTransactionReplay:
		return sqlResultErrorClassified(h.CorrelationID, "", "", err), true
	case wire.MsgUpsert:
		errMsg := err.Error()
		code, retryAfterMs := classifyError(err)
		return wire.UpsertResultMessage{
			H:            header(h.CorrelationID, wire.MsgUpsertResult),
			Error:        &errMsg,
			ErrorCode:    &code,
			RetryAfterMs: retryAfterMs,
		}, true
	default:
		return nil, false
	}
}
