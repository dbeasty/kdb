package server

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ReplyTooLargeError reports a reply that cannot be sent because it would not fit one wire frame
// (or the result byte cap below it). Surfaced as RESOURCE_EXHAUSTED: the same request produces
// the same reply next time, so only a smaller request - a LIMIT, a narrower predicate, a page
// size - helps.
type ReplyTooLargeError struct {
	// Bytes is the reply's size, or a lower bound on it; Limit is the bound it exceeded.
	Bytes int
	Limit int
	// What names the reply ("result", "document", ...), Remedy says what to do about it.
	What   string
	Remedy string
}

func (e *ReplyTooLargeError) Error() string {
	what := e.What
	if what == "" {
		what = "reply"
	}
	msg := fmt.Sprintf("kdb: %s is %d bytes, over the %d-byte reply limit", what, e.Bytes, e.Limit)
	if e.Remedy != "" {
		msg += " - " + e.Remedy
	}
	return msg
}

// DefaultMaxResultBytes bounds the encoded size of the rows in one SQL reply. It sits 1 MiB under
// the 16 MiB frame limit, leaving room for the envelope, so a result that fits it always fits the
// frame. It only changes queries that already failed - a result over it used to be encoded,
// refused by the frame check, and dropped without a reply.
const DefaultMaxResultBytes = wire.DefaultMaxFrameBytes - 1<<20

// MaxResultBytes is the most encoded row bytes one SQL reply may carry. A larger unpaged result
// fails with RESOURCE_EXHAUSTED; a paged one ends its page early and continues on the next.
func (s *KdbServerRuntime) MaxResultBytes() int {
	if n := s.maxResultBytes.Load(); n > 0 {
		return int(min(n, int64(DefaultMaxResultBytes)))
	}
	return DefaultMaxResultBytes
}

// SetMaxResultBytes changes the bound; n <= 0 restores the default. It cannot be raised past
// DefaultMaxResultBytes, since a reply over that does not fit a frame.
func (s *KdbServerRuntime) SetMaxResultBytes(n int) {
	if n <= 0 || n > DefaultMaxResultBytes {
		n = DefaultMaxResultBytes
	}
	s.maxResultBytes.Store(int64(n))
}

// rowEncodedBytes is an upper bound on what one row adds to an encoded SqlResult: each cell as a
// JSON string, the separators, and the row's brackets.
func rowEncodedBytes(row []string) int {
	n := 2 + len(row) // [ ] and the commas, with one to spare for the separator before the row
	for _, cell := range row {
		n += wire.EncodedStringLen(cell)
	}
	return n
}

// boundResultRows applies the byte cap to a result's rows. It returns how many leading rows fit
// within limit, and their total encoded size.
func boundResultRows(rows [][]string, limit int) (fit int, bytes int) {
	for i, row := range rows {
		n := rowEncodedBytes(row)
		if bytes+n > limit {
			return i, bytes + n
		}
		bytes += n
	}
	return len(rows), bytes
}

// oversizedReply turns a reply that failed to encode into a typed error of the same reply type,
// so the client - waiting on that correlation id for that message type - gets an answer it can
// act on rather than silence until its own timeout. The frame-size case is RESOURCE_EXHAUSTED;
// any other encode failure is INTERNAL.
func oversizedReply(reply wire.Message, cause error) wire.Message {
	err := cause
	var tooLarge *wire.FrameTooLargeError
	if errors.As(cause, &tooLarge) {
		err = &ReplyTooLargeError{
			Bytes:  tooLarge.Length,
			Limit:  tooLarge.Max,
			What:   "reply",
			Remedy: "request less at once (a LIMIT, a narrower predicate, or a page size)",
		}
	}
	errMsg := err.Error()
	code, retryAfterMs := classifyError(err)
	h := header(reply.Header().CorrelationID, reply.Header().MessageType)
	switch r := reply.(type) {
	case wire.SqlResultMessage:
		return wire.SqlResultMessage{H: h, Namespace: r.Namespace, SessionID: r.SessionID, ReadOnly: r.ReadOnly,
			ResolvedCommitHex: r.ResolvedCommitHex, Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.DocumentGetResultMessage:
		return wire.DocumentGetResultMessage{H: h, Namespace: r.Namespace, DocID: r.DocID, CommitHex: r.CommitHex,
			Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.UpsertResultMessage:
		return wire.UpsertResultMessage{H: h, Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.SearchResultMessage:
		return wire.SearchResultMessage{H: h, Namespace: r.Namespace, ResolvedCommitHex: r.ResolvedCommitHex,
			Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.HistoryResultMessage:
		return wire.HistoryResultMessage{H: h, Namespace: r.Namespace, ResolvedHex: r.ResolvedHex,
			Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.RevertResultMessage:
		return wire.RevertResultMessage{H: h, Namespace: r.Namespace, Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.TxCommitMultiResultMessage:
		return wire.TxCommitMultiResultMessage{H: h, GroupID: r.GroupID, Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	case wire.LockResultMessage:
		return wire.LockResultMessage{H: h, Namespace: r.Namespace, SessionID: r.SessionID, DocID: r.DocID,
			Error: &errMsg, ErrorCode: &code}
	default:
		// No error-carrying shape of its own: answer as SQL_RESULT, as the panic backstop does
		// (internalErrorReply) - the client's request matcher still resolves the call, as an
		// error, instead of waiting out its timeout.
		return wire.SqlResultMessage{H: header(reply.Header().CorrelationID, wire.MsgSqlResult), ReadOnly: true,
			Error: &errMsg, ErrorCode: &code, RetryAfterMs: retryAfterMs}
	}
}

// DefaultMaxQueuedReplyBytes bounds the encoded reply bytes one connection may have waiting to be
// written (kdb-memory-cassandra-plan B6; Cassandra's native_transport_max_concurrent_requests_in_
// bytes, per connection). Past it the connection's reader stops taking new frames until replies
// drain, so a client that pipelines large reads faster than it reads the answers is paced by TCP
// backpressure instead of by server memory. MaxInFlightFrames alone bounds the count, not the
// size: 64 frames of 16 MiB replies is a gigabyte. 64 MiB is generous - four maximum-size frames,
// or thousands of ordinary ones - so normal workloads never meet it.
const DefaultMaxQueuedReplyBytes = 64 << 20

// replyBudget tracks one connection's reply bytes: those encoded and waiting to be written, plus
// an estimate for each dispatched frame whose reply is still being built.
//
// The estimate matters because a reply's size is only known once it is encoded, and a client
// pipelining large reads can hand the reader dozens of frames in the time the first reply takes
// to build - counting only encoded bytes, the reader would not notice the backlog until most of
// MaxInFlightFrames was already committed. Each dispatched frame is therefore charged the
// connection's running reply-size estimate up front (reserve), corrected to the real size once
// encoded (settle), and released when its Send returns.
type replyBudget struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queued int64
	limit  int64 // <= 0: unbounded
	// avg is a running estimate of this connection's encoded reply size (see settle).
	avg int64
	// waits counts how often the reader had to stop for replies to drain. Tests and diagnostics.
	waits int64
}

func newReplyBudget(limit int64) *replyBudget {
	b := &replyBudget{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// reserve charges a dispatched frame the expected size of its reply and returns that estimate,
// to be passed to settle.
func (b *replyBudget) reserve() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queued += b.avg
	return b.avg
}

// settle replaces a frame's reservation with its reply's real encoded size (0 for no reply).
func (b *replyBudget) settle(estimate int64, actual int) {
	b.mu.Lock()
	b.queued += int64(actual) - estimate
	if a := int64(actual); a > 0 {
		// Fast attack, slow decay: a large reply raises the estimate at once - under-reserving
		// is the failure this exists to prevent - while a run of small ones lowers it gradually.
		if a >= b.avg {
			b.avg = a
		} else {
			b.avg -= (b.avg - a) / 8
		}
	}
	b.wakeLocked()
	b.mu.Unlock()
}

func (b *replyBudget) charge(n int) {
	b.mu.Lock()
	b.queued += int64(n)
	b.mu.Unlock()
}

func (b *replyBudget) release(n int) {
	b.mu.Lock()
	b.queued -= int64(n)
	b.wakeLocked()
	b.mu.Unlock()
}

func (b *replyBudget) wakeLocked() {
	if b.limit <= 0 || b.queued <= b.limit {
		b.cond.Broadcast()
	}
}

// waitBelowLimit blocks while the connection's reply bytes exceed the limit. Every reservation is
// settled once its reply is encoded (or turns out to have none), and every charge is released once
// its Send returns - written, failed, or cut off by the write deadline - so the wait always ends.
func (b *replyBudget) waitBelowLimit() {
	if b.limit <= 0 {
		return
	}
	b.mu.Lock()
	if b.queued > b.limit {
		b.waits++
	}
	for b.queued > b.limit {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

func (b *replyBudget) snapshot() (queued, waits int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queued, b.waits
}

// MaxQueuedReplyBytes resolves the runtime's per-connection reply byte bound: 0 is the default,
// negative is unbounded.
func (s *KdbServerRuntime) MaxQueuedReplyBytes() int64 {
	switch n := s.MaxQueuedReplyBytesPerConn; {
	case n < 0:
		return 0
	case n == 0:
		return DefaultMaxQueuedReplyBytes
	default:
		return n
	}
}

// encodeReply encodes reply for conn: into a pooled buffer when conn's Send provably finishes
// with the slice before returning, otherwise into a buffer of its own. release must be called
// once Send has returned.
func encodeReply(c wire.Codec, conn FrameConn, reply wire.Message) (frame []byte, release func(), err error) {
	if dc, ok := c.(*wire.DefaultCodec); ok {
		if s, ok := conn.(core.SynchronousSender); ok && s.SendCompletesSynchronously() {
			return dc.EncodePooled(reply)
		}
	}
	frame, err = c.Encode(reply)
	return frame, noRelease, err
}

func noRelease() {}

// encodeReplyOrError encodes reply, falling back to its typed error form when it cannot be
// encoded. ok=false means not even the error could be encoded, which is logged.
func encodeReplyOrError(c wire.Codec, conn FrameConn, reply wire.Message) (frame []byte, release func(), ok bool) {
	frame, release, err := encodeReply(c, conn, reply)
	if err == nil {
		return frame, release, true
	}
	slog.Warn("kdb: reply could not be encoded; answering with an error instead",
		"message_type", reply.Header().MessageType.String(), "error", err)
	frame, release, err = encodeReply(c, conn, oversizedReply(reply, err))
	if err != nil {
		slog.Error("kdb: error reply could not be encoded", "message_type", reply.Header().MessageType.String(), "error", err)
		return nil, nil, false
	}
	return frame, release, true
}

// writeTimeoutMs maps ReplyWriteTimeout onto core.TransportConnectOptions.ReplyWriteTimeoutMs.
func (s *KdbServerRuntime) writeTimeoutMs() int64 {
	switch {
	case s.ReplyWriteTimeout < 0:
		return -1
	case s.ReplyWriteTimeout == 0:
		return 0
	default:
		return max(s.ReplyWriteTimeout.Milliseconds(), 1)
	}
}
