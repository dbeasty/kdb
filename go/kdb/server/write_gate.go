package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// writeGate bounds how many callers may be waiting to commit at once, and gives each waiter a
// deadline: "start only what we can finish" (kdb-spec-layer13 Component 49 §6.2), applied to
// time rather than memory. It replaces a bare sync.Mutex as commitWith's serialization primitive
// with the same single-active-commit guarantee, plus two additional, explicit outcomes a bare
// mutex cannot express:
//
//  1. The queue is already at capacity -> reject immediately with *BusyError, rather than let an
//     unbounded number of goroutines pile up blocked on Lock() with no way to tell a healthy
//     "almost my turn" wait from an unhealthy "the server fell behind an hour ago" one.
//  2. A caller has been waiting long enough that its own deadline has passed -> reject with
//     *DeadlineExceededError instead of eventually running the commit anyway for a client that
//     has, in all likelihood, already given up and moved on.
//
// Both are populated from the exact same admission decision a client actually needs (see
// BusyError/DeadlineExceededError): whether and when to retry.
type writeGate struct {
	queued  chan struct{} // bounds how many callers may be waiting at all
	running chan struct{} // capacity 1: the actual single-active-commit serialization
	// serviceNanos is an exponentially-weighted mean of how long one commit holds the running
	// slot. It exists to put an honest number behind the retry-after hint the server sends a
	// client whose transaction lost a race (see KdbServerRuntime.conflictRetryAfterMs): "wait
	// for the writers ahead of you to drain" is only actionable if the server can say how long
	// draining one writer actually takes on this node, under this durability mode, right now.
	// Written only by the goroutine releasing the gate - i.e. serialized with itself - so a
	// plain load/store pair needs no further synchronization.
	serviceNanos atomic.Int64
}

// serviceEWMAShift sets the smoothing: each sample moves the mean by 1/8 of the gap. Slow
// enough that one unusually long fsync does not spike every client's backoff, fast enough to
// track a real change in durability mode or disk behavior within a few dozen commits.
const serviceEWMAShift = 3

func newWriteGate(maxQueued int) *writeGate {
	if maxQueued <= 0 {
		maxQueued = 64
	}
	return &writeGate{
		queued:  make(chan struct{}, maxQueued),
		running: make(chan struct{}, 1),
	}
}

// acquire admits one caller to run its commit, respecting ctx's deadline while queued. On
// success, the returned release func must be called exactly once (typically via defer)  once the
// commit is done, to free the slot for the next queued caller.
func (g *writeGate) acquire(ctx context.Context) (release func(), err error) {
	select {
	case g.queued <- struct{}{}:
	default:
		return nil, &BusyError{RetryAfterMs: 50, Reason: "write queue is full"}
	}
	defer func() { <-g.queued }()

	select {
	case g.running <- struct{}{}:
		start := time.Now()
		return func() {
			g.observeService(time.Since(start))
			<-g.running
		}, nil
	case <-ctx.Done():
		return nil, &DeadlineExceededError{Reason: "timed out waiting for an earlier write to finish"}
	}
}

// observeService folds one commit's gate occupancy into the running mean.
func (g *writeGate) observeService(d time.Duration) {
	sample := d.Nanoseconds()
	if sample < 0 {
		return
	}
	prev := g.serviceNanos.Load()
	if prev == 0 {
		g.serviceNanos.Store(sample)
		return
	}
	g.serviceNanos.Store(prev + (sample-prev)>>serviceEWMAShift)
}

// meanServiceTime returns how long one commit currently takes to pass through the gate, or 0
// before any commit has completed.
func (g *writeGate) meanServiceTime() time.Duration {
	return time.Duration(g.serviceNanos.Load())
}

// queueDepth returns how many callers hold a queued slot right now - waiters plus the one
// running. This is the number that says how stale a queued writer's base version is about to
// be: every commit that drains ahead of it advances the head it was anchored on.
func (g *writeGate) queueDepth() int { return len(g.queued) }

// quiesced reports whether no caller currently holds a queued or running slot - i.e. every
// admitted write has finished. Only meaningful once new admissions are being rejected (see
// KdbServerRuntime.BeginDraining), otherwise a new caller can arrive right after the check.
func (g *writeGate) quiesced() bool {
	return len(g.queued) == 0 && len(g.running) == 0
}

// BusyError means the server cannot admit this operation *right now*, but the same request is
// expected to succeed later - retry after RetryAfterMs (kdb-spec-layer13 Component 51 §8.1's
// BUSY code). Distinct from DeadlineExceededError: this is about the server's current state
// (a full queue, memory pressure), not about how long the specific caller was willing to wait.
type BusyError struct {
	RetryAfterMs int
	Reason       string
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("kdb server: busy (retry after %dms): %s", e.RetryAfterMs, e.Reason)
}

// RetryAfter returns how long a caller should wait before retrying.
func (e *BusyError) RetryAfter() time.Duration {
	return time.Duration(e.RetryAfterMs) * time.Millisecond
}

// DeadlineExceededError means the caller's own deadline passed before its operation could run -
// unlike BusyError, retrying without raising the deadline is unlikely to help (kdb-spec-layer13
// Component 51 §8.1's DEADLINE_EXCEEDED code). Safe to retry (idempotently, since commit ids make
// retries idempotent - see transaction.Engine's findExistingCommit) with a longer deadline.
type DeadlineExceededError struct {
	Reason string
}

func (e *DeadlineExceededError) Error() string {
	return "kdb server: deadline exceeded: " + e.Reason
}

// UnavailableError means the server is shutting down (an orderly abort, kdb-spec-layer13
// Component 50, or a plain process shutdown) and cannot accept new work at all right now.
// Retryable after reconnecting to a (likely different, likely just-restarted) server instance.
type UnavailableError struct {
	Reason string
}

func (e *UnavailableError) Error() string {
	return "kdb server: unavailable: " + e.Reason
}
