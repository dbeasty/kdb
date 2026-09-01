package server

import (
	"math/rand/v2"
	"time"
)

// Conflict backoff bounds. The floor keeps a hint from ever meaning "immediately" - a client
// that retries with no pause at all is the herd this exists to break up - and the ceiling keeps
// a transient stall (one slow fsync, one memory-pressure pause) from telling every client to
// disappear for a second.
const (
	minConflictRetryMs = 2
	maxConflictRetryMs = 250
)

// conflictRetryAfterMs answers the question a conflict response has never been able to answer:
// not "you lost" but "try again in this many milliseconds".
//
// An optimistic-concurrency conflict means some other writer landed on the same document
// between the time this caller resolved its base version and the time its commit reached the
// front of the write gate. Two facts about that are the server's alone to know, and neither is
// available to the client:
//
//   - How many writers are queued (writeGate.queueDepth). This is the staleness window: a
//     caller's base version ages by one commit for every writer that drains ahead of it, so it
//     is also, directly, how likely an immediate retry is to lose again.
//   - How long one commit takes to drain (writeGate.meanServiceTime), measured on this node
//     under its actual durability mode rather than assumed.
//
// Their product is the expected time for the writers currently ahead to clear, which is the
// earliest a retry has a real chance. That value is then used as a *ceiling* on a uniform draw,
// not as the delay itself: this is full jitter (AWS's "Exponential Backoff and Jitter"), and the
// jitter is the load-bearing part. N clients told to wait the same 40ms collide again at 40ms;
// N clients each drawing independently from [2, 40] arrive spread out, which is what actually
// converts a re-colliding herd into a queue. Drawing server-side is deliberate - it is the only
// point that can see the whole herd, and it works for a client too simple to jitter for itself.
//
// Before any commit has completed, meanServiceTime is 0 and this returns the floor: a small,
// jittered, non-zero pause, which is still strictly better than the immediate retry that was
// the only option before.
func (s *KdbServerRuntime) conflictRetryAfterMs() int {
	depth := s.writeGate.queueDepth()
	if depth < 1 {
		depth = 1
	}
	drain := time.Duration(depth) * s.writeGate.meanServiceTime()
	ceiling := int(drain.Milliseconds())
	if ceiling > maxConflictRetryMs {
		ceiling = maxConflictRetryMs
	}
	if ceiling <= minConflictRetryMs {
		ceiling = minConflictRetryMs
	}
	return minConflictRetryMs + rand.IntN(ceiling-minConflictRetryMs+1)
}
