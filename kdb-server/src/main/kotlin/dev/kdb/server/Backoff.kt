package dev.kdb.server

import kotlin.random.Random

// Conflict backoff bounds - mirrors Go's server/backoff.go exactly. The floor keeps a hint from
// ever meaning "immediately" (a client that retries with no pause at all is the herd this exists
// to break up), and the ceiling keeps a transient stall (one slow fsync, one memory-pressure
// pause) from telling every client to disappear for a second.
private const val MIN_CONFLICT_RETRY_MS = 2
private const val MAX_CONFLICT_RETRY_MS = 250

/**
 * Answers the question a conflict response has never been able to answer: not "you lost" but
 * "try again in this many milliseconds".
 *
 * An optimistic-concurrency conflict means some other writer landed on the same document between
 * the time this caller resolved its base version and the time its commit reached the front of
 * [KdbServerRuntime.writeCoordinatorFor] (Component 74: one gate per namespace, so a busy namespace never inflates another's hint). Two facts about that are the server's alone to know, and
 * neither is available to the client:
 *
 * - How many writers are queued ([WriteCoordinator.queueDepth]). This is the staleness window: a
 *   caller's base version ages by one commit for every writer that drains ahead of it, so it is
 *   also, directly, how likely an immediate retry is to lose again.
 * - How long one commit takes to drain ([WriteCoordinator.meanServiceTime]), measured on this
 *   node rather than assumed.
 *
 * Their product is the expected time for the writers currently ahead to clear, used as a
 * *ceiling* on a uniform draw, not as the delay itself: this is full jitter (AWS's "Exponential
 * Backoff and Jitter"), and the jitter is the load-bearing part - N clients told to wait the
 * same 40ms collide again at 40ms, N clients each drawing independently from `[2, 40]` arrive
 * spread out, which is what actually converts a re-colliding herd into a queue.
 *
 * Before any commit has completed, [WriteCoordinator.meanServiceTime] is zero and this returns
 * the floor: a small, jittered, non-zero pause - still strictly better than the immediate retry
 * that was the only option before this existed.
 */
public fun KdbServerRuntime.conflictRetryAfterMs(namespaceId: String = runtime.defaultNamespace): Int {
    val coordinator = writeCoordinatorFor(namespaceId)
    val depth = coordinator.queueDepth().coerceAtLeast(1)
    val drainMs = depth * coordinator.meanServiceTime().inWholeMilliseconds
    var ceiling = drainMs.coerceAtMost(MAX_CONFLICT_RETRY_MS.toLong()).toInt()
    if (ceiling <= MIN_CONFLICT_RETRY_MS) ceiling = MIN_CONFLICT_RETRY_MS
    return MIN_CONFLICT_RETRY_MS + Random.nextInt(ceiling - MIN_CONFLICT_RETRY_MS + 1)
}
