package org.antoined.core

import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch

/**
 * The spine: a bounded mutation channel drained by **one** coroutine, giving every
 * mutation in the runtime a single total order. Verbs handled on the loop: *apply*
 * (resolve handle via the identity map, version-guarded apply) and *route* (run the
 * registered [routers] — live-set maintenance). Reads never touch the spine;
 * handle/entry eviction runs in the identity map's grace watchers, off-loop.
 *
 * Backpressure is honest: a full channel suspends producers rather than buffering
 * unboundedly.
 */
internal class Spine(
    scope: CoroutineScope,
    private val identityMap: IdentityMap,
    capacity: Int = DEFAULT_CAPACITY,
    tapBuffer: Int = DEFAULT_TAP_BUFFER,
) {
    private class Envelope(
        val mutation: Mutation,
        val applied: CompletableDeferred<ApplyOutcome<Any>>? = null,
    )

    private val channel = Channel<Envelope>(capacity)
    private val tap = MutableSharedFlow<Mutation>(extraBufferCapacity = tapBuffer)

    /**
     * On-loop routing hooks, run for every mutation that *applied* (in loop order,
     * before the tap emission). Must honor the [Predicate] contract: pure and cheap.
     * A throwing router is logged and skipped — it never kills the loop.
     */
    val routers = CopyOnWriteArrayList<(Mutation, ApplyOutcome.Applied<Any>) -> Unit>()

    private val drainer = scope.launch { drain() }

    /**
     * The tap: a [kotlinx.coroutines.flow.SharedFlow] mirror of applied mutations,
     * in spine order. Rejected-stale and absorbed-echo mutations do not appear.
     */
    fun mutations(): Flow<Mutation> = tap.asSharedFlow()

    /** Enqueues [mutation]; suspends when the spine is saturated. */
    suspend fun enqueue(mutation: Mutation) {
        channel.send(Envelope(mutation))
    }

    /** Enqueues [mutation] and awaits its apply outcome (the write path's revert material). */
    suspend fun enqueueAwaitingApply(mutation: Mutation): ApplyOutcome<Any> {
        val applied = CompletableDeferred<ApplyOutcome<Any>>()
        channel.send(Envelope(mutation, applied))
        return applied.await()
    }

    fun close() {
        channel.close()
    }

    private suspend fun drain() {
        for (envelope in channel) {
            try {
                val outcome = identityMap.apply(envelope.mutation)
                envelope.applied?.complete(outcome)
                if (outcome is ApplyOutcome.Applied) {
                    for (router in routers) {
                        try {
                            router(envelope.mutation, outcome)
                        } catch (c: kotlinx.coroutines.CancellationException) {
                            throw c
                        } catch (t: Throwable) {
                            log.log(
                                System.Logger.Level.ERROR,
                                "spine router failed for ${envelope.mutation}; loop continues",
                                t,
                            )
                        }
                    }
                    tap.emit(envelope.mutation)
                }
            } catch (c: kotlinx.coroutines.CancellationException) {
                throw c
            } catch (t: Throwable) {
                envelope.applied?.completeExceptionally(t)
                log.log(
                    System.Logger.Level.ERROR,
                    "spine apply failed for ${envelope.mutation}; loop continues",
                    t,
                )
            }
        }
    }

    companion object {
        const val DEFAULT_CAPACITY = 4096
        const val DEFAULT_TAP_BUFFER = 1024
        private val log: System.Logger = System.getLogger(Spine::class.qualifiedName!!)
    }
}
