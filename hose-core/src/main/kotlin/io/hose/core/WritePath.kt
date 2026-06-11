package io.hose.core

import io.hose.core.IdentityMap.Companion.erasedToken
import io.hose.store.spi.EntityStore
import io.hose.store.spi.StoredEntity
import java.util.concurrent.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch

/** A persist that did not take: the write was either reverted or dropped as stale. */
data class WriteFailure(val mutation: Mutation, val cause: Throwable)

/** A LOCAL write rejected by the version guard: older than the state it tried to replace. */
class StaleLocalWriteException(message: String) : IllegalStateException(message)

/**
 * The write path: **route, then persist**.
 *
 * `upsert`/`delete` enqueue a LOCAL mutation and suspend only until the spine applies
 * it (instant optimistic routing — collectors see the write before the store does).
 * Persistence happens off-loop on [ioDispatcher], through a single ordered queue so
 * writes reach the store in spine order. A failed persist enqueues a [Mutation.Revert]
 * carrying the pre-write value captured at apply time, and surfaces a [WriteFailure].
 *
 * A LOCAL write the version guard rejects as stale is **not** persisted (it would
 * clobber newer store state); it surfaces as a [WriteFailure] with
 * [StaleLocalWriteException].
 */
internal class WritePath(
    scope: CoroutineScope,
    private val spine: Spine,
    private val store: EntityStore,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    persistCapacity: Int = Spine.DEFAULT_CAPACITY,
) {
    private class PersistTask(val mutation: Mutation, val revertTo: Any?, val revertOnFailure: Boolean)

    private val persistQueue = Channel<PersistTask>(persistCapacity)
    private val failures = MutableSharedFlow<WriteFailure>(extraBufferCapacity = 256)
    val writeFailures: SharedFlow<WriteFailure> = failures.asSharedFlow()

    private val persister = scope.launch(ioDispatcher) { drainPersists() }

    suspend fun upsert(type: EntityType<*, *, *>, entity: Any) {
        write(Mutation.Upsert(type, entity, Origin.LOCAL))
    }

    suspend fun delete(type: EntityType<*, *, *>, pk: Any, version: Any?) {
        write(Mutation.Delete(type, pk, version, Origin.LOCAL))
    }

    private suspend fun write(mutation: Mutation) {
        when (val outcome = spine.enqueueAwaitingApply(mutation)) {
            is ApplyOutcome.Applied ->
                persistQueue.send(PersistTask(mutation, outcome.previous, revertOnFailure = true))
            is ApplyOutcome.AbsorbedEcho ->
                // value already current; persist for store durability, nothing to revert
                persistQueue.send(PersistTask(mutation, null, revertOnFailure = false))
            is ApplyOutcome.RejectedStale ->
                failures.emit(
                    WriteFailure(
                        mutation,
                        StaleLocalWriteException("local write is older than current state; dropped, not persisted"),
                    ),
                )
        }
    }

    private suspend fun drainPersists() {
        for (task in persistQueue) {
            try {
                persist(task.mutation)
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                if (task.revertOnFailure) {
                    val type = task.mutation.type
                    val pk = when (val m = task.mutation) {
                        is Mutation.Upsert -> type.erasedToken().pk(m.entity)
                        is Mutation.Delete -> m.pk
                        is Mutation.Revert -> m.pk
                    }
                    spine.enqueue(Mutation.Revert(type, pk, task.revertTo, t))
                }
                failures.emit(WriteFailure(task.mutation, t))
            }
        }
    }

    private fun persist(mutation: Mutation) {
        val type = mutation.type.erasedToken()
        when (mutation) {
            is Mutation.Upsert -> store.upsert(
                StoredEntity(
                    type.name,
                    type.encodeKey(type.pk(mutation.entity)),
                    type.encodeVersion(type.version(mutation.entity)),
                    mutation.entity,
                ),
            )
            is Mutation.Delete -> store.delete(
                type.name,
                type.encodeKey(mutation.pk),
                mutation.version?.let { type.encodeVersion(it) },
            )
            is Mutation.Revert -> error("reverts are never persisted")
        }
    }

    fun close() {
        persistQueue.close()
    }
}
