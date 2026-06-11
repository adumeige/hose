package io.hose.core

import app.cash.turbine.test
import app.cash.turbine.turbineScope
import io.hose.store.spi.StoreQuery
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest

private val lsTodoType = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

private val lsBase: Instant = Instant.parse("2026-06-11T10:00:00Z")

private fun openTodos(): StoreQuery =
    StoreQuery("todo", listOf(StoreQuery.FieldComparison("done", StoreQuery.Op.EQ, false)))

private class Fixture(scope: CoroutineScope, snapshot: Collection<Todo> = emptyList()) {
    val snapshotLoads = AtomicInteger()
    val identityMap = IdentityMap(scope)
    val spine = Spine(scope, identityMap)
    val liveSets = LiveSets(
        scope = scope,
        spine = spine,
        identityMap = identityMap,
        graceMillis = 30_000,
        snapshotLoader = { _ ->
            snapshotLoads.incrementAndGet()
            snapshot
        },
    )

    suspend fun upsert(todo: Todo, origin: Origin = Origin.LOCAL) {
        spine.enqueue(Mutation.Upsert(lsTodoType, todo, origin))
    }

    suspend fun delete(pk: Long, version: Instant?) {
        spine.enqueue(Mutation.Delete(lsTodoType, pk, version, Origin.LOCAL))
    }
}

class LiveSetsTest {

    @Test
    fun `late subscriber gets snapshot first, then deltas`() = runTest {
        val fx = Fixture(
            backgroundScope,
            snapshot = listOf(
                Todo(1, "open", false, lsBase),
                Todo(2, "already done", true, lsBase),
            ),
        )
        turbineScope {
            val early = fx.liveSets.events(lsTodoType, openTodos()).testIn(backgroundScope)
            // the very first subscriber races the snapshot load: empty snapshot, then the merge delta
            assertEquals(SetEvent.Snapshot(emptySet()), early.awaitItem())
            assertEquals(SetEvent.Added<Any>(1L), early.awaitItem())

            fx.upsert(Todo(3, "new open", false, lsBase.plusSeconds(1)))
            assertEquals(SetEvent.Added<Any>(3L), early.awaitItem())

            // the late subscriber: membership arrives as one snapshot, not replayed deltas
            val late = fx.liveSets.events(lsTodoType, openTodos()).testIn(backgroundScope)
            assertEquals(SetEvent.Snapshot(setOf<Any>(1L, 3L)), late.awaitItem())

            // subsequent deltas reach both, in the same order
            fx.upsert(Todo(1, "open", true, lsBase.plusSeconds(2)))
            assertEquals(SetEvent.Removed<Any>(1L), early.awaitItem())
            assertEquals(SetEvent.Removed<Any>(1L), late.awaitItem())

            early.cancelAndIgnoreRemainingEvents()
            late.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `membership transitions - add, no-op, remove, delete`() = runTest {
        val fx = Fixture(backgroundScope)
        fx.liveSets.events(lsTodoType, openTodos()).test {
            assertEquals(SetEvent.Snapshot(emptySet()), awaitItem())

            // newly matching: add
            fx.upsert(Todo(1, "a", false, lsBase))
            assertEquals(SetEvent.Added<Any>(1L), awaitItem())

            // present & still matching: no-op even though the entity changed
            fx.upsert(Todo(1, "a renamed", false, lsBase.plusSeconds(1)))
            expectNoEvents()

            // present & no longer matching: remove
            fx.upsert(Todo(1, "a done", true, lsBase.plusSeconds(2)))
            assertEquals(SetEvent.Removed<Any>(1L), awaitItem())

            // not present & not matching: no-op
            fx.upsert(Todo(2, "b done", true, lsBase))
            expectNoEvents()

            // delete of a member: remove
            fx.upsert(Todo(3, "c", false, lsBase))
            assertEquals(SetEvent.Added<Any>(3L), awaitItem())
            fx.delete(3L, lsBase.plusSeconds(5))
            assertEquals(SetEvent.Removed<Any>(3L), awaitItem())

            // delete of a non-member: no-op
            fx.delete(2L, lsBase.plusSeconds(5))
            expectNoEvents()

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `membership-neutral entity mutation emits on the handle but not the set`() = runTest {
        val fx = Fixture(backgroundScope)
        val first = Todo(1, "a", false, lsBase)

        fx.liveSets.events(lsTodoType, openTodos()).test {
            assertEquals(SetEvent.Snapshot(emptySet()), awaitItem())
            fx.upsert(first)
            assertEquals(SetEvent.Added<Any>(1L), awaitItem())

            val handle = fx.identityMap.handle(lsTodoType, 1L, load = false)
            handle.flow.test {
                assertEquals(first, awaitItem())
                val renamed = Todo(1, "a renamed", false, lsBase.plusSeconds(1))
                fx.upsert(renamed)
                assertEquals(renamed, awaitItem(), "the entity handle must emit")
                cancelAndIgnoreRemainingEvents()
            }
            expectNoEvents() // ...while the set stays silent

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `set and handle are fed by a single spine assignment`() = runTest {
        val fx = Fixture(backgroundScope)
        turbineScope {
            val tap = fx.spine.mutations().testIn(backgroundScope)
            val set = fx.liveSets.events(lsTodoType, openTodos()).testIn(backgroundScope)
            assertEquals(SetEvent.Snapshot(emptySet()), set.awaitItem())

            val todo = Todo(1, "a", false, lsBase)
            fx.upsert(todo)

            // one tap emission == one spine assignment; by the time it is visible the
            // handle AND the set are already consistent (apply, then route, then tap)
            val applied = assertIs<Mutation.Upsert>(tap.awaitItem())
            assertEquals(todo, applied.entity)
            assertEquals(todo, fx.identityMap.handle(lsTodoType, 1L, load = false).flow.value)
            assertEquals(SetEvent.Added<Any>(1L), set.awaitItem())
            set.expectNoEvents()
            tap.expectNoEvents()

            tap.cancelAndIgnoreRemainingEvents()
            set.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `live sets refcount and evict like handles`() = runTest {
        val fx = Fixture(backgroundScope, snapshot = listOf(Todo(1, "open", false, lsBase)))

        fx.liveSets.events(lsTodoType, openTodos()).test {
            assertEquals(SetEvent.Snapshot(emptySet()), awaitItem())
            assertEquals(SetEvent.Added<Any>(1L), awaitItem())
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(1, fx.snapshotLoads.get())
        assertEquals(1, fx.liveSets.size)

        // grace expiry with zero subscribers: evicted, router released
        advanceTimeBy(30_001)
        runCurrent()
        assertEquals(0, fx.liveSets.size, "grace expiry must evict the set")
        assertEquals(0, fx.spine.routers.size, "eviction must deregister the router")

        // resubscription re-registers and re-queries
        fx.liveSets.events(lsTodoType, openTodos()).test {
            assertEquals(SetEvent.Snapshot(emptySet()), awaitItem())
            assertEquals(SetEvent.Added<Any>(1L), awaitItem())
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(2, fx.snapshotLoads.get(), "re-subscription after eviction must re-query the store")
    }
}
