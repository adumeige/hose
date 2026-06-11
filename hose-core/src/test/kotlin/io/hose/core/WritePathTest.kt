package io.hose.core

import app.cash.turbine.test
import app.cash.turbine.turbineScope
import io.hose.store.spi.EntityStore
import io.hose.store.spi.Link
import io.hose.store.spi.StoredEntity
import io.hose.store.spi.StoreQuery
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.runTest

private val wpTodoType = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

private val wpBase: Instant = Instant.parse("2026-06-11T10:00:00Z")

/** Store stub: records writes, optionally gates them on a latch, optionally fails. */
private class StubStore : EntityStore {
    val upserts = CopyOnWriteArrayList<StoredEntity>()
    val deletes = CopyOnWriteArrayList<String>()
    var gate: CountDownLatch? = null
    var failWith: (() -> Throwable)? = null
    val persistAttempts = CountDownLatch(1)

    override fun get(type: String, key: String): StoredEntity? = null
    override fun query(query: StoreQuery): Set<StoredEntity> = emptySet()
    override fun follow(links: Set<Link>): Map<Link, StoredEntity> = emptyMap()

    override fun upsert(entity: StoredEntity): StoredEntity {
        persistAttempts.countDown()
        gate?.let { check(it.await(5, TimeUnit.SECONDS)) { "gate timed out" } }
        failWith?.let { throw it() }
        upserts += entity
        return entity
    }

    override fun delete(type: String, key: String, version: String?) {
        persistAttempts.countDown()
        failWith?.let { throw it() }
        deletes += "$type/$key@$version"
    }

    fun awaitUpserts(n: Int) = awaitCount { upserts.size >= n }
    fun awaitDeletes(n: Int) = awaitCount { deletes.size >= n }

    private fun awaitCount(done: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!done()) {
            check(System.currentTimeMillis() < deadline) { "timed out awaiting store writes" }
            Thread.sleep(5)
        }
    }
}

private class WriteFixture(scope: CoroutineScope, val store: StubStore = StubStore()) {
    val identityMap = IdentityMap(scope)
    val spine = Spine(scope, identityMap)
    val writePath = WritePath(scope, spine, store)
}

class WritePathTest {

    @Test
    fun `optimistic emission precedes store write completion`() = runTest {
        val store = StubStore().apply { gate = CountDownLatch(1) }
        val fx = WriteFixture(backgroundScope, store)
        val todo = Todo(1, "instant", false, wpBase)

        val handle = fx.identityMap.handle(wpTodoType, 1L, load = false)
        handle.flow.test {
            assertEquals(null, awaitItem())
            fx.writePath.upsert(wpTodoType, todo)

            // the optimistic value is already observable...
            assertEquals(todo, awaitItem())
            // ...while the store write is still in flight (blocked on the gate)
            assertTrue(store.persistAttempts.await(5, TimeUnit.SECONDS), "persist must have started")
            assertEquals(0, store.upserts.size, "store write must not have completed yet")

            store.gate!!.countDown()
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `failing store yields a revert that restores the prior value, in order`() = runTest {
        val fx = WriteFixture(backgroundScope)
        val original = Todo(1, "original", false, wpBase)
        fx.writePath.upsert(wpTodoType, original)
        fx.store.awaitUpserts(1) // first persist must land before the store starts failing

        fx.store.failWith = { IllegalStateException("disk on fire") }

        val handle = fx.identityMap.handle(wpTodoType, 1L, load = false)
        turbineScope {
            val tap = fx.spine.mutations().testIn(backgroundScope)
            val flow = handle.flow.testIn(backgroundScope)
            assertEquals(original, flow.awaitItem())

            val doomed = Todo(1, "doomed", false, wpBase.plusSeconds(1))
            fx.writePath.upsert(wpTodoType, doomed)

            // optimistic apply first, then the revert restores the prior value
            assertEquals(doomed, flow.awaitItem())
            assertEquals(original, flow.awaitItem())

            // spine order is visible on the tap: Upsert then Revert
            val upsert = assertIs<Mutation.Upsert>(tap.awaitItem())
            assertEquals(doomed, upsert.entity)
            val revert = assertIs<Mutation.Revert>(tap.awaitItem())
            assertEquals(original, revert.toEntity)
            assertEquals(1L, revert.pk)

            tap.cancelAndIgnoreRemainingEvents()
            flow.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `revert of a first write restores absence`() = runTest {
        val fx = WriteFixture(backgroundScope)
        fx.store.failWith = { IllegalStateException("no space left") }

        val handle = fx.identityMap.handle(wpTodoType, 7L, load = false)
        handle.flow.test {
            assertEquals(null, awaitItem())
            val doomed = Todo(7, "doomed", false, wpBase)
            fx.writePath.upsert(wpTodoType, doomed)
            assertEquals(doomed, awaitItem())
            assertEquals(null, awaitItem(), "revert of a first write must restore absence")
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `WriteFailure is observable`() = runTest {
        val fx = WriteFixture(backgroundScope)
        fx.store.failWith = { IllegalStateException("disk on fire") }

        fx.writePath.writeFailures.test {
            val doomed = Todo(1, "doomed", false, wpBase)
            fx.writePath.upsert(wpTodoType, doomed)
            val failure = awaitItem()
            assertEquals(doomed, assertIs<Mutation.Upsert>(failure.mutation).entity)
            assertEquals("disk on fire", failure.cause.message)
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `stale local write is dropped, not persisted, and surfaces as failure`() = runTest {
        val fx = WriteFixture(backgroundScope)
        val current = Todo(1, "current", false, wpBase.plusSeconds(10))
        fx.writePath.upsert(wpTodoType, current)

        fx.writePath.writeFailures.test {
            val stale = Todo(1, "stale", false, wpBase)
            fx.writePath.upsert(wpTodoType, stale)
            val failure = awaitItem()
            assertIs<StaleLocalWriteException>(failure.cause)
            cancelAndIgnoreRemainingEvents()
        }
        assertEquals(current, fx.identityMap.handle(wpTodoType, 1L, load = false).flow.value)
        fx.store.awaitUpserts(1)
        assertEquals(listOf("current"), fx.store.upserts.map { (it.payload as Todo).title })
    }

    @Test
    fun `delete routes optimistically and persists encoded forms`() = runTest {
        val fx = WriteFixture(backgroundScope)
        val todo = Todo(1, "to delete", false, wpBase)
        fx.writePath.upsert(wpTodoType, todo)

        val handle = fx.identityMap.handle(wpTodoType, 1L, load = false)
        handle.flow.test {
            assertEquals(todo, awaitItem())
            fx.writePath.delete(wpTodoType, 1L, wpBase.plusSeconds(1))
            assertEquals(null, awaitItem())
            cancelAndIgnoreRemainingEvents()
        }
        // persisted in order, with codec-encoded key and version
        fx.store.awaitDeletes(1)
        assertEquals(listOf("todo/1@${wpBase.plusSeconds(1)}"), fx.store.deletes)
    }
}
