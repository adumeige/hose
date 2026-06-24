package org.antoined.core

import app.cash.turbine.test
import app.cash.turbine.turbineScope
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest

private val todoType = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

private val t1: Instant = Instant.parse("2026-06-11T10:00:00.000Z")
private val t2: Instant = Instant.parse("2026-06-11T10:00:01.000Z")

class EntityHandleTest {

    private fun TestScopeIdentityMap() = Unit // marker to keep imports honest

    @Test
    fun `one handle, many collectors - both receive the same emissions`() = runTest {
        val map = IdentityMap(backgroundScope)
        val h1 = map.handle(todoType, 1L)
        val h2 = map.handle(todoType, 1L)
        assertSame(h1, h2, "identity map must hand out one shared handle per key")

        turbineScope {
            val c1 = h1.flow.testIn(backgroundScope)
            val c2 = h2.flow.testIn(backgroundScope)
            assertEquals(null, c1.awaitItem())
            assertEquals(null, c2.awaitItem())

            val todo = Todo(1, "shared", false, t1)
            h1.applyUpsert(todo)
            assertEquals(todo, c1.awaitItem())
            assertEquals(todo, c2.awaitItem())
        }
    }

    @Test
    fun `echo absorption - applying an equal entity emits nothing`() = runTest {
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(todoType, 1L)
        val todo = Todo(1, "original", false, t1)
        handle.applyUpsert(todo)

        handle.flow.test {
            assertEquals(todo, awaitItem())
            val echo = Todo(1, "original", false, t1) // distinct instance, equal value
            assertIs<ApplyOutcome.AbsorbedEcho>(handle.applyUpsert(echo))
            expectNoEvents()
        }
    }

    @Test
    fun `stale rejection - applying a comparator-older version emits nothing`() = runTest {
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(todoType, 1L)
        val newer = Todo(1, "newer", false, t2)
        handle.applyUpsert(newer)

        handle.flow.test {
            assertEquals(newer, awaitItem())
            val stale = Todo(1, "stale", true, t1)
            assertIs<ApplyOutcome.RejectedStale>(handle.applyUpsert(stale))
            expectNoEvents()
        }
    }

    @Test
    fun `tie semantics - same version unequal applies, equal absorbs`() = runTest {
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(todoType, 1L)
        // two writes in the same millisecond: identical Instant version
        val first = Todo(1, "first", false, t1)
        val second = Todo(1, "second", false, t1)
        handle.applyUpsert(first)

        handle.flow.test {
            assertEquals(first, awaitItem())

            // tie + unequal value: applies (arrival order arbitrates)
            assertIs<ApplyOutcome.Applied<Todo>>(handle.applyUpsert(second))
            assertEquals(second, awaitItem())

            // tie + equal value: absorbed
            assertIs<ApplyOutcome.AbsorbedEcho>(handle.applyUpsert(Todo(1, "second", false, t1)))
            expectNoEvents()
        }
    }

    @Test
    fun `conflation - slow collector observes only the latest of a burst`() = runTest {
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(todoType, 1L)
        val v1 = Todo(1, "v1", false, t1)
        handle.applyUpsert(v1)

        val seen = mutableListOf<Todo?>()
        val collector = launch {
            handle.flow.collect {
                seen += it
                delay(100) // a slow collector
            }
        }
        runCurrent()
        assertEquals(listOf<Todo?>(v1), seen)

        // burst lands while the collector is busy: StateFlow conflates to the latest
        handle.applyUpsert(Todo(1, "v2", false, t1.plusMillis(1)))
        handle.applyUpsert(Todo(1, "v3", false, t1.plusMillis(2)))
        val v4 = Todo(1, "v4", false, t1.plusMillis(3))
        handle.applyUpsert(v4)

        advanceTimeBy(101)
        runCurrent()
        collector.cancel()
        assertEquals(listOf<Todo?>(v1, v4), seen, "intermediate burst values must be conflated away")
    }

    @Test
    fun `eviction - grace expiry evicts, re-get reloads, early re-collect cancels the timer`() = runTest {
        val loads = AtomicInteger()
        val stored = Todo(1, "from store", false, t1)
        val map = IdentityMap(
            scope = backgroundScope,
            graceMillis = 30_000,
            loader = { _, _ ->
                loads.incrementAndGet()
                stored
            },
        )

        val h1 = map.handle(todoType, 1L)
        runCurrent()
        assertEquals(1, loads.get(), "creation triggers one store get")

        // collect, then cancel; within the grace window re-collect: replays current value
        h1.flow.test {
            assertEquals(stored, awaitItem())
            cancelAndIgnoreRemainingEvents()
        }
        advanceTimeBy(10_000)
        h1.flow.test {
            assertEquals(stored, awaitItem(), "re-collect before expiry replays the current value")
            // crossing the original expiry while subscribed: timer was cancelled
            advanceTimeBy(25_000)
            runCurrent()
            assertEquals(1, map.size, "subscribed handle must not be evicted")
            cancelAndIgnoreRemainingEvents()
        }

        // zero subscribers again: full grace elapses, handle evicts
        advanceTimeBy(30_001)
        runCurrent()
        assertEquals(0, map.size, "grace expiry with zero subscribers evicts")

        // re-get after eviction is a fresh handle and hits the store again
        val h2 = map.handle(todoType, 1L)
        runCurrent()
        assertNotSame(h1, h2)
        assertEquals(2, loads.get(), "re-get after eviction must reload from the store")
    }

    @Test
    fun `Versions none - guard degrades to equality-only echo absorption`() = runTest {
        val noneType = entityType<Todo, Long, Instant>("todo-unversioned") {
            pk { it.id }
            version({ it.updatedAt }, Versions.none())
            encodeVersion { it.toString() }
            decodeVersion { Instant.parse(it) }
        }
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(noneType, 1L)
        val newer = Todo(1, "newer", false, t2)
        handle.applyUpsert(newer)

        handle.flow.test {
            assertEquals(newer, awaitItem())

            // "older" timestamp still applies: there is no order, only equality
            val older = Todo(1, "older", false, t1)
            assertIs<ApplyOutcome.Applied<Todo>>(handle.applyUpsert(older))
            assertEquals(older, awaitItem())

            // equal value absorbs as echo
            assertIs<ApplyOutcome.AbsorbedEcho>(handle.applyUpsert(Todo(1, "older", false, t1)))
            expectNoEvents()
        }
    }

    @Test
    fun `delete respects the version guard and revert bypasses it`() = runTest {
        val map = IdentityMap(backgroundScope)
        val handle = map.handle(todoType, 1L)
        val current = Todo(1, "current", false, t2)
        handle.applyUpsert(current)

        // stale delete dropped
        assertIs<ApplyOutcome.RejectedStale>(handle.applyDelete(t1))
        assertEquals(current, handle.flow.value)

        // versionless delete applies
        assertIs<ApplyOutcome.Applied<Todo>>(handle.applyDelete(null))
        assertEquals(null, handle.flow.value)

        // revert restores unconditionally
        handle.applyRevert(current)
        assertEquals(current, handle.flow.value)
    }
}
