package io.hose.core

import app.cash.turbine.turbineScope
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout

private val spineTodoType = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

private val base: Instant = Instant.parse("2026-06-11T10:00:00Z")

private fun upsert(key: Long, seq: Int, origin: Origin = Origin.LOCAL) =
    Mutation.Upsert(spineTodoType, Todo(key, "k$key-$seq", false, base.plusMillis(seq.toLong())), origin)

class SpineTest {

    @Test
    fun `total order - all observers see one consistent global sequence`() = runTest {
        val spine = Spine(backgroundScope, IdentityMap(backgroundScope))
        turbineScope {
            val observer1 = spine.mutations().testIn(backgroundScope)
            val observer2 = spine.mutations().testIn(backgroundScope)

            val perProducer = 25
            val producers = listOf(1L, 2L).map { key ->
                launch {
                    repeat(perProducer) { seq -> spine.enqueue(upsert(key, seq)) }
                }
            }
            producers.joinAll()

            val seen1 = (1..2 * perProducer).map { observer1.awaitItem() }
            val seen2 = (1..2 * perProducer).map { observer2.awaitItem() }
            assertEquals(seen1, seen2, "all tap observers must see the same global sequence")

            // and per-key order is each producer's enqueue order
            for (key in listOf(1L, 2L)) {
                val perKey = seen1.filterIsInstance<Mutation.Upsert>()
                    .map { it.entity as Todo }
                    .filter { it.id == key }
                assertEquals((0 until perProducer).map { "k$key-$it" }, perKey.map { it.title })
            }
            observer1.cancelAndIgnoreRemainingEvents()
            observer2.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `LOCAL and FEED mutations for one key merge sequentially under the version guard`() = runTest {
        val identityMap = IdentityMap(backgroundScope)
        val spine = Spine(backgroundScope, identityMap)
        turbineScope {
            val tap = spine.mutations().testIn(backgroundScope)

            val local = Todo(1, "local", false, base.plusSeconds(10))
            spine.enqueue(Mutation.Upsert(spineTodoType, local, Origin.LOCAL))

            val staleFeed = Todo(1, "stale-feed", false, base.plusSeconds(5))
            spine.enqueue(Mutation.Upsert(spineTodoType, staleFeed, Origin.FEED))

            val newerFeed = Todo(1, "newer-feed", false, base.plusSeconds(20))
            spine.enqueue(Mutation.Upsert(spineTodoType, newerFeed, Origin.FEED))

            // the stale FEED mutation was rejected deterministically: it never taps
            assertEquals(local, (tap.awaitItem() as Mutation.Upsert).entity)
            assertEquals(newerFeed, (tap.awaitItem() as Mutation.Upsert).entity)
            tap.expectNoEvents()

            assertEquals(newerFeed, identityMap.handle(spineTodoType, 1L, load = false).flow.value)
            tap.cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `bounded channel - saturating the spine suspends the producer instead of buffering`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + Job())
        try {
            val identityMap = IdentityMap(scope)
            val spine = Spine(scope, identityMap, capacity = 1)

            val gate = CountDownLatch(1)
            val sent = AtomicInteger()
            // the first mutation parks the loop inside a router: nothing else drains
            spine.routers += { _, _ -> gate.await() }

            val producer = scope.launch {
                repeat(4) { seq ->
                    spine.enqueue(upsert(1, seq))
                    sent.incrementAndGet()
                }
            }

            // mutation 0 occupies the loop, mutation 1 fills the buffer; the producer
            // must then be suspended mid-`send`, having completed at most 2 sends
            withTimeout(5_000) {
                while (sent.get() < 2) delay(10)
            }
            delay(200)
            assertEquals(2, sent.get(), "producer must suspend once capacity-1 channel is full")

            gate.countDown()
            withTimeout(5_000) { producer.join() }
            assertEquals(4, sent.get())
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `a throwing router does not kill the loop`() = runTest {
        val identityMap = IdentityMap(backgroundScope)
        val spine = Spine(backgroundScope, identityMap)
        spine.routers += { _, _ -> error("router boom") }

        turbineScope {
            val tap = spine.mutations().testIn(backgroundScope)
            spine.enqueue(upsert(1, 0))
            spine.enqueue(upsert(1, 1))
            // both mutations survive routing failure and reach the tap in order
            assertEquals("k1-0", ((tap.awaitItem() as Mutation.Upsert).entity as Todo).title)
            assertEquals("k1-1", ((tap.awaitItem() as Mutation.Upsert).entity as Todo).title)
            tap.cancelAndIgnoreRemainingEvents()
        }
        assertEquals(
            "k1-1",
            identityMap.handle(spineTodoType, 1L, load = false).flow.value?.title,
        )
    }

    @Test
    fun `the loop is single-threaded - routing never overlaps even on a multithreaded dispatcher`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + Job())
        try {
            val spine = Spine(scope, IdentityMap(scope))
            val inLoop = AtomicInteger()
            val maxConcurrency = AtomicInteger()
            val processed = AtomicInteger()
            spine.routers += { _, _ ->
                val now = inLoop.incrementAndGet()
                maxConcurrency.accumulateAndGet(now, ::maxOf)
                Thread.sleep(1) // widen the window: any second loop coroutine would overlap here
                inLoop.decrementAndGet()
                processed.incrementAndGet()
            }

            val producers = (0 until 8).map { p ->
                scope.launch { repeat(10) { seq -> spine.enqueue(upsert(p.toLong(), seq)) } }
            }
            producers.joinAll()
            withTimeout(10_000) {
                while (processed.get() < 80) delay(10)
            }
            assertEquals(1, maxConcurrency.get(), "mutations must be processed strictly sequentially by one coroutine")
        } finally {
            scope.cancel()
        }
    }
}
