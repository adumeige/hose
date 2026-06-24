package org.antoined.contract

import app.cash.turbine.test
import app.cash.turbine.turbineScope
import org.antoined.core.Hose
import org.antoined.core.HoseConfig
import org.antoined.core.Mutation
import org.antoined.core.SetEvent
import org.antoined.core.StaleLocalWriteException
import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.StoredEntity
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/**
 * Flow semantics through a [Hose] over the adapter: version-guard behaviors (Step 06),
 * total order (07), snapshot-then-deltas (08), revert (09), echo-once (10), and the
 * topology tiers — own-writes liveness without a feed, external-writes liveness with
 * one.
 */
abstract class HoseFlowContract {

    protected abstract fun fixture(): StoreAdapterFixture

    /** Short grace so eviction is testable; adapters may override for slow stores. */
    protected open fun hoseConfig(): HoseConfig = HoseConfig(graceMillis = 400)

    private lateinit var fx: StoreAdapterFixture
    private val stores = mutableListOf<EntityStore>()

    @BeforeEach
    fun setUpFixture() {
        fx = fixture()
    }

    @AfterEach
    fun tearDownStores() {
        stores.forEach(fx::destroy)
        stores.clear()
    }

    private fun newStore(observable: Boolean): EntityStore {
        if (observable) assumeTrue(fx.observableSupported, "adapter does not support the observable tier")
        return fx.createStore(observable).also { stores += it }
    }

    private fun <T> withHose(store: EntityStore, block: suspend Hose.() -> T): T = runBlocking {
        Hose(store, setOf(kitEntityType), hoseConfig()).use { it.block() }
    }

    private fun entity(id: Long, name: String = "e$id", atSecond: Long = 0, active: Boolean = true) =
        KitEntity(id, name, 0, active, kitBase.plusSeconds(atSecond))

    // ---- Step 06 behaviors, through the public API ----

    @Test
    fun `one entity, many collectors - all see the same emissions`() {
        withHose(newStore(observable = false)) {
            turbineScope {
                val c1 = entity(kitEntityType, 1L).testIn(this)
                val c2 = entity(kitEntityType, 1L).testIn(this)
                assertEquals(null, c1.awaitItem())
                assertEquals(null, c2.awaitItem())

                val written = entity(1, "shared")
                upsert(kitEntityType, written)
                assertEquals(written, c1.awaitItem())
                assertEquals(written, c2.awaitItem())

                c1.cancelAndIgnoreRemainingEvents()
                c2.cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `tie semantics - same version unequal applies, equal absorbs`() {
        withHose(newStore(observable = false)) {
            entity(kitEntityType, 1L).test {
                assertEquals(null, awaitItem())
                val first = entity(1, "first", atSecond = 5)
                upsert(kitEntityType, first)
                assertEquals(first, awaitItem())

                val tieDifferent = entity(1, "second", atSecond = 5)
                upsert(kitEntityType, tieDifferent)
                assertEquals(tieDifferent, awaitItem())

                upsert(kitEntityType, entity(1, "second", atSecond = 5)) // equal echo
                delay(200)
                expectNoEvents()
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `stale local write is rejected and surfaces as WriteFailure`() {
        withHose(newStore(observable = false)) {
            turbineScope {
                val failures = writeFailures.testIn(this)
                entity(kitEntityType, 1L).test {
                    assertEquals(null, awaitItem())
                    val current = entity(1, "current", atSecond = 10)
                    upsert(kitEntityType, current)
                    assertEquals(current, awaitItem())

                    upsert(kitEntityType, entity(1, "stale", atSecond = 1))
                    delay(200)
                    expectNoEvents()
                    cancelAndIgnoreRemainingEvents()
                }
                assertInstanceOf(StaleLocalWriteException::class.java, failures.awaitItem().cause)
                failures.cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `eviction - re-subscription after grace reloads from the store`() {
        val store = newStore(observable = false)
        withHose(store) {
            val first = entity(1, "first", atSecond = 0)
            upsert(kitEntityType, first)
            entity(kitEntityType, 1L).test {
                assertEquals(first, awaitItem())
                cancelAndIgnoreRemainingEvents()
            }

            delay(800) // beyond the 400ms grace: handle evicted

            // a write the feedless hose cannot observe...
            val external = entity(1, "rewritten outside", atSecond = 60)
            store.upsert(storedKitEntity(external))

            // ...is visible after re-subscription only because eviction forced a fresh get
            withTimeout(5_000) {
                assertEquals(external, entity(kitEntityType, 1L).first { it?.name == "rewritten outside" })
            }
        }
    }

    // ---- Step 07: total order ----

    @Test
    fun `all tap observers see one consistent global sequence`() {
        withHose(newStore(observable = false)) {
            turbineScope {
                val o1 = subscribe(setOf(kitEntityType)).testIn(this)
                val o2 = subscribe(setOf(kitEntityType)).testIn(this)

                val producers = listOf(1L, 2L).map { key ->
                    launch {
                        repeat(15) { s -> upsert(kitEntityType, entity(key, "k$key-$s", atSecond = s.toLong())) }
                    }
                }
                producers.joinAll()

                val seen1 = (1..30).map { o1.awaitItem() }
                val seen2 = (1..30).map { o2.awaitItem() }
                assertEquals(seen1, seen2)

                o1.cancelAndIgnoreRemainingEvents()
                o2.cancelAndIgnoreRemainingEvents()
            }
        }
    }

    // ---- Step 08: snapshot-then-deltas ----

    @Test
    fun `live set - snapshot of pre-existing content, then deltas`() {
        val store = newStore(observable = false)
        store.upsert(storedKitEntity(entity(1, active = true)))
        store.upsert(storedKitEntity(entity(2, active = false)))

        withHose(store) {
            liveSet(kitEntityType, activeKitEntities()).test {
                val members = mutableSetOf<Long>()
                fun fold(event: SetEvent<Long>) = when (event) {
                    is SetEvent.Snapshot -> { members.clear(); members += event.keys }
                    is SetEvent.Added -> members += event.key
                    is SetEvent.Removed -> members -= event.key
                }

                // first event is always a snapshot; the initial store read may land
                // just after it, as deltas
                assertInstanceOf(SetEvent.Snapshot::class.java, awaitItem().also { fold(it) })
                while (members != setOf(1L)) fold(awaitItem())

                upsert(kitEntityType, entity(3, atSecond = 1, active = true))
                fold(awaitItem())
                assertEquals(setOf(1L, 3L), members)

                upsert(kitEntityType, entity(1, atSecond = 2, active = false))
                fold(awaitItem())
                assertEquals(setOf(3L), members)

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    // ---- Step 09: revert ----

    @Test
    fun `a failing persist reverts the optimistic value, in order`() {
        // Seed original directly into the store, bypassing WritePath, so no persist task
        // is queued for it. If we used upsert() instead, the persister (on Dispatchers.IO)
        // might not have processed task 1 before failNextUpsert is set, causing it to fail
        // the wrong task and revert to null instead of original.
        val raw = newStore(observable = false)
        val original = entity(1, "original", atSecond = 0)
        raw.upsert(storedKitEntity(original))

        val failing = FailingStore(raw)
        withHose(failing) {
            // The handle loads from the store asynchronously; wait for it before proceeding.
            withTimeout(10_000) { entity(kitEntityType, 1L).first { it != null } }

            turbineScope {
                val failures = writeFailures.testIn(this)
                entity(kitEntityType, 1L).test {
                    assertEquals(original, awaitItem())

                    failing.failNextUpsert = true
                    val doomed = entity(1, "doomed", atSecond = 1)
                    upsert(kitEntityType, doomed)

                    assertEquals(doomed, awaitItem(), "optimistic value first")
                    assertEquals(original, awaitItem(), "then the revert restores the prior value")
                    cancelAndIgnoreRemainingEvents()
                }
                val failure = failures.awaitItem()
                assertInstanceOf(Mutation.Upsert::class.java, failure.mutation)
                failures.cancelAndIgnoreRemainingEvents()
            }
        }
    }

    // ---- Step 10: echo-once + topology tiers ----

    @Test
    fun `echo-once - a LOCAL write and its feed echo emit exactly once`() {
        withHose(newStore(observable = true)) {
            entity(kitEntityType, 1L).test {
                assertEquals(null, awaitItem())
                val written = entity(1, "echo me")
                upsert(kitEntityType, written)
                assertEquals(written, awaitItem())
                delay(300) // long enough for the persist + feed echo to round-trip
                expectNoEvents()
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `topology tier 1 - own writes stay live without a feed`() {
        withHose(newStore(observable = false)) {
            turbineScope {
                val flow = entity(kitEntityType, 1L).testIn(this)
                assertEquals(null, flow.awaitItem())
                val set = liveSet(kitEntityType, activeKitEntities()).testIn(this)
                assertEquals(SetEvent.Snapshot(emptySet<Long>()), set.awaitItem())

                val written = entity(1)
                upsert(kitEntityType, written)
                assertEquals(written, flow.awaitItem())
                assertEquals(SetEvent.Added(1L), set.awaitItem())

                flow.cancelAndIgnoreRemainingEvents()
                set.cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `topology tier 2 - external writes stay live with a feed`() {
        val store = newStore(observable = true)
        val external = fx.externalWriter(store)
        assumeTrue(external != null, "adapter fixture provides no external writer")

        withHose(store) {
            entity(kitEntityType, 7L).test {
                assertEquals(null, awaitItem())

                val written = entity(7, "from outside")
                external!!.upsert(storedKitEntity(written))
                assertEquals(written, awaitItem())

                external.delete(kitEntityType.name, "7", kitEntityType.encodeVersion(written.updatedAt))
                assertEquals(null, awaitItem())

                cancelAndIgnoreRemainingEvents()
            }
        }
    }
}

/**
 * Kit-internal decorator that injects one persist failure, so revert semantics are
 * testable against any adapter.
 */
class FailingStore(private val delegate: EntityStore) : EntityStore by delegate {
    @Volatile
    var failNextUpsert: Boolean = false

    override fun upsert(entity: StoredEntity): StoredEntity {
        if (failNextUpsert) {
            failNextUpsert = false
            throw IllegalStateException("kit-injected persist failure")
        }
        return delegate.upsert(entity)
    }
}
