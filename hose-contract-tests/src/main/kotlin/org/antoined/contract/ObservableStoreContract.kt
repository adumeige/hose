package org.antoined.contract

import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.FeedListener
import org.antoined.store.spi.ObservableStore
import org.antoined.store.spi.StoredEntity
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Feed-semantics portion of the adapter contract, run only when the adapter
 * supports the observable tier: events for every write, per-key ordering,
 * type filtering, listener resubscribe, and external-writer visibility.
 */
abstract class ObservableStoreContract {

    protected abstract fun fixture(): StoreAdapterFixture

    private lateinit var fx: StoreAdapterFixture
    private lateinit var plainStore: EntityStore
    protected lateinit var store: ObservableStore

    @BeforeEach
    fun createObservableStore() {
        fx = fixture()
        assumeTrue(fx.observableSupported, "adapter does not support the observable tier")
        plainStore = fx.createStore(observable = true)
        store = plainStore as ObservableStore
    }

    @AfterEach
    fun destroyObservableStore() {
        if (::plainStore.isInitialized) fx.destroy(plainStore)
    }

    protected class RecordingListener : FeedListener {
        val events = CopyOnWriteArrayList<String>()
        override fun onUpsert(entity: StoredEntity) {
            events += "upsert ${entity.type}/${entity.key}@${entity.version}"
        }
        override fun onDelete(type: String, key: String, version: String?) {
            events += "delete $type/$key@$version"
        }

        fun awaitEvents(n: Int) {
            val deadline = System.currentTimeMillis() + 5_000
            while (events.size < n) {
                check(System.currentTimeMillis() < deadline) {
                    "timed out waiting for $n feed events, got ${events.size}: $events"
                }
                Thread.sleep(5)
            }
        }
    }

    private fun entity(id: Long, atSecond: Long, active: Boolean = true) =
        KitEntity(id, "e$id@$atSecond", 0, active, kitBase.plusSeconds(atSecond))

    @Test
    fun `the feed delivers an event for every write`() {
        val listener = RecordingListener()
        store.changeFeed(setOf(kitEntityType.name), listener).use {
            store.upsert(storedKitEntity(entity(1, 0)))
            store.delete(kitEntityType.name, "1", kitEntityType.encodeVersion(kitBase))
            listener.awaitEvents(2)
            assertEquals(
                listOf(
                    "upsert kit.entity/1@${kitBase}",
                    "delete kit.entity/1@${kitBase}",
                ),
                listener.events,
            )
        }
    }

    @Test
    fun `per-key event ordering follows write order`() {
        val listener = RecordingListener()
        store.changeFeed(setOf(kitEntityType.name), listener).use {
            for (s in 0L..9L) store.upsert(storedKitEntity(entity(1, s)))
            listener.awaitEvents(10)
            assertEquals(
                (0L..9L).map { "upsert kit.entity/1@${kitBase.plusSeconds(it)}" },
                listener.events,
            )
        }
    }

    @Test
    fun `the feed filters by subscribed types`() {
        val listener = RecordingListener()
        store.changeFeed(setOf("some.other.type"), listener).use {
            store.upsert(storedKitEntity(entity(1, 0)))
            Thread.sleep(150)
            assertEquals(emptyList<String>(), listener.events)
        }
    }

    @Test
    fun `a listener can resubscribe after closing`() {
        val first = RecordingListener()
        val subscription = store.changeFeed(setOf(kitEntityType.name), first)
        store.upsert(storedKitEntity(entity(1, 0)))
        first.awaitEvents(1)
        subscription.close()

        store.upsert(storedKitEntity(entity(1, 1)))
        Thread.sleep(100)
        assertEquals(1, first.events.size, "closed subscription must not receive events")

        val second = RecordingListener()
        store.changeFeed(setOf(kitEntityType.name), second).use {
            store.upsert(storedKitEntity(entity(1, 2)))
            second.awaitEvents(1)
            assertEquals(listOf("upsert kit.entity/1@${kitBase.plusSeconds(2)}"), second.events)
        }
    }

    @Test
    fun `writes through a second store handle reach the feed`() {
        val external = fx.externalWriter(plainStore)
        assumeTrue(external != null, "adapter fixture provides no external writer")
        val listener = RecordingListener()
        store.changeFeed(setOf(kitEntityType.name), listener).use {
            external!!.upsert(storedKitEntity(entity(5, 0)))
            listener.awaitEvents(1)
            assertTrue(listener.events.single().startsWith("upsert kit.entity/5@"))
        }
    }
}
