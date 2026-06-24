package org.antoined.store.memory

import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.FeedListener
import org.antoined.store.spi.FeedSubscription
import org.antoined.store.spi.Link
import org.antoined.store.spi.ObservableStore
import org.antoined.store.spi.StoreQueries
import org.antoined.store.spi.StoredEntity
import org.antoined.store.spi.StoreQuery
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * ConcurrentHashMap-backed store implementing **both capability tiers**.
 *
 * [observable] selects the tier the returned instance *presents*: with `false` the
 * instance is not an [ObservableStore] (`store is ObservableStore` is honest), which
 * lets one adapter exercise both tiers and the topology axis in tests.
 *
 * Writes and feed dispatch share one lock: the feed is totally ordered (hence
 * per-key ordered), and listeners observe writes in the order they took effect.
 */
fun InMemoryStore(observable: Boolean = true): EntityStore {
    val store = ObservableInMemoryStore()
    return if (observable) store else RequiredTierView(store)
}

/** The required-tier-only face: delegation hides the [ObservableStore] capability. */
private class RequiredTierView(delegate: EntityStore) : EntityStore by delegate

internal class ObservableInMemoryStore : ObservableStore {

    private data class TypeKey(val type: String, val key: String)

    private val data = ConcurrentHashMap<TypeKey, StoredEntity>()
    private val subscriptions = CopyOnWriteArrayList<Subscription>()
    private val writeLock = Any()

    private inner class Subscription(
        val types: Set<String>,
        val listener: FeedListener,
    ) : FeedSubscription {
        override fun close() {
            subscriptions.remove(this)
        }
    }

    override fun get(type: String, key: String): StoredEntity? = data[TypeKey(type, key)]

    override fun query(query: StoreQuery): Set<StoredEntity> =
        data.values
            .filter { it.type == query.type && StoreQueries.matches(query, it.payload) }
            .toSet()

    override fun follow(links: Set<Link>): Map<Link, StoredEntity> =
        links.mapNotNull { link -> get(link.type, link.key)?.let { link to it } }.toMap()

    override fun upsert(entity: StoredEntity): StoredEntity {
        synchronized(writeLock) {
            data[TypeKey(entity.type, entity.key)] = entity
            for (sub in subscriptions) {
                if (entity.type in sub.types) sub.listener.onUpsert(entity)
            }
        }
        return entity
    }

    override fun delete(type: String, key: String, version: String?) {
        synchronized(writeLock) {
            val removed = data.remove(TypeKey(type, key)) ?: return
            for (sub in subscriptions) {
                if (type in sub.types) sub.listener.onDelete(type, key, version ?: removed.version)
            }
        }
    }

    override fun changeFeed(types: Set<String>, listener: FeedListener): FeedSubscription =
        Subscription(types.toSet(), listener).also { subscriptions.add(it) }
}
