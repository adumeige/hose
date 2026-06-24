package org.antoined.core

import org.antoined.core.IdentityMap.Companion.erasedToken
import org.antoined.core.IdentityMap.Companion.narrowKeys
import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.ObservableStore
import org.antoined.store.spi.StoreQuery
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.withContext

/** Tuning knobs; every default is the design's documented default. */
data class HoseConfig(
    /** Bounded spine capacity; producers suspend beyond it. */
    val spineCapacity: Int = Spine.DEFAULT_CAPACITY,
    /** Grace period before a subscriber-less handle or live set is evicted. */
    val graceMillis: Long = 30_000,
    /** Where blocking SPI calls run. */
    val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
)

/**
 * The hose: live entity state over a pluggable [EntityStore].
 *
 * Token-anchored throughout — the [EntityType] token is the generic anchor; no casts
 * and no string type names in application code. If the store is an [ObservableStore],
 * external writes flow in through its change feed; otherwise liveness covers this
 * instance's own writes only.
 */
class Hose(
    store: EntityStore,
    types: Set<EntityType<*, *, *>>,
    config: HoseConfig = HoseConfig(),
) : AutoCloseable {

    private val registry = EntityTypeRegistry(types)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val identityMap = IdentityMap(
        scope = scope,
        graceMillis = config.graceMillis,
        loader = { type, pk ->
            withContext(config.ioDispatcher) { store.get(type.name, type.encodeKey(pk))?.payload }
        },
    )

    private val spine = Spine(scope, identityMap, config.spineCapacity)

    private val liveSets = LiveSets(
        scope = scope,
        spine = spine,
        identityMap = identityMap,
        graceMillis = config.graceMillis,
        snapshotLoader = { query ->
            withContext(config.ioDispatcher) { store.query(query).mapNotNull { it.payload } }
        },
    )

    private val writePath = WritePath(scope, spine, store, config.ioDispatcher, config.spineCapacity)

    private val feedBridge: FeedBridge? = (store as? ObservableStore)?.let {
        FeedBridge(scope, spine, identityMap, registry, it, resync = { resyncFromStore(store, config) })
            .also(FeedBridge::start)
    }

    /**
     * Snapshot-on-reconnect: the feed lost continuity, so the store is re-read and
     * every difference re-enters through the spine as FEED mutations — the version
     * guard absorbs what the runtime already has, routers fix live-set membership,
     * and total order is preserved. Swept: every live handle, every live-set member
     * (even if its handle evicted), and every live-set query (for entities the
     * runtime has never seen).
     */
    private suspend fun resyncFromStore(store: EntityStore, config: HoseConfig) {
        val swept = mutableSetOf<Pair<String, Any>>()

        suspend fun sweep(type: EntityType<*, *, *>, pk: Any) {
            if (!swept.add(type.name to pk)) return
            val stored = withContext(config.ioDispatcher) {
                store.get(type.name, type.erasedToken().encodeKey(pk))
            }
            val payload = stored?.payload
            if (payload != null) {
                spine.enqueue(Mutation.Upsert(type, payload, Origin.FEED))
            } else {
                spine.enqueue(Mutation.Delete(type, pk, null, Origin.FEED))
            }
        }

        for ((type, pk) in identityMap.liveEntries()) sweep(type, pk)
        for ((typeName, pk) in liveSets.memberKeys()) registry[typeName]?.let { sweep(it, pk) }
        for (query in liveSets.activeQueries()) {
            val type = registry[query.type] ?: continue
            val rows = withContext(config.ioDispatcher) { store.query(query) }
            for (row in rows) {
                row.payload?.let { spine.enqueue(Mutation.Upsert(type, it, Origin.FEED)) }
            }
        }
    }

    /** The shared live state of one entity; null while absent (unloaded or deleted). */
    fun <E : Any, K : Any> entity(type: EntityType<E, K, *>, pk: K): StateFlow<E?> {
        checkRegistered(type)
        return identityMap.handleOf(type, pk).flow
    }

    /**
     * The live set for [query]: a [SetEvent.Snapshot] of current membership first,
     * then [SetEvent.Added]/[SetEvent.Removed] deltas in spine order. Members resolve
     * to shared state via [entity].
     */
    fun <E : Any, K : Any> liveSet(type: EntityType<E, K, *>, query: StoreQuery): Flow<SetEvent<K>> {
        checkRegistered(type)
        return liveSets.events(type, query).narrowKeys()
    }

    /** The spine tap, filtered to [types]: every applied mutation, in total order. */
    fun subscribe(types: Set<EntityType<*, *, *>>): Flow<Mutation> {
        types.forEach(::checkRegistered)
        val names = types.mapTo(mutableSetOf()) { it.name }
        return spine.mutations().filter { it.type.name in names }
    }

    /** Optimistic write: returns once routed; persistence follows in spine order. */
    suspend fun <E : Any> upsert(type: EntityType<E, *, *>, entity: E) {
        checkRegistered(type)
        writePath.upsert(type, entity)
    }

    /** Optimistic delete; [version] is what the caller believes it is deleting. */
    suspend fun <E : Any, K : Any, V : Any> delete(type: EntityType<E, K, V>, pk: K, version: V) {
        checkRegistered(type)
        writePath.delete(type, pk, version)
    }

    /** Persist failures (reverted writes, dropped stale writes), as they happen. */
    val writeFailures: SharedFlow<WriteFailure> = writePath.writeFailures

    override fun close() {
        feedBridge?.stop()
        writePath.close()
        spine.close()
        scope.cancel()
    }

    private fun checkRegistered(type: EntityType<*, *, *>) {
        require(type in registry) {
            "Entity type '${type.name}' is not registered with this Hose instance"
        }
    }
}
