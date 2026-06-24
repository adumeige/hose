package org.antoined.core

import org.antoined.core.IdentityMap.Companion.erasedToken
import org.antoined.store.spi.StoreQueries
import org.antoined.store.spi.StoreQuery
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch

/**
 * Registry and maintenance of live sets: one materialized membership per distinct
 * [StoreQuery], shared by all subscribers.
 *
 * Lifecycle of one set: registration installs a spine router (maintenance) and runs
 * the one-shot store snapshot; each applied mutation of the set's type re-evaluates
 * the query per entity — present & still matching: no-op; present & no longer
 * matching: remove; newly matching: add. Snapshot results are merged under the
 * "deltas win" rule: any key the router touched while the snapshot was loading is
 * left alone (the spine is fresher than the store read). Snapshot entities seed
 * shared handles through the identity map, so members resolve without duplication.
 *
 * Sets refcount their subscribers and evict through the same grace mechanism as
 * entity handles; eviction deregisters the router, and a later subscription
 * re-registers and re-queries.
 */
internal class LiveSets(
    private val scope: CoroutineScope,
    private val spine: Spine,
    private val identityMap: IdentityMap,
    private val graceMillis: Long = 30_000,
    private val snapshotLoader: (suspend (StoreQuery) -> Collection<Any>)? = null,
) {
    private val sets = ConcurrentHashMap<StoreQuery, LiveSetEntry>()

    /** Number of live (non-evicted) sets; test/diagnostic surface. */
    val size: Int get() = sets.size

    /** Queries of every live set — the resync sweep re-runs them. */
    fun activeQueries(): List<StoreQuery> = sets.keys.toList()

    /** (typeName, pk) of every current member — swept on resync even if its handle evicted. */
    fun memberKeys(): List<Pair<String, Any>> =
        sets.values.flatMap { entry -> entry.memberKeysSnapshot() }

    /**
     * Subscribes to the live set for [query]: a [SetEvent.Snapshot] of current
     * membership first, then deltas, in spine order.
     */
    fun events(type: EntityType<*, *, *>, query: StoreQuery): Flow<SetEvent<Any>> = flow {
        require(type.name == query.type) {
            "Query targets type '${query.type}' but token is '${type.name}'"
        }
        var subscription: Pair<LiveSetEntry, ReceiveChannel<SetEvent<Any>>>? = null
        while (subscription == null) {
            val entry = sets.computeIfAbsent(query) { newEntry(type, query) }
            // an entry caught mid-eviction refuses subscribers; retry with a fresh one
            subscription = entry.subscribe()?.let { entry to it }
        }
        val (entry, channel) = subscription
        try {
            for (event in channel) emit(event)
        } finally {
            entry.unsubscribe(channel)
        }
    }

    private fun newEntry(type: EntityType<*, *, *>, query: StoreQuery): LiveSetEntry {
        val entry = LiveSetEntry(type.erasedToken(), query)
        spine.routers += entry.router
        entry.watcher = scope.launch { entry.watchEviction() }
        val loader = snapshotLoader
        if (loader != null) {
            scope.launch { entry.completeSnapshot(loader(query)) }
        } else {
            entry.completeSnapshot(emptyList())
        }
        return entry
    }

    private inner class LiveSetEntry(
        private val type: EntityType<Any, Any, Any>,
        private val query: StoreQuery,
    ) {
        private val lock = Any()
        private val members = LinkedHashSet<Any>()
        private val touchedDuringInit = HashSet<Any>()
        private var initializing = true
        private var evicted = false
        private val subscribers = mutableListOf<Channel<SetEvent<Any>>>()
        private val subscriberCount = MutableStateFlow(0)
        var watcher: Job? = null

        val router: (Mutation, ApplyOutcome.Applied<Any>) -> Unit = { mutation, _ ->
            if (mutation.type.name == query.type) route(mutation)
        }

        private fun route(mutation: Mutation) = synchronized(lock) {
            when (mutation) {
                is Mutation.Upsert -> evaluate(type.pk(mutation.entity), mutation.entity)
                is Mutation.Delete -> dropMember(mutation.pk)
                is Mutation.Revert ->
                    if (mutation.toEntity == null) dropMember(mutation.pk)
                    else evaluate(mutation.pk, mutation.toEntity)
            }
        }

        private fun evaluate(pk: Any, entity: Any) {
            if (initializing) touchedDuringInit += pk
            val matches = StoreQueries.matches(query, entity)
            if (pk in members) {
                if (!matches) {
                    members -= pk
                    broadcast(SetEvent.Removed(pk))
                }
            } else if (matches) {
                members += pk
                broadcast(SetEvent.Added(pk))
            }
        }

        private fun dropMember(pk: Any) {
            if (initializing) touchedDuringInit += pk
            if (members.remove(pk)) broadcast(SetEvent.Removed(pk))
        }

        private fun broadcast(event: SetEvent<Any>) {
            // subscriber channels are UNLIMITED: trySend cannot fail while open
            for (subscriber in subscribers) subscriber.trySend(event)
        }

        fun completeSnapshot(entities: Collection<Any>) {
            synchronized(lock) {
                for (entity in entities) {
                    val pk = type.pk(entity)
                    if (pk in touchedDuringInit) continue // deltas win over the stale read
                    identityMap.handle(type, pk, load = false).seed(entity)
                    if (StoreQueries.matches(query, entity) && members.add(pk)) {
                        broadcast(SetEvent.Added(pk))
                    }
                }
                touchedDuringInit.clear()
                initializing = false
            }
        }

        fun memberKeysSnapshot(): List<Pair<String, Any>> = synchronized(lock) {
            members.map { query.type to it }
        }

        fun subscribe(): ReceiveChannel<SetEvent<Any>>? = synchronized(lock) {
            if (evicted) return null
            val channel = Channel<SetEvent<Any>>(Channel.UNLIMITED)
            channel.trySend(SetEvent.Snapshot(members.toSet()))
            subscribers += channel
            subscriberCount.value = subscribers.size
            channel
        }

        fun unsubscribe(channel: ReceiveChannel<SetEvent<Any>>) {
            synchronized(lock) {
                subscribers.remove(channel)
                subscriberCount.value = subscribers.size
            }
            channel.cancel()
        }

        suspend fun watchEviction() {
            subscriberCount
                .map { it > 0 }
                .distinctUntilChanged()
                .collectLatest { active ->
                    if (!active) {
                        delay(graceMillis)
                        synchronized(lock) {
                            if (subscribers.isNotEmpty()) return@collectLatest
                            evicted = true
                        }
                        spine.routers.remove(router)
                        sets.remove(query, this)
                        watcher?.cancel()
                    }
                }
        }
    }
}
