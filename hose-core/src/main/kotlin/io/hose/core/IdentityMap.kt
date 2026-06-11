package io.hose.core

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch

/**
 * The runtime's single source of entity handles: one [EntityHandle] per
 * ([EntityType.name], pk), created on demand, shared by every collector.
 *
 * ### The one-cast invariant
 * Narrowing `EntityHandle<*, *, *>`/`EntityType<*, *, *>` back to their typed forms
 * happens in **exactly one place — this file** (see [narrow]/[erased]), justified by
 * the registry's `name ↔ EntityType` bijection: a handle keyed by `(name, pk)` was
 * necessarily created from the unique `EntityType` of that name, so the token the
 * caller presents *is* the token the handle was built with. No cast escapes this map.
 *
 * ### Eviction
 * Each handle gets a watcher: when its subscription count drops to zero, a grace
 * timer starts ([graceMillis], default 30s); a collector arriving before expiry
 * cancels the timer; expiry with still-zero subscribers evicts the handle and
 * releases its upstream resources ([onEvict]). A fresh `handle()` call after
 * eviction re-creates and re-loads from the store.
 */
class IdentityMap(
    private val scope: CoroutineScope,
    private val graceMillis: Long = 30_000,
    private val loader: (suspend (type: EntityType<Any, Any, Any>, pk: Any) -> Any?)? = null,
    private val onEvict: ((EntityKey) -> Unit)? = null,
) {
    private class Entry(
        val handle: EntityHandle<Any, Any, Any>,
        var watcher: Job? = null,
    )

    private val entries = ConcurrentHashMap<EntityKey, Entry>()

    // (typeName, encodedKey) -> pk, learned at handle creation. The feed delivers
    // deletes by encoded key, and key encoding is deliberately one-way; this index is
    // how FEED deletes find their way back to a domain pk. Entries are never removed:
    // a live-set member can outlive its evicted handle, and the index must still
    // resolve its delete. Bounded by distinct keys seen — acceptable at M1.
    private val keyIndex = ConcurrentHashMap<Pair<String, String>, Any>()

    @Suppress("UNCHECKED_CAST") // the one-cast invariant: see class KDoc
    private fun <E : Any, K : Any, V : Any> narrow(handle: EntityHandle<Any, Any, Any>): EntityHandle<E, K, V> =
        handle as EntityHandle<E, K, V>

    private val EntityType<*, *, *>.erased: EntityType<Any, Any, Any>
        get() = erasedToken()

    /** Number of live handles; test/diagnostic surface. */
    val size: Int get() = entries.size

    /**
     * The shared handle for ([type], [pk]), created (and, when [load], seeded from
     * the store loader) if absent.
     */
    fun <E : Any, K : Any, V : Any> handle(type: EntityType<E, K, V>, pk: K, load: Boolean = true): EntityHandle<E, K, V> =
        narrow(entry(type, pk, load).handle)

    /** [handle] for callers holding a version-projected token (the public facade). */
    @Suppress("UNCHECKED_CAST") // the one-cast invariant: see class KDoc
    fun <E : Any, K : Any> handleOf(type: EntityType<E, K, *>, pk: K, load: Boolean = true): EntityHandle<E, K, *> =
        entry(type, pk, load).handle as EntityHandle<E, K, *>

    /** Domain pk previously seen for ([typeName], [encodedKey]), or null if never seen. */
    fun pkFor(typeName: String, encodedKey: String): Any? = keyIndex[typeName to encodedKey]

    /** (token, pk) of every live handle — the resync sweep's working set. */
    fun liveEntries(): List<Pair<EntityType<Any, Any, Any>, Any>> =
        entries.map { (key, entry) -> entry.handle.type to key.pk }

    private fun entry(type: EntityType<*, *, *>, pk: Any, load: Boolean): Entry {
        val erasedType = type.erased
        val key = EntityKey(type.name, pk)
        var created = false
        val entry = entries.computeIfAbsent(key) {
            created = true
            Entry(EntityHandle(erasedType))
        }
        if (created) {
            keyIndex[type.name to erasedType.encodeKey(pk)] = pk
            entry.watcher = scope.launch { watchForEviction(key, entry) }
            if (load && loader != null) {
                scope.launch {
                    entry.handle.seed(loader.invoke(erasedType, pk))
                }
            }
        }
        return entry
    }

    /**
     * Resolves the handle for [mutation] and applies it under the version guard.
     * Mutation-created handles are not store-loaded: the mutation itself supplies
     * the freshest value the runtime knows (a delete-created handle is correctly
     * absent).
     */
    fun apply(mutation: Mutation): ApplyOutcome<Any> {
        val type = mutation.type.erased
        return when (mutation) {
            is Mutation.Upsert -> entry(type, type.pk(mutation.entity), load = false)
                .handle.applyUpsert(mutation.entity)
            is Mutation.Delete -> entry(type, mutation.pk, load = false)
                .handle.applyDelete(mutation.version)
            is Mutation.Revert -> entry(type, mutation.pk, load = false)
                .handle.applyRevert(mutation.toEntity)
        }
    }

    internal companion object {
        /**
         * Part of the one-cast invariant: erasing a wildcard token to its `Any` form
         * lives in this file only, justified by the registry's name ↔ type bijection.
         * Spine-side collaborators (live sets) use it for pk extraction; no other
         * file may cast tokens or handles.
         */
        @Suppress("UNCHECKED_CAST")
        internal fun EntityType<*, *, *>.erasedToken(): EntityType<Any, Any, Any> =
            this as EntityType<Any, Any, Any>

        /**
         * Also part of the one-cast invariant: a live set's keys are produced by
         * `type.pk(...)`, so for a token typed `EntityType<E, K, *>` they are
         * genuinely `K`. The facade narrows the erased event flow here and nowhere
         * else.
         */
        @Suppress("UNCHECKED_CAST")
        internal fun <K : Any> kotlinx.coroutines.flow.Flow<SetEvent<Any>>.narrowKeys(): kotlinx.coroutines.flow.Flow<SetEvent<K>> =
            this as kotlinx.coroutines.flow.Flow<SetEvent<K>>
    }

    private suspend fun watchForEviction(key: EntityKey, entry: Entry) {
        entry.handle.subscriptionCount
            .map { it > 0 }
            .distinctUntilChanged()
            .collectLatest { active ->
                if (!active) {
                    delay(graceMillis)
                    // still zero subscribers after the grace period: evict
                    entries.remove(key, entry)
                    onEvict?.invoke(key)
                    entry.watcher?.cancel()
                }
            }
    }
}
