package org.antoined.core

import org.antoined.store.spi.FeedListener
import org.antoined.store.spi.FeedSubscription
import org.antoined.store.spi.ObservableStore
import org.antoined.store.spi.StoredEntity
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

/**
 * Bridges an [ObservableStore]'s listener-based change feed onto the spine: listener
 * callbacks (adapter threads, must not block) enqueue into an unbounded ordered
 * channel; one forwarder coroutine drains it into the spine, preserving feed order.
 *
 * Echo absorption and stale rejection need no code here — the version guard on the
 * handles already provides both; the bridge marks everything [Origin.FEED] and lets
 * the spine decide.
 *
 * Feed deletes arrive by encoded key (key encoding is one-way); they resolve back to
 * a domain pk through the identity map's key index. A delete for a key this runtime
 * has never seen is dropped: there is no handle and no set member it could affect.
 */
internal class FeedBridge(
    private val scope: CoroutineScope,
    private val spine: Spine,
    private val identityMap: IdentityMap,
    private val registry: EntityTypeRegistry,
    private val store: ObservableStore,
    /** Snapshot-on-reconnect: invoked when the adapter signals lost feed continuity. */
    private val resync: (suspend () -> Unit)? = null,
) : FeedListener {

    private val bridge = Channel<Mutation>(Channel.UNLIMITED)
    private var subscription: FeedSubscription? = null

    init {
        scope.launch { for (mutation in bridge) spine.enqueue(mutation) }
    }

    fun start() {
        subscription = store.changeFeed(registry.all.mapTo(mutableSetOf()) { it.name }, this)
    }

    override fun onUpsert(entity: StoredEntity) {
        val type = registry[entity.type] ?: return
        val payload = entity.payload ?: return
        bridge.trySend(Mutation.Upsert(type, payload, Origin.FEED))
    }

    override fun onDelete(typeName: String, key: String, version: String?) {
        val type = registry[typeName] ?: return
        val pk = identityMap.pkFor(typeName, key) ?: return
        val decodedVersion = version?.let { encoded ->
            runCatching { type.decodeVersion(encoded) }.getOrElse {
                log.log(System.Logger.Level.WARNING, "undecodable feed version '$encoded' for $typeName/$key", it)
                null
            }
        }
        bridge.trySend(Mutation.Delete(type, pk, decodedVersion, Origin.FEED))
    }

    override fun onResync() {
        val handler = resync ?: return
        scope.launch {
            log.log(System.Logger.Level.INFO, "feed signalled resync; refreshing from store snapshots")
            try {
                handler()
            } catch (t: Throwable) {
                log.log(System.Logger.Level.ERROR, "resync sweep failed", t)
            }
        }
    }

    fun stop() {
        subscription?.close()
        subscription = null
        bridge.close()
    }

    private companion object {
        val log: System.Logger = System.getLogger(FeedBridge::class.qualifiedName!!)
    }
}
