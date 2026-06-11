package io.hose.store.surreal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.hose.store.spi.EntityStore
import io.hose.store.spi.FeedListener
import io.hose.store.spi.FeedSubscription
import io.hose.store.spi.Link
import io.hose.store.spi.ObservableStore
import io.hose.store.spi.StoreQueries
import io.hose.store.spi.StoredEntity
import io.hose.store.spi.StoreQuery
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * SurrealDB adapter. Triad mapping: entity types → tables (one per logical name,
 * see [tableFor] — the logical→physical mapping is adapter-side and stable);
 * relations → record links resolved through [follow]; edges (`RELATE`) become
 * relevant once the SPI grows an edge concept — `follow` is the dereference
 * primitive both build on.
 *
 * Record shape: `{ key, version, payload, payloadClass }` where `payload` is the
 * Jackson-serialized domain instance. Queries are evaluated **in-process** with the
 * SPI's reference evaluator over the reconstructed payloads — by construction the
 * adapter can never diverge from [StoreQueries] semantics (DECISIONS.md #014).
 *
 * The tier flag mirrors the in-memory adapter: `observable = false` returns a
 * required-tier-only view.
 */
fun SurrealStore(
    url: String,
    namespace: String,
    database: String,
    user: String = "root",
    pass: String = "root",
    observable: Boolean = true,
): EntityStore {
    val store = ObservableSurrealStore(url, namespace, database, user, pass)
    return if (observable) store else RequiredTierSurrealView(store)
}

private class RequiredTierSurrealView(private val delegate: ObservableSurrealStore) :
    EntityStore by delegate, AutoCloseable by delegate

internal class ObservableSurrealStore(
    url: String,
    namespace: String,
    database: String,
    user: String,
    pass: String,
) : ObservableStore, AutoCloseable {

    private val subscriptions = CopyOnWriteArrayList<Subscription>()

    private val client = SurrealRpcClient(
        url = url,
        namespace = namespace,
        database = database,
        user = user,
        pass = pass,
        onDisconnect = ::onConnectionLost,
    ).apply { signinAndUse() }

    private val payloadMapper: ObjectMapper = ObjectMapper().findAndRegisterModules()

    /** Stable logical → physical name mapping: lowercase, non-alphanumerics to '_'. */
    private val tables = ConcurrentHashMap<String, String>()
    private fun tableFor(type: String): String =
        tables.computeIfAbsent(type) { name ->
            name.lowercase().map { if (it.isLetterOrDigit()) it else '_' }.joinToString("")
        }

    // ---- required tier ----

    override fun get(type: String, key: String): StoredEntity? {
        val rows = client.query(
            "SELECT * FROM type::thing(\$tb, \$id)",
            mapOf("tb" to tableFor(type), "id" to key),
        )
        return rows.firstOrNull()?.let { fromRow(type, it) }
    }

    override fun query(query: StoreQuery): Set<StoredEntity> {
        val rows = client.query(
            "SELECT * FROM type::table(\$tb)",
            mapOf("tb" to tableFor(query.type)),
        )
        return rows.mapNotNull { row ->
            fromRow(query.type, row).takeIf { StoreQueries.matches(query, it.payload) }
        }.toSet()
    }

    override fun follow(links: Set<Link>): Map<Link, StoredEntity> =
        links.mapNotNull { link -> get(link.type, link.key)?.let { link to it } }.toMap()

    override fun upsert(entity: StoredEntity): StoredEntity {
        client.query(
            "UPSERT type::thing(\$tb, \$id) CONTENT \$content",
            mapOf(
                "tb" to tableFor(entity.type),
                "id" to entity.key,
                "content" to toContent(entity),
            ),
        )
        return entity
    }

    override fun delete(type: String, key: String, version: String?) {
        client.query(
            "DELETE type::thing(\$tb, \$id)",
            mapOf("tb" to tableFor(type), "id" to key),
        )
    }

    // ---- observable tier (LIVE SELECT) ----

    private inner class Subscription(
        val types: Set<String>,
        val listener: FeedListener,
        val liveIds: MutableList<String> = mutableListOf(),
    ) : FeedSubscription {
        override fun close() {
            subscriptions.remove(this)
            liveIds.forEach(client::kill)
            liveIds.clear()
        }
    }

    override fun changeFeed(types: Set<String>, listener: FeedListener): FeedSubscription {
        val subscription = Subscription(types.toSet(), listener)
        registerLiveQueries(subscription)
        subscriptions += subscription
        return subscription
    }

    private fun registerLiveQueries(subscription: Subscription) {
        for (type in subscription.types) {
            val liveId = client.live(tableFor(type)) { action, record ->
                dispatch(subscription.listener, type, action, record)
            }
            subscription.liveIds += liveId
        }
    }

    private fun dispatch(listener: FeedListener, type: String, action: String, record: JsonNode) {
        when (action) {
            "CREATE", "UPDATE" -> listener.onUpsert(fromRow(type, record))
            "DELETE" -> {
                val key = record.get("key")?.asText() ?: keyFromRecordId(record) ?: return
                listener.onDelete(type, key, record.get("version")?.asText())
            }
        }
    }

    /** Fallback when a DELETE notification carries only the record id. */
    private fun keyFromRecordId(record: JsonNode): String? {
        val id = record.takeIf { it.isTextual }?.asText() ?: record.get("id")?.asText() ?: return null
        return id.substringAfter(':').removeSurrounding("⟨", "⟩")
    }

    /**
     * Connection loss: signal resync (the feed is lossy across the gap), reconnect,
     * and re-register every live query — snapshot-on-reconnect heals the rest
     * core-side.
     */
    private val reconnecting = java.util.concurrent.atomic.AtomicBoolean(false)

    private fun onConnectionLost() {
        // abort + onError can both signal one loss; run a single recovery
        if (!reconnecting.compareAndSet(false, true)) return
        Thread.ofVirtual().name("surreal-reconnect").start {
            try {
                var delayMillis = 100L
                while (true) {
                    try {
                        client.reconnect()
                        break
                    } catch (t: Throwable) {
                        log.log(System.Logger.Level.INFO, "SurrealDB reconnect attempt failed: $t")
                        Thread.sleep(delayMillis)
                        delayMillis = (delayMillis * 2).coerceAtMost(5_000)
                    }
                }
                log.log(System.Logger.Level.INFO, "SurrealDB reconnected; re-registering live queries")
                for (subscription in subscriptions) {
                    subscription.liveIds.clear()
                    runCatching { registerLiveQueries(subscription) }
                        .onFailure { log.log(System.Logger.Level.WARNING, "live re-registration failed", it) }
                    subscription.listener.onResync()
                }
            } finally {
                reconnecting.set(false)
            }
        }
    }

    private companion object {
        val log: System.Logger = System.getLogger(ObservableSurrealStore::class.qualifiedName!!)
    }

    // ---- record mapping ----

    private fun toContent(entity: StoredEntity): Map<String, Any?> = mapOf(
        "key" to entity.key,
        "version" to entity.version,
        "payload" to entity.payload?.let { payloadMapper.writeValueAsString(it) },
        "payloadClass" to entity.payload?.javaClass?.name,
    )

    private fun fromRow(type: String, row: JsonNode): StoredEntity {
        val key = row.get("key").asText()
        val version = row.get("version")?.takeUnless { it.isNull }?.asText()
        val payloadClass = row.get("payloadClass")?.takeUnless { it.isNull }?.asText()
        val payload = payloadClass?.let {
            payloadMapper.readValue(row.get("payload").asText(), Class.forName(it))
        }
        return StoredEntity(type, key, version, payload)
    }

    /** Test hook: simulate network loss; reconnect + resync follow the real path. */
    internal fun simulateConnectionLoss() {
        client.failConnectionForTest()
    }

    override fun close() {
        subscriptions.forEach { it.liveIds.clear() }
        subscriptions.clear()
        client.close()
    }
}
