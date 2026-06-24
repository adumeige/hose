package org.antoined.store.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.FeedListener
import org.antoined.store.spi.FeedSubscription
import org.antoined.store.spi.Link
import org.antoined.store.spi.ObservableStore
import org.antoined.store.spi.StoreQueries
import org.antoined.store.spi.StoredEntity
import org.antoined.store.spi.StoreQuery
import java.sql.Connection
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Postgres adapter over plain JDBC (HikariCP pool). Schema strategy: **one table per
 * entity type** — `key TEXT PRIMARY KEY, version TEXT, payload JSONB, payload_class
 * TEXT` — the simplest honest mapping; tables and their notify triggers are created
 * lazily on first touch. Logical→physical naming matches the other adapters
 * (lowercase, non-alphanumerics → `_`; stable). Relations resolve through [follow];
 * an edge concept would add a link table on the same primitive.
 *
 * The feed: per-table triggers `pg_notify` on the `hose_changes` channel carrying
 * `{table, op, key, version, payload?}`; a dedicated listen connection polls
 * `PGConnection.getNotifications(timeout)` — pgjdbc has no push, polling *is* the
 * mechanism ([pollIntervalMillis] is the config). Payloads above ~7.5KB are omitted
 * from the notification (pg_notify's 8KB ceiling) and resolved by select-back;
 * superseded select-backs are dropped — a newer notification is already queued.
 *
 * NOTIFY is lossy across disconnects: on reconnect the poller re-LISTENs and signals
 * [FeedListener.onResync]; the core answers with a snapshot refresh.
 */
fun PostgresStore(
    jdbcUrl: String,
    user: String,
    password: String,
    observable: Boolean = true,
    pollIntervalMillis: Int = 250,
): EntityStore {
    val store = ObservablePostgresStore(jdbcUrl, user, password, pollIntervalMillis)
    return if (observable) store else RequiredTierPostgresView(store)
}

private class RequiredTierPostgresView(private val delegate: ObservablePostgresStore) :
    EntityStore by delegate, AutoCloseable by delegate

internal class ObservablePostgresStore(
    private val jdbcUrl: String,
    private val user: String,
    private val password: String,
    private val pollIntervalMillis: Int,
) : ObservableStore, AutoCloseable {

    private val pool = run {
        val config = HikariConfig()
        config.jdbcUrl = jdbcUrl
        config.username = user
        config.password = password
        config.maximumPoolSize = 4
        HikariDataSource(config)
    }

    private val payloadMapper: ObjectMapper = ObjectMapper().findAndRegisterModules()
    private val jsonMapper: ObjectMapper = ObjectMapper()

    private val tables = ConcurrentHashMap<String, String>()
    private val created = ConcurrentHashMap.newKeySet<String>()
    private val subscriptions = CopyOnWriteArrayList<Subscription>()
    private val poller = NotificationPoller()
    private val closed = AtomicBoolean(false)

    private fun tableFor(type: String): String =
        tables.computeIfAbsent(type) { name ->
            "hose_" + name.lowercase().map { if (it.isLetterOrDigit()) it else '_' }.joinToString("")
        }

    private fun <T> withConnection(block: (Connection) -> T): T = pool.connection.use(block)

    private fun ensureTable(type: String): String {
        val table = tableFor(type)
        if (table in created) return table
        synchronized(created) {
            if (table in created) return table
            withConnection { c ->
                c.createStatement().use { st ->
                    st.execute(
                        """
                        CREATE TABLE IF NOT EXISTS "$table" (
                            key TEXT PRIMARY KEY,
                            version TEXT NOT NULL,
                            payload JSONB,
                            payload_class TEXT
                        )
                        """.trimIndent(),
                    )
                    st.execute(NOTIFY_FUNCTION_DDL)
                    st.execute("""DROP TRIGGER IF EXISTS hose_notify_trg ON "$table"""")
                    st.execute(
                        """
                        CREATE TRIGGER hose_notify_trg
                        AFTER INSERT OR UPDATE OR DELETE ON "$table"
                        FOR EACH ROW EXECUTE FUNCTION hose_notify()
                        """.trimIndent(),
                    )
                }
            }
            created += table
        }
        return table
    }

    // ---- required tier ----

    override fun get(type: String, key: String): StoredEntity? {
        val table = ensureTable(type)
        return withConnection { c ->
            c.prepareStatement("""SELECT key, version, payload::text, payload_class FROM "$table" WHERE key = ?""").use { ps ->
                ps.setString(1, key)
                ps.executeQuery().use { rs -> if (rs.next()) fromRow(type, rs) else null }
            }
        }
    }

    override fun query(query: StoreQuery): Set<StoredEntity> {
        val table = ensureTable(query.type)
        return withConnection { c ->
            c.prepareStatement("""SELECT key, version, payload::text, payload_class FROM "$table"""").use { ps ->
                ps.executeQuery().use { rs ->
                    val results = mutableSetOf<StoredEntity>()
                    while (rs.next()) {
                        val entity = fromRow(query.type, rs)
                        if (StoreQueries.matches(query, entity.payload)) results += entity
                    }
                    results
                }
            }
        }
    }

    override fun follow(links: Set<Link>): Map<Link, StoredEntity> =
        links.mapNotNull { link -> get(link.type, link.key)?.let { link to it } }.toMap()

    override fun upsert(entity: StoredEntity): StoredEntity {
        val table = ensureTable(entity.type)
        withConnection { c ->
            c.prepareStatement(
                """
                INSERT INTO "$table" (key, version, payload, payload_class)
                VALUES (?, ?, ?::jsonb, ?)
                ON CONFLICT (key) DO UPDATE
                SET version = EXCLUDED.version, payload = EXCLUDED.payload, payload_class = EXCLUDED.payload_class
                """.trimIndent(),
            ).use { ps ->
                ps.setString(1, entity.key)
                ps.setString(2, entity.version)
                ps.setString(3, entity.payload?.let { payloadMapper.writeValueAsString(it) })
                ps.setString(4, entity.payload?.javaClass?.name)
                ps.executeUpdate()
            }
        }
        return entity
    }

    override fun delete(type: String, key: String, version: String?) {
        val table = ensureTable(type)
        withConnection { c ->
            c.prepareStatement("""DELETE FROM "$table" WHERE key = ?""").use { ps ->
                ps.setString(1, key)
                ps.executeUpdate()
            }
        }
    }

    private fun fromRow(type: String, rs: ResultSet): StoredEntity {
        val key = rs.getString(1)
        val version = rs.getString(2)
        val payloadJson = rs.getString(3)
        val payloadClass = rs.getString(4)
        val payload = payloadClass?.let { payloadMapper.readValue(payloadJson, Class.forName(it)) }
        return StoredEntity(type, key, version, payload)
    }

    // ---- observable tier (triggers + LISTEN/NOTIFY) ----

    private inner class Subscription(val types: Set<String>, val listener: FeedListener) : FeedSubscription {
        val tableToType: Map<String, String> = types.associateBy { ensureTable(it) }
        override fun close() {
            subscriptions.remove(this)
        }
    }

    override fun changeFeed(types: Set<String>, listener: FeedListener): FeedSubscription {
        val subscription = Subscription(types.toSet(), listener)
        subscriptions += subscription
        poller.ensureStarted()
        return subscription
    }

    private fun dispatchNotification(payload: String) {
        val node = jsonMapper.readTree(payload)
        val table = node.get("table")?.asText() ?: return
        val op = node.get("op")?.asText() ?: return
        val key = node.get("key")?.asText() ?: return
        val version = node.get("version")?.asText()
        for (subscription in subscriptions) {
            val type = subscription.tableToType[table] ?: continue
            if (op == "DELETE") {
                subscription.listener.onDelete(type, key, version)
                continue
            }
            val inlinePayload = node.get("payload")?.takeUnless { it.isNull }?.asText()
            val payloadClass = node.get("payload_class")?.takeUnless { it.isNull }?.asText()
            val entity = if (inlinePayload != null && payloadClass != null) {
                StoredEntity(type, key, version, payloadMapper.readValue(inlinePayload, Class.forName(payloadClass)))
            } else {
                // oversized notification: select back; drop if superseded — a newer
                // notification for this key is already behind this one
                val current = get(type, key) ?: continue
                if (current.version != version) continue
                current
            }
            subscription.listener.onUpsert(entity)
        }
    }

    private inner class NotificationPoller {
        private val started = AtomicBoolean(false)
        private val listening = java.util.concurrent.CountDownLatch(1)

        @Volatile
        var listenConnection: Connection? = null

        /** Returns only once LISTEN is active: a write after changeFeed() must notify. */
        fun ensureStarted() {
            if (started.compareAndSet(false, true)) {
                Thread.ofPlatform().name("hose-pg-listen").daemon(true).start { pollLoop() }
            }
            check(listening.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                "LISTEN connection did not come up"
            }
        }

        private fun newListenConnection(): Connection {
            val c = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
            c.createStatement().use { it.execute("LISTEN hose_changes") }
            return c
        }

        private fun pollLoop() {
            var connection: Connection? = null
            var everConnected = false
            while (!closed.get()) {
                try {
                    if (connection == null || connection.isClosed) {
                        connection = newListenConnection()
                        listenConnection = connection
                        listening.countDown()
                        if (everConnected) {
                            // NOTIFY is lossy across the gap we just crossed
                            log.log(System.Logger.Level.INFO, "pg listen connection re-established; signalling resync")
                            subscriptions.forEach { it.listener.onResync() }
                        }
                        everConnected = true
                    }
                    val pg = connection.unwrap(org.postgresql.PGConnection::class.java)
                    val notifications = pg.getNotifications(pollIntervalMillis) ?: continue
                    for (notification in notifications) {
                        runCatching { dispatchNotification(notification.parameter) }
                            .onFailure { log.log(System.Logger.Level.WARNING, "bad notification dropped", it) }
                    }
                } catch (t: Throwable) {
                    if (closed.get()) return
                    runCatching { connection?.close() }
                    connection = null
                    listenConnection = null
                    Thread.sleep(250)
                }
            }
        }
    }

    /** Test hook: kill the listen connection as if the network died. */
    internal fun killListenConnectionForTest() {
        runCatching { poller.listenConnection?.close() }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        subscriptions.clear()
        runCatching { poller.listenConnection?.close() }
        pool.close()
    }

    private companion object {
        val log: System.Logger = System.getLogger(ObservablePostgresStore::class.qualifiedName!!)

        val NOTIFY_FUNCTION_DDL = """
            CREATE OR REPLACE FUNCTION hose_notify() RETURNS trigger AS ${'$'}body${'$'}
            DECLARE
                base JSONB;
                msg TEXT;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    base := jsonb_build_object('table', TG_TABLE_NAME, 'op', TG_OP, 'key', OLD.key, 'version', OLD.version);
                    PERFORM pg_notify('hose_changes', base::text);
                    RETURN OLD;
                END IF;
                base := jsonb_build_object('table', TG_TABLE_NAME, 'op', TG_OP, 'key', NEW.key, 'version', NEW.version);
                msg := (base || jsonb_build_object('payload', NEW.payload::text, 'payload_class', NEW.payload_class))::text;
                IF octet_length(msg) > 7500 THEN
                    msg := base::text;
                END IF;
                PERFORM pg_notify('hose_changes', msg);
                RETURN NEW;
            END
            ${'$'}body${'$'} LANGUAGE plpgsql
        """.trimIndent()
    }
}
