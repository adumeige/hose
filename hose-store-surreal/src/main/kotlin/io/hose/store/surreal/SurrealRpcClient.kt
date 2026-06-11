package io.hose.store.surreal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Minimal SurrealDB WebSocket RPC client (JSON subprotocol): request/response
 * correlation by id, plus LIVE-query notification dispatch.
 *
 * The official JVM SDK (1.0.0-beta.1) exposes no live-query API at all, which this
 * adapter's feed tier requires — so the adapter speaks the documented RPC protocol
 * directly over the JDK's [WebSocket]. See DECISIONS.md #013.
 */
internal class SurrealRpcClient(
    private val url: String,
    private val namespace: String,
    private val database: String,
    private val user: String,
    private val pass: String,
    private val requestTimeout: Duration = Duration.ofSeconds(15),
    /** Called once per connection loss, before any reconnect attempt. */
    private val onDisconnect: (() -> Unit)? = null,
) : AutoCloseable {

    val mapper: ObjectMapper = ObjectMapper()

    private val nextId = AtomicLong(1)
    private val pending = ConcurrentHashMap<String, CompletableFuture<JsonNode>>()
    private val liveListeners = ConcurrentHashMap<String, (action: String, result: JsonNode) -> Unit>()
    private val closed = AtomicBoolean(false)
    private val fragments = StringBuilder()
    private val sendLock = Any()

    @Volatile
    private var webSocket: WebSocket = connect()

    private fun connect(): WebSocket {
        val listener = object : WebSocket.Listener {
            override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
                synchronized(fragments) {
                    fragments.append(data)
                    if (last) {
                        val message = fragments.toString()
                        fragments.setLength(0)
                        dispatch(message)
                    }
                }
                ws.request(1)
                return null
            }

            override fun onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
                handleDisconnect()
                return null
            }

            override fun onError(ws: WebSocket, error: Throwable) {
                handleDisconnect()
            }
        }
        val ws = HttpClient.newHttpClient()
            .newWebSocketBuilder()
            .subprotocols("json")
            .buildAsync(URI.create(url), listener)
            .get(15, TimeUnit.SECONDS)
        return ws
    }

    /** Run after [connect]; separate so reconnects can reuse it. */
    fun signinAndUse() {
        request("signin", listOf(mapOf("user" to user, "pass" to pass)))
        request("use", listOf(namespace, database))
    }

    private fun handleDisconnect() {
        if (closed.get()) return
        pending.values.forEach { it.completeExceptionally(IllegalStateException("SurrealDB connection lost")) }
        pending.clear()
        onDisconnect?.invoke()
    }

    /** Re-establishes the socket after a loss; live queries must be re-registered by the caller. */
    fun reconnect() {
        if (closed.get()) return
        liveListeners.clear()
        webSocket = connect()
        signinAndUse()
    }

    private fun dispatch(message: String) {
        val node = mapper.readTree(message)
        val id = node.get("id")?.takeUnless { it.isNull }?.asText()
        if (id != null) {
            val future = pending.remove(id) ?: return
            val error = node.get("error")
            if (error != null && !error.isNull) {
                future.completeExceptionally(IllegalStateException("SurrealDB RPC error: $error"))
            } else {
                future.complete(node.get("result"))
            }
            return
        }
        // no id: a live-query notification {result: {id: <uuid>, action, result}}
        val result = node.get("result") ?: return
        val liveId = result.get("id")?.asText() ?: return
        val action = result.get("action")?.asText() ?: return
        val record = result.get("result") ?: return
        liveListeners[liveId]?.invoke(action, record)
    }

    /** Blocking RPC call. */
    fun request(method: String, params: List<Any?> = emptyList()): JsonNode? {
        check(!closed.get()) { "client is closed" }
        val id = nextId.getAndIncrement().toString()
        val payload: ObjectNode = mapper.createObjectNode().apply {
            put("id", id)
            put("method", method)
            set<JsonNode>("params", mapper.valueToTree(params))
        }
        val future = CompletableFuture<JsonNode>()
        pending[id] = future
        // the JDK WebSocket forbids overlapping sends; serialize them
        synchronized(sendLock) {
            webSocket.sendText(mapper.writeValueAsString(payload), true).get(15, TimeUnit.SECONDS)
        }
        try {
            return future.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } finally {
            pending.remove(id)
        }
    }

    /**
     * Runs one SurrealQL statement (with bindings) and returns its rows; throws on a
     * non-OK statement status.
     */
    fun query(sql: String, vars: Map<String, Any?> = emptyMap()): JsonNode {
        val result = request("query", listOf(sql, vars))
            ?: throw IllegalStateException("null result for: $sql")
        val statement = result.first()
            ?: throw IllegalStateException("no statement result for: $sql")
        val status = statement.get("status")?.asText()
        check(status == "OK") { "SurrealDB query failed ($status): ${statement.get("result")}" }
        return statement.get("result")
    }

    /**
     * Registers a LIVE query on [table]; [listener] receives (action, record).
     *
     * The §7 hazard made real: `LIVE SELECT` rejects `type::table($tb)` although
     * plain `SELECT` accepts it — when the parameterized registration fails, fall
     * back to interpolating the (adapter-sanitized, `[a-z0-9_]`-only) table name and
     * log the downgrade.
     */
    fun live(table: String, listener: (action: String, record: JsonNode) -> Unit): String {
        val rows = try {
            query("LIVE SELECT * FROM type::table(\$tb)", mapOf("tb" to table))
        } catch (e: Exception) {
            log.log(
                System.Logger.Level.INFO,
                "LIVE SELECT rejected the parameterized form for '$table'; downgrading to direct interpolation",
            )
            query("LIVE SELECT * FROM `$table`")
        }
        val liveId = rows.asText()
        liveListeners[liveId] = listener
        return liveId
    }

    private companion object {
        val log: System.Logger = System.getLogger(SurrealRpcClient::class.qualifiedName!!)
    }

    /** Kills the LIVE query [liveId] and stops dispatching to its listener. */
    fun kill(liveId: String) {
        liveListeners.remove(liveId)
        runCatching { query("KILL type::string(\$id)", mapOf("id" to liveId)) }
            .recoverCatching { query("KILL u\"$liveId\"") }
    }

    /** Test hook: drop the socket as if the network died; recovery follows the real path. */
    internal fun failConnectionForTest() {
        runCatching { webSocket.abort() }
        handleDisconnect()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        liveListeners.clear()
        runCatching { webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").get(5, TimeUnit.SECONDS) }
        runCatching { webSocket.abort() }
    }
}
