package org.antoined.store.surreal

import org.antoined.contract.EntityStoreContract
import org.antoined.contract.StoreAdapterFixture
import org.antoined.contract.TypeClassInvariantsContract
import org.antoined.store.spi.EntityStore
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicLong

/**
 * One real SurrealDB container per JVM (in-memory engine, root/root); each store gets
 * its own database for isolation. The container is launched through the docker CLI:
 * this machine's Docker Desktop hardened socket rejects third-party API clients
 * (docker-java/Testcontainers get masked 400s), while the CLI is allowlisted — same
 * container, same fidelity, different launcher (DECISIONS.md #015).
 *
 * The external writer is a second RPC connection to the same database — a genuinely
 * separate session, as the topology tests intend.
 */
object SurrealTestSupport {
    private val databases = AtomicLong()
    private val storeDatabases = HashMap<EntityStore, String>()

    private val port: Int by lazy {
        val containerId = exec(
            "docker", "run", "-d", "--rm", "-p", "127.0.0.1:0:8000",
            "surrealdb/surrealdb:v2",
            "start", "--user", "root", "--pass", "root", "memory",
        ).lines().last { it.isNotBlank() }.trim()
        Runtime.getRuntime().addShutdownHook(Thread { runCatching { exec("docker", "rm", "-f", containerId) } })
        val mapped = exec("docker", "port", containerId, "8000/tcp")
            .lineSequence().first().substringAfterLast(':').trim().toInt()
        awaitHealthy("127.0.0.1", mapped)
        mapped
    }

    private fun exec(vararg command: String): String {
        val process = ProcessBuilder(*command).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor() == 0) { "${command.joinToString(" ")} failed: $output" }
        return output
    }

    private fun awaitHealthy(host: String, port: Int) {
        // the docker port proxy accepts TCP before SurrealDB serves: poll /health
        val http = java.net.http.HttpClient.newHttpClient()
        val request = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://$host:$port/health")).build()
        val deadline = System.currentTimeMillis() + 60_000
        while (true) {
            try {
                val status = http.send(request, java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode()
                if (status == 200) return
            } catch (ignored: Exception) {
                // not up yet
            }
            check(System.currentTimeMillis() < deadline) { "SurrealDB never became healthy on $host:$port" }
            Thread.sleep(250)
        }
    }

    private fun url() = "ws://127.0.0.1:$port/rpc"

    fun newStore(observable: Boolean): EntityStore {
        val database = "db${databases.incrementAndGet()}"
        val store = SurrealStore(url(), "test", database, observable = observable)
        synchronized(storeDatabases) { storeDatabases[store] = database }
        return store
    }

    fun connectionTo(primary: EntityStore): EntityStore? {
        val database = synchronized(storeDatabases) { storeDatabases[primary] } ?: return null
        return SurrealStore(url(), "test", database, observable = true)
    }

    fun destroy(store: EntityStore) {
        synchronized(storeDatabases) { storeDatabases.remove(store) }
        (store as? AutoCloseable)?.close()
    }
}

class SurrealFixture : StoreAdapterFixture {
    override fun createStore(observable: Boolean): EntityStore = SurrealTestSupport.newStore(observable)
    override fun externalWriter(primary: EntityStore): EntityStore? = SurrealTestSupport.connectionTo(primary)
    override fun destroy(store: EntityStore) = SurrealTestSupport.destroy(store)
}

class SurrealEntityStoreContractTest : EntityStoreContract() {
    override fun fixture() = SurrealFixture()
}

class SurrealTypeClassInvariantsContractTest : TypeClassInvariantsContract()
