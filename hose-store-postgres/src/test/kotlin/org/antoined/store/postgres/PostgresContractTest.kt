package org.antoined.store.postgres

import app.cash.turbine.test
import org.antoined.contract.EntityStoreContract
import org.antoined.contract.HoseFlowContract
import org.antoined.contract.KitEntity
import org.antoined.contract.ObservableStoreContract
import org.antoined.contract.StoreAdapterFixture
import org.antoined.contract.TypeClassInvariantsContract
import org.antoined.contract.kitBase
import org.antoined.contract.kitEntityType
import org.antoined.contract.storedKitEntity
import org.antoined.core.Hose
import org.antoined.core.HoseConfig
import org.antoined.store.spi.EntityStore
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/**
 * One real Postgres container per JVM; each store gets its own database (NOTIFY is
 * per-database, so this also isolates feeds). Launched via the docker CLI for the
 * same reason as the SurrealDB fixture (DECISIONS.md #015). The external writer is a
 * second connection pool on the same database.
 */
object PostgresTestSupport {
    private val databases = AtomicLong()
    private val storeDatabases = HashMap<EntityStore, String>()

    private val port: Int by lazy {
        val containerId = exec(
            "docker", "run", "-d", "--rm", "-p", "127.0.0.1:0:5432",
            "-e", "POSTGRES_PASSWORD=hose",
            "postgres:17-alpine",
        ).trim()
        Runtime.getRuntime().addShutdownHook(Thread { runCatching { exec("docker", "rm", "-f", containerId) } })
        val mapped = exec("docker", "port", containerId, "5432/tcp")
            .lineSequence().first().substringAfterLast(':').trim().toInt()
        awaitReady(mapped)
        mapped
    }

    private fun exec(vararg command: String): String {
        val process = ProcessBuilder(*command).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor() == 0) { "${command.joinToString(" ")} failed: $output" }
        return output
    }

    private fun awaitReady(port: Int) {
        val deadline = System.currentTimeMillis() + 90_000
        while (true) {
            try {
                DriverManager.getConnection(adminUrl(port), "postgres", "hose").use { c ->
                    c.createStatement().use { it.execute("SELECT 1") }
                }
                return
            } catch (e: Exception) {
                check(System.currentTimeMillis() < deadline) { "Postgres never became ready on :$port" }
                Thread.sleep(250)
            }
        }
    }

    private fun adminUrl(port: Int) = "jdbc:postgresql://127.0.0.1:$port/postgres"
    private fun url(database: String) = "jdbc:postgresql://127.0.0.1:$port/$database"

    fun newStore(observable: Boolean): EntityStore {
        val database = "hose_db${databases.incrementAndGet()}"
        DriverManager.getConnection(adminUrl(port), "postgres", "hose").use { c ->
            c.createStatement().use { it.execute("CREATE DATABASE $database") }
        }
        val store = PostgresStore(url(database), "postgres", "hose", observable = observable)
        synchronized(storeDatabases) { storeDatabases[store] = database }
        return store
    }

    fun connectionTo(primary: EntityStore): EntityStore? {
        val database = synchronized(storeDatabases) { storeDatabases[primary] } ?: return null
        return PostgresStore(url(database), "postgres", "hose", observable = true)
    }

    fun destroy(store: EntityStore) {
        synchronized(storeDatabases) { storeDatabases.remove(store) }
        (store as? AutoCloseable)?.close()
    }
}

class PostgresFixture : StoreAdapterFixture {
    override fun createStore(observable: Boolean): EntityStore = PostgresTestSupport.newStore(observable)
    override fun externalWriter(primary: EntityStore): EntityStore? = PostgresTestSupport.connectionTo(primary)
    override fun destroy(store: EntityStore) = PostgresTestSupport.destroy(store)
}

class PostgresEntityStoreContractTest : EntityStoreContract() {
    override fun fixture() = PostgresFixture()
}

class PostgresObservableStoreContractTest : ObservableStoreContract() {
    override fun fixture() = PostgresFixture()
}

class PostgresHoseFlowContractTest : HoseFlowContract() {
    override fun fixture() = PostgresFixture()
}

class PostgresTypeClassInvariantsContractTest : TypeClassInvariantsContract()

/**
 * Step 15's gate: kill the listen connection, write externally during the gap, and
 * prove snapshot-on-reconnect heals the missed NOTIFY end-to-end.
 */
class PostgresReconnectTest {

    @Test
    fun `snapshot-on-reconnect heals events missed during a connection loss`() {
        val fixture = PostgresFixture()
        val store = fixture.createStore(observable = true)
        val external = fixture.externalWriter(store)!!
        try {
            runBlocking {
                Hose(store, setOf(kitEntityType), HoseConfig(graceMillis = 60_000)).use { hose ->
                    hose.entity(kitEntityType, 1L).test {
                        assertEquals(null, awaitItem())

                        // baseline through the live feed
                        val before = KitEntity(1, "before the cut", 0, true, kitBase)
                        external.upsert(storedKitEntity(before))
                        assertEquals(before, awaitItem())

                        // the listen connection dies; an external write lands during the gap
                        (store as ObservablePostgresStore).killListenConnectionForTest()
                        val during = KitEntity(1, "written while disconnected", 1, true, kitBase.plusSeconds(10))
                        external.upsert(storedKitEntity(during))

                        // re-LISTEN + resync must deliver it without any local action
                        withTimeout(15_000) {
                            assertEquals(during, awaitItem())
                        }
                        cancelAndIgnoreRemainingEvents()
                    }
                }
            }
        } finally {
            fixture.destroy(store)
            fixture.destroy(external)
        }
    }
}
