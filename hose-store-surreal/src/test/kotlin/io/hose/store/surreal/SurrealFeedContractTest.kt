package io.hose.store.surreal

import app.cash.turbine.test
import io.hose.contract.HoseFlowContract
import io.hose.contract.KitEntity
import io.hose.contract.ObservableStoreContract
import io.hose.contract.kitBase
import io.hose.contract.kitEntityType
import io.hose.contract.storedKitEntity
import io.hose.core.Hose
import io.hose.core.HoseConfig
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class SurrealObservableStoreContractTest : ObservableStoreContract() {
    override fun fixture() = SurrealFixture()
}

class SurrealHoseFlowContractTest : HoseFlowContract() {
    override fun fixture() = SurrealFixture()
}

/**
 * Step 14's reconnect gate: kill the store's connection, write externally during the
 * gap, and prove snapshot-on-reconnect heals the missed event end-to-end.
 */
class SurrealReconnectTest {

    @Test
    fun `snapshot-on-reconnect heals events missed during a connection loss`() {
        val fixture = SurrealFixture()
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

                        // the network dies; an external write lands during the gap
                        (unwrap(store)).simulateConnectionLoss()
                        val during = KitEntity(1, "written while disconnected", 1, true, kitBase.plusSeconds(10))
                        external.upsert(storedKitEntity(during))

                        // reconnect + resync must deliver it without any local action
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

    private fun unwrap(store: Any): ObservableSurrealStore = store as ObservableSurrealStore
}
