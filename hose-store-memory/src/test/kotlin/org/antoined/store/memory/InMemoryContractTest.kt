package org.antoined.store.memory

import org.antoined.contract.EntityStoreContract
import org.antoined.contract.HoseFlowContract
import org.antoined.contract.ObservableStoreContract
import org.antoined.contract.StoreAdapterFixture
import org.antoined.contract.TypeClassInvariantsContract
import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.ObservableStore
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The memory adapter's whole test surface is the contract kit; an in-process store
 * has no adapter-specific behavior beyond the tier flag below.
 */
object InMemoryFixture : StoreAdapterFixture {
    override fun createStore(observable: Boolean): EntityStore = InMemoryStore(observable)

    // an in-process store IS its own storage: a second reference is a second handle
    override fun externalWriter(primary: EntityStore): EntityStore = primary
}

class InMemoryEntityStoreContractTest : EntityStoreContract() {
    override fun fixture() = InMemoryFixture
}

class InMemoryObservableStoreContractTest : ObservableStoreContract() {
    override fun fixture() = InMemoryFixture
}

class InMemoryHoseFlowContractTest : HoseFlowContract() {
    override fun fixture() = InMemoryFixture
}

class InMemoryTypeClassInvariantsContractTest : TypeClassInvariantsContract()

class InMemoryTierFlagTest {
    @Test
    fun `observable flag controls the presented tier`() {
        assertTrue(InMemoryStore(observable = true) is ObservableStore)
        assertFalse(InMemoryStore(observable = false) is ObservableStore)
    }
}
