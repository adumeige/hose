package org.antoined.contract

import org.antoined.store.spi.EntityStore

/**
 * What an adapter module supplies to run the contract kit: a way to create fresh,
 * empty stores, and (optionally) a second handle on the same underlying storage to
 * play the external writer in topology tests.
 */
interface StoreAdapterFixture {

    /**
     * A fresh, empty store. With `observable = true` the returned instance must be an
     * [org.antoined.store.spi.ObservableStore] iff [observableSupported]; with `false` it
     * must present the required tier only.
     */
    fun createStore(observable: Boolean): EntityStore

    /** Whether this adapter implements the optional observable tier. */
    val observableSupported: Boolean get() = true

    /**
     * A second, independent handle (connection) on the same storage as [primary] —
     * the kit's "external writer". Null disables external-writer topology tests.
     */
    fun externalWriter(primary: EntityStore): EntityStore? = null

    /** Release [store] and its backing resources. */
    fun destroy(store: EntityStore) {}
}
