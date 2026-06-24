package org.antoined.contract

import org.antoined.store.spi.EntityStore
import org.antoined.store.spi.Link
import org.antoined.store.spi.StoreQuery
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * SPI-semantics portion of the adapter contract: CRUD, query correctness, follow,
 * version handling on delete. Runs against the required tier only.
 *
 * Adapter modules subclass this (and the kit's other base classes) and implement
 * [fixture]; everything else is inherited.
 */
abstract class EntityStoreContract {

    protected abstract fun fixture(): StoreAdapterFixture

    private lateinit var fx: StoreAdapterFixture
    protected lateinit var store: EntityStore

    @BeforeEach
    fun createStoreUnderTest() {
        fx = fixture()
        store = fx.createStore(observable = false)
    }

    @AfterEach
    fun destroyStoreUnderTest() {
        fx.destroy(store)
    }

    private fun put(
        id: Long,
        name: String = "e$id",
        score: Int = 0,
        active: Boolean = true,
        atSecond: Long = 0,
    ): KitEntity {
        val entity = KitEntity(id, name, score, active, kitBase.plusSeconds(atSecond))
        store.upsert(storedKitEntity(entity))
        return entity
    }

    @Test
    fun `upsert then get round-trips the envelope`() {
        val entity = put(1, "round trip", score = 7)
        val read = store.get(kitEntityType.name, "1")!!
        assertEquals(kitEntityType.name, read.type)
        assertEquals("1", read.key)
        assertEquals(kitEntityType.encodeVersion(entity.updatedAt), read.version)
        assertEquals(entity, read.payload)
    }

    @Test
    fun `get of an absent key returns null`() {
        assertNull(store.get(kitEntityType.name, "404"))
    }

    @Test
    fun `upsert replaces an existing entity`() {
        put(1, "first", atSecond = 0)
        val second = put(1, "second", atSecond = 1)
        assertEquals(second, store.get(kitEntityType.name, "1")!!.payload)
    }

    @Test
    fun `delete removes the entity`() {
        val entity = put(1)
        store.delete(kitEntityType.name, "1", kitEntityType.encodeVersion(entity.updatedAt))
        assertNull(store.get(kitEntityType.name, "1"))
    }

    @Test
    fun `delete accepts a null version`() {
        put(1)
        store.delete(kitEntityType.name, "1", null)
        assertNull(store.get(kitEntityType.name, "1"))
    }

    @Test
    fun `delete of an absent key is a no-op`() {
        store.delete(kitEntityType.name, "404", null)
        assertNull(store.get(kitEntityType.name, "404"))
    }

    @Test
    fun `query matches the reference evaluator semantics`() {
        put(1, score = 10, active = true)
        put(2, score = 20, active = true)
        put(3, score = 30, active = false)

        fun keysOf(query: StoreQuery) = store.query(query).map { it.key }.toSet()

        assertEquals(setOf("1", "2"), keysOf(activeKitEntities()))
        assertEquals(
            setOf("2", "3"),
            keysOf(
                StoreQuery(
                    kitEntityType.name,
                    listOf(StoreQuery.FieldComparison("score", StoreQuery.Op.GTE, 20)),
                ),
            ),
        )
        // conjunction
        assertEquals(
            setOf("2"),
            keysOf(
                StoreQuery(
                    kitEntityType.name,
                    listOf(
                        StoreQuery.FieldComparison("score", StoreQuery.Op.GTE, 20),
                        StoreQuery.FieldComparison("active", StoreQuery.Op.EQ, true),
                    ),
                ),
            ),
        )
        // empty comparison list selects the whole type
        assertEquals(setOf("1", "2", "3"), keysOf(StoreQuery.all(kitEntityType.name)))
    }

    @Test
    fun `query never leaks across types`() {
        put(1)
        assertEquals(emptySet<Any>(), store.query(StoreQuery.all("some.other.type")))
    }

    @Test
    fun `follow resolves existing links and omits missing ones`() {
        val entity = put(1)
        val hit = Link(kitEntityType.name, "1")
        val miss = Link(kitEntityType.name, "404")
        val resolved = store.follow(setOf(hit, miss))
        assertEquals(setOf(hit), resolved.keys)
        assertEquals(entity, resolved[hit]!!.payload)
    }
}
