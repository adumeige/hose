package org.antoined.contract

import org.antoined.core.EntityType
import org.antoined.core.Versions
import org.antoined.core.entityType
import org.antoined.store.spi.StoredEntity
import org.antoined.store.spi.StoreQuery
import java.time.Instant

/** The kit's standard entity: plain data class, no hose imports needed in real apps. */
data class KitEntity(
    val id: Long,
    val name: String,
    val score: Int,
    val active: Boolean,
    val updatedAt: Instant,
)

/** The kit's type-class token; logical name is deliberately not the FQN. */
val kitEntityType: EntityType<KitEntity, Long, Instant> =
    entityType<KitEntity, Long, Instant>("kit.entity") {
        pk { it.id }
        version({ it.updatedAt }, Versions.instant())
    }

/** Base timestamp for kit fixtures. */
val kitBase: Instant = Instant.parse("2026-01-01T00:00:00Z")

/** [entity] in SPI envelope form, encoded by the kit type-class. */
fun storedKitEntity(entity: KitEntity): StoredEntity = StoredEntity(
    kitEntityType.name,
    kitEntityType.encodeKey(entity.id),
    kitEntityType.encodeVersion(entity.updatedAt),
    entity,
)

/** Query: all active kit entities. */
fun activeKitEntities(): StoreQuery = StoreQuery(
    kitEntityType.name,
    listOf(StoreQuery.FieldComparison("active", StoreQuery.Op.EQ, true)),
)
