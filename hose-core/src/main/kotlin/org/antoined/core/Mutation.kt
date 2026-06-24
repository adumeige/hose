package org.antoined.core

/** Where a mutation entered the runtime. */
enum class Origin {
    /** A write made through this [Hose] instance (optimistic routing). */
    LOCAL,

    /** An event from the store's change feed (possibly an echo of a LOCAL write). */
    FEED,
}

/**
 * The envelope that travels the spine. Mutations travel in *domain* form — type-class
 * token plus decoded key/version/entity values; string encoding happens only at the
 * SPI and feed boundaries.
 *
 * The wildcard-to-typed narrowing of `EntityType<*, *, *>` happens exactly once,
 * inside the identity map (the one-cast invariant) — no cast escapes it.
 */
sealed interface Mutation {

    /** The type-class token of the affected entity. */
    val type: EntityType<*, *, *>

    /** Create-or-update of one entity. */
    data class Upsert(
        override val type: EntityType<*, *, *>,
        val entity: Any,
        val origin: Origin,
    ) : Mutation

    /** Removal of one entity; [version] is null when the origin can't supply one. */
    data class Delete(
        override val type: EntityType<*, *, *>,
        val pk: Any,
        val version: Any?,
        val origin: Origin,
    ) : Mutation

    /**
     * Rollback of an optimistic LOCAL routing whose persist failed: restores
     * [toEntity] (the value before the failed write), or removal when it was null.
     */
    data class Revert(
        override val type: EntityType<*, *, *>,
        val pk: Any,
        val toEntity: Any?,
        val cause: Throwable,
    ) : Mutation
}
