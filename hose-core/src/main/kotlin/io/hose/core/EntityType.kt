package io.hose.core

/**
 * Type-class describing how hose handles an entity class: identity (primary key),
 * versioning, and the string encodings that cross the store SPI.
 *
 * Entities are arbitrary classes — data classes, Java records, generated DTOs — with
 * **no marker interface**. The capabilities hose needs live *beside* the type in this
 * type-class, registered at [Hose] construction.
 *
 * ### Persistence caveats (read before refactoring)
 * [name] and the output of [encodeKey] are **persisted artifacts**: adapters derive
 * physical identifiers (table names, key columns) from them and stored data outlives
 * the process. With the FQN default for [name], **moving a class to another package
 * orphans its data just as surely as renaming it** — override [name] explicitly before
 * any refactoring of a long-lived entity. Composite keys should declare [encodeKey]
 * explicitly rather than relying on `toString()`.
 *
 * Entity **equality is load-bearing**: echo suppression compares the locally written
 * entity against its change-feed echo with `==`. Data classes and records give this
 * for free; hand-written classes must implement `equals` consistently.
 */
interface EntityType<E : Any, K : Any, V : Any> {

    /**
     * Logical type name, unique within one [Hose]. Default: the entity's qualified
     * class name. Adapters map this logical name to physical identifiers; that
     * mapping is adapter-side and must itself be stable.
     */
    val name: String

    /** Extracts the primary key. Must be stable for the lifetime of the entity. */
    fun pk(e: E): K

    /** Extracts the version used by the staleness guard. */
    fun version(e: E): V

    /** Total order over versions; drives the comparator-based version guard. */
    val versionOrder: Comparator<V>

    /**
     * Encodes a key for the SPI boundary. Must be **stable** (same key, same string,
     * forever) and **injective** (distinct keys never collide — or two entities
     * silently merge into one handle). Default: `toString()`.
     */
    fun encodeKey(k: K): String

    /** Encodes a version for the SPI boundary. Paired with [decodeVersion]; no default. */
    fun encodeVersion(v: V): String

    /** Decodes a version from the SPI boundary. Paired with [encodeVersion]; no default. */
    fun decodeVersion(s: String): V
}

/**
 * Internal identity of one entity instance across the whole runtime:
 * logical type name + primary key.
 */
data class EntityKey(val typeName: String, val pk: Any)

/** [EntityKey] for an entity instance under this type. */
fun <E : Any> EntityType<E, *, *>.keyOf(e: E): EntityKey = EntityKey(name, pk(e))

/**
 * Pure predicate over an entity, used by live sets.
 *
 * **Contract: pure and cheap — runs on the spine.** The single mutation-routing
 * coroutine evaluates every registered predicate for every matching-type mutation;
 * a slow or side-effecting predicate stalls or corrupts the whole runtime.
 */
typealias Predicate<E> = (E) -> Boolean
