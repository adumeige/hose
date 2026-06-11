package io.hose.core

/**
 * Immutable name → [EntityType] registry, built once at [Hose] construction.
 *
 * Fails fast on duplicate logical names: near-impossible with FQN defaults, but cheap
 * to check and it catches deliberate `name` overrides colliding.
 */
class EntityTypeRegistry(types: Collection<EntityType<*, *, *>>) {

    private val byName: Map<String, EntityType<*, *, *>> = buildMap {
        for (type in types) {
            val previous = put(type.name, type)
            require(previous == null) {
                "Duplicate entity type name '${type.name}': $previous and $type"
            }
        }
    }

    /** All registered types. */
    val all: Collection<EntityType<*, *, *>> get() = byName.values

    /** The type registered under [name], or null. */
    operator fun get(name: String): EntityType<*, *, *>? = byName[name]

    /** The type registered under [name], throwing if absent. */
    fun require(name: String): EntityType<*, *, *> =
        requireNotNull(byName[name]) { "No entity type registered under '$name'" }

    /** True if [type] (by name) is part of this registry. */
    operator fun contains(type: EntityType<*, *, *>): Boolean = byName[type.name] === type
}
