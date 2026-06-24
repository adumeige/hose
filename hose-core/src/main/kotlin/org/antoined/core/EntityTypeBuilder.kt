package org.antoined.core

/**
 * Builder DSL for [EntityType]. Typical use:
 *
 * ```kotlin
 * val TODO = entityType<Todo, Long, Instant>("todo") {
 *     pk { it.id }
 *     version({ it.updatedAt }, Versions.instant())
 * }
 * ```
 *
 * Defaults: [EntityType.name] falls back to the entity's qualified class name (see the
 * persistence caveat on [EntityType]); [EntityType.encodeKey] falls back to `toString()`.
 * The version codec has **no default**: supply it via a [Versions] strategy or via
 * [encodeVersion]/[decodeVersion] *together* — the builder rejects a half-supplied
 * codec at build time.
 */
class EntityTypeBuilder<E : Any, K : Any, V : Any> @PublishedApi internal constructor(
    private val name: String,
) {
    private var pk: ((E) -> K)? = null
    private var version: ((E) -> V)? = null
    private var comparator: Comparator<V>? = null
    private var encodeKey: (K) -> String = { it.toString() }
    private var encodeVersion: ((V) -> String)? = null
    private var decodeVersion: ((String) -> V)? = null

    /** Primary-key extractor. Required. */
    fun pk(extract: (E) -> K) {
        pk = extract
    }

    /** Version extractor plus a [Versions] strategy supplying comparator (and codec, if it has one). */
    fun version(extract: (E) -> V, strategy: VersionStrategy<V>) {
        version = extract
        comparator = strategy.comparator
        strategy.encode?.let { encodeVersion = it }
        strategy.decode?.let { decodeVersion = it }
    }

    /** Version extractor alone; comparator and codec must be supplied separately. */
    fun version(extract: (E) -> V) {
        version = extract
    }

    /** Explicit version comparator (overrides a previously supplied strategy's). */
    fun versionOrder(order: Comparator<V>) {
        comparator = order
    }

    /** Key encoding for the SPI boundary — must be stable and injective. Default: `toString()`. */
    fun encodeKey(encode: (K) -> String) {
        encodeKey = encode
    }

    /** Version-encoding half of the codec. The decoding half is required too. */
    fun encodeVersion(encode: (V) -> String) {
        encodeVersion = encode
    }

    /** Version-decoding half of the codec. The encoding half is required too. */
    fun decodeVersion(decode: (String) -> V) {
        decodeVersion = decode
    }

    @PublishedApi
    internal fun build(): EntityType<E, K, V> {
        val pk = requireNotNull(pk) { "EntityType '$name': pk extractor is required" }
        val version = requireNotNull(version) { "EntityType '$name': version extractor is required" }
        val comparator = requireNotNull(comparator) {
            "EntityType '$name': version comparator is required — supply a Versions strategy or versionOrder(...)"
        }
        val encV = encodeVersion
        val decV = decodeVersion
        require(encV != null && decV != null) {
            "EntityType '$name': version codec incomplete — " +
                "encodeVersion ${if (encV != null) "present" else "MISSING"}, " +
                "decodeVersion ${if (decV != null) "present" else "MISSING"}. " +
                "Supply both, together (toString is not invertible; there is no default)."
        }
        return BuiltEntityType(name, pk, version, comparator, encodeKey, encV, decV)
    }
}

private class BuiltEntityType<E : Any, K : Any, V : Any>(
    override val name: String,
    private val pkFn: (E) -> K,
    private val versionFn: (E) -> V,
    override val versionOrder: Comparator<V>,
    private val encodeKeyFn: (K) -> String,
    private val encodeVersionFn: (V) -> String,
    private val decodeVersionFn: (String) -> V,
) : EntityType<E, K, V> {
    override fun pk(e: E): K = pkFn(e)
    override fun version(e: E): V = versionFn(e)
    override fun encodeKey(k: K): String = encodeKeyFn(k)
    override fun encodeVersion(v: V): String = encodeVersionFn(v)
    override fun decodeVersion(s: String): V = decodeVersionFn(s)
    override fun toString(): String = "EntityType($name)"
}

/**
 * Builds an [EntityType] for [E]. [name] defaults to the qualified class name —
 * a persisted artifact; see the caveat on [EntityType.name].
 */
inline fun <reified E : Any, K : Any, V : Any> entityType(
    name: String = E::class.qualifiedName ?: error("Anonymous/local classes need an explicit entity type name"),
    block: EntityTypeBuilder<E, K, V>.() -> Unit,
): EntityType<E, K, V> = EntityTypeBuilder<E, K, V>(name).apply(block).build()
