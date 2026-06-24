package org.antoined.core

import java.time.Instant

/**
 * A version-handling strategy: comparator plus (optionally) the full string codec.
 *
 * The codec is all-or-nothing: either both [encode] and [decode] are present, or
 * neither is and the application supplies both explicitly on the builder. The builder
 * rejects a half-supplied codec at build time — half an automatism is worse than none.
 */
class VersionStrategy<V : Any> internal constructor(
    val comparator: Comparator<V>,
    val encode: ((V) -> String)?,
    val decode: ((String) -> V)?,
) {
    init {
        require((encode == null) == (decode == null)) {
            "Version codec must be supplied whole: encode and decode together, or neither"
        }
    }
}

/**
 * Shelf of ready-made version strategies for [entityType] builders:
 * comparator and codec supplied together where a canonical codec exists.
 */
object Versions {

    /** Monotonic counter versions. Natural order, decimal codec. */
    fun long(): VersionStrategy<Long> =
        VersionStrategy(naturalOrder(), Long::toString, String::toLong)

    /** Timestamp versions. Natural order, ISO-8601 codec. */
    fun instant(): VersionStrategy<Instant> =
        VersionStrategy(naturalOrder(), Instant::toString, Instant::parse)

    /**
     * Natural order for any [Comparable] version — **comparator only**, the string
     * codec must still be supplied explicitly on the builder ([EntityTypeBuilder.encodeVersion]
     * / [EntityTypeBuilder.decodeVersion]).
     */
    fun <V : Comparable<V>> comparable(): VersionStrategy<V> =
        VersionStrategy(naturalOrder(), null, null)

    /**
     * String versions compared **lexicographically**, identity codec.
     *
     * **Warning:** lexicographic order is not numeric order — `"9" > "10"`. Only use
     * this when versions are genuinely lexicographically ordered (e.g. fixed-width or
     * ULID-style strings), never for stringified counters.
     */
    fun lexicographic(): VersionStrategy<String> =
        VersionStrategy(naturalOrder(), { it }, { it })

    /**
     * No version order: the constant comparator (everything ties). Degrades the
     * version guard to equality-only echo absorption — ties fall through to the
     * equality check, so equal echoes absorb and everything else applies, with zero
     * special-case code. No staleness protection. Codec must still be supplied
     * explicitly.
     */
    fun <V : Any> none(): VersionStrategy<V> =
        VersionStrategy({ _, _ -> 0 }, null, null)
}
