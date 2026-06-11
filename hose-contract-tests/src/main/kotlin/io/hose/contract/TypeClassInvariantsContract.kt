package io.hose.contract

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.streams.asSequence
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Type-class invariants the whole runtime leans on:
 *
 * - **key-encoding stability** — the same key encodes identically across calls;
 * - **key-encoding injectivity** — distinct keys never encode identically, or two
 *   entities silently merge into one handle (a property test over [sampleKeys];
 *   adapters/applications override to feed their real key shapes);
 * - **one-cast invariant** — adapter sources contain no unchecked casts; the single
 *   blessed site lives in hose-core's `IdentityMap.kt` (enforced by hose-core's own
 *   suite). This scan covers the module under test.
 */
abstract class TypeClassInvariantsContract {

    /** Keys to property-test; override with adapter/application key shapes. */
    protected open fun sampleKeys(): List<Long> = (0L..999L).toList()

    /** Root of the module's main sources for the cast scan. */
    protected open fun mainSourcesRoot(): Path = Path.of("src", "main")

    private fun encode(key: Long): String = kitEntityType.encodeKey(key)

    @Test
    fun `key encoding is stable across calls`() {
        for (key in sampleKeys()) {
            assertEquals(encode(key), encode(key), "key $key must encode identically every time")
        }
    }

    @Test
    fun `key encoding is injective over the sample keys`() {
        val keys = sampleKeys()
        val encodings = keys.associateWith { encode(it) }
        val collisions = encodings.entries
            .groupBy({ it.value }, { it.key })
            .filterValues { it.size > 1 }
        assertEquals(
            emptyMap<String, List<Long>>(),
            collisions,
            "distinct keys must never share an encoding — colliding keys merge into one handle",
        )
    }

    @Test
    fun `no unchecked casts outside the identity map`() {
        val root = mainSourcesRoot()
        assumeTrue(Files.isDirectory(root), "no main sources at $root to scan")
        val offenders = Files.walk(root).use { paths ->
            paths.asSequence()
                .filter { it.extension == "kt" || it.extension == "java" }
                .filter { it.name != "IdentityMap.kt" } // the single blessed site
                .filter { source ->
                    val text = source.readText()
                    "UNCHECKED_CAST" in text || "\"unchecked\"" in text
                }
                .map { it.toString() }
                .toList()
        }
        assertEquals(
            emptyList<String>(),
            offenders,
            "unchecked casts may exist only inside hose-core's IdentityMap.kt",
        )
    }
}
