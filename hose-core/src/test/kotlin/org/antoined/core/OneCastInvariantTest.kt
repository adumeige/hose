package org.antoined.core

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.streams.asSequence
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The one-cast invariant, enforced at the source level: every unchecked narrowing in
 * hose-core lives in `IdentityMap.kt`, justified there by the registry's
 * name ↔ EntityType bijection. No cast escapes the identity map.
 */
class OneCastInvariantTest {

    @Test
    fun `UNCHECKED_CAST appears only in IdentityMap kt`() {
        val root = Path.of("src", "main", "kotlin")
        assertTrue(Files.isDirectory(root), "expected to run from the hose-core module root")
        val offenders = Files.walk(root).use { paths ->
            paths.asSequence()
                .filter { it.extension == "kt" }
                .filter { it.name != "IdentityMap.kt" }
                .filter { "UNCHECKED_CAST" in it.readText() }
                .map { it.toString() }
                .toList()
        }
        assertEquals(emptyList(), offenders)
    }
}
