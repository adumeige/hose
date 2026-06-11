package io.hose.core

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/** Plain data class — deliberately no hose imports, no marker interface. */
data class Todo(val id: Long, val title: String, val done: Boolean, val updatedAt: Instant)

private fun todoType(name: String = "todo"): EntityType<Todo, Long, Instant> =
    entityType<Todo, Long, Instant>(name) {
        pk { it.id }
        version({ it.updatedAt }, Versions.instant())
    }

class EntityTypeTest {

    private val t0 = Instant.parse("2026-06-11T10:00:00Z")
    private val todo = Todo(42, "write tests", false, t0)

    @Test
    fun `plain data class registers and round-trips key and version extraction`() {
        val type = todoType()
        assertEquals("todo", type.name)
        assertEquals(42L, type.pk(todo))
        assertEquals(t0, type.version(todo))
        assertEquals("42", type.encodeKey(type.pk(todo)))
        val encoded = type.encodeVersion(type.version(todo))
        assertEquals(t0, type.decodeVersion(encoded))
    }

    @Test
    fun `name defaults to qualified class name`() {
        val type = entityType<Todo, Long, Instant> {
            pk { it.id }
            version({ it.updatedAt }, Versions.instant())
        }
        assertEquals("io.hose.core.Todo", type.name)
    }

    @Test
    fun `equality round-trip - echo suppression depends on it`() {
        // An entity reconstructed from the same values must be == to the original:
        // the feed echo of a local write arrives as a distinct instance.
        val echo = Todo(42, "write tests", false, t0)
        assertEquals(todo, echo)
        assertNotEquals(todo, echo.copy(done = true))
        // and version/key extraction agree across the two instances
        val type = todoType()
        assertEquals(type.pk(todo), type.pk(echo))
        assertEquals(type.version(todo), type.version(echo))
    }

    @Test
    fun `missing decode half of version codec fails at build time`() {
        val failure = assertFailsWith<IllegalArgumentException> {
            entityType<Todo, Long, Instant>("todo") {
                pk { it.id }
                version { it.updatedAt }
                versionOrder(naturalOrder())
                encodeVersion { it.toString() }
                // decodeVersion deliberately missing
            }
        }
        assertTrue("decodeVersion MISSING" in failure.message!!, failure.message)
    }

    @Test
    fun `missing encode half of version codec fails at build time`() {
        assertFailsWith<IllegalArgumentException> {
            entityType<Todo, Long, Instant>("todo") {
                pk { it.id }
                version { it.updatedAt }
                versionOrder(naturalOrder())
                decodeVersion { Instant.parse(it) }
            }
        }
    }

    @Test
    fun `comparator-only strategy still requires explicit codec`() {
        assertFailsWith<IllegalArgumentException> {
            entityType<Todo, Long, Instant>("todo") {
                pk { it.id }
                version({ it.updatedAt }, Versions.comparable())
            }
        }
        // and succeeds once both halves are added
        val type = entityType<Todo, Long, Instant>("todo") {
            pk { it.id }
            version({ it.updatedAt }, Versions.comparable())
            encodeVersion { it.toEpochMilli().toString() }
            decodeVersion { Instant.ofEpochMilli(it.toLong()) }
        }
        assertEquals(t0, type.decodeVersion(type.encodeVersion(t0)))
    }

    @Test
    fun `missing pk fails at build time`() {
        assertFailsWith<IllegalArgumentException> {
            entityType<Todo, Long, Instant>("todo") {
                version({ it.updatedAt }, Versions.instant())
            }
        }
    }

    @Test
    fun `keyOf produces EntityKey from type name and pk`() {
        assertEquals(EntityKey("todo", 42L), todoType().keyOf(todo))
    }
}

class VersionsTest {

    @Test
    fun `long strategy orders naturally and round-trips`() {
        val s = Versions.long()
        assertTrue(s.comparator.compare(9, 10) < 0)
        assertEquals(123L, s.decode!!(s.encode!!(123L)))
    }

    @Test
    fun `lexicographic strategy has the documented 9 greater than 10 hazard`() {
        val s = Versions.lexicographic()
        assertTrue(s.comparator.compare("9", "10") > 0)
        assertEquals("abc", s.decode!!(s.encode!!("abc")))
    }

    @Test
    fun `none strategy ties everything`() {
        val s = Versions.none<String>()
        assertEquals(0, s.comparator.compare("a", "z"))
    }

    @Test
    fun `half codec is rejected by VersionStrategy itself`() {
        assertFailsWith<IllegalArgumentException> {
            VersionStrategy<Long>(naturalOrder(), Long::toString, null)
        }
    }
}

class EntityTypeRegistryTest {

    @Test
    fun `lookup by name`() {
        val type = todoType()
        val registry = EntityTypeRegistry(listOf(type))
        assertEquals(type, registry["todo"])
        assertEquals(type, registry.require("todo"))
        assertTrue(type in registry)
    }

    @Test
    fun `duplicate name registration throws`() {
        val failure = assertFailsWith<IllegalArgumentException> {
            EntityTypeRegistry(listOf(todoType(), todoType()))
        }
        assertTrue("Duplicate entity type name 'todo'" in failure.message!!)
    }

    @Test
    fun `unknown name require throws`() {
        assertFailsWith<IllegalArgumentException> {
            EntityTypeRegistry(emptyList()).require("nope")
        }
    }
}
