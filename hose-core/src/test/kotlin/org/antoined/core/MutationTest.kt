package org.antoined.core

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class MutationTest {

    private val type = entityType<Todo, Long, Instant>("todo") {
        pk { it.id }
        version({ it.updatedAt }, Versions.instant())
    }
    private val todo = Todo(1, "x", false, Instant.parse("2026-06-11T10:00:00Z"))

    /** The gate: an exhaustive `when` over [Mutation] compiles without `else`. */
    private fun routePlaceholder(m: Mutation): String = when (m) {
        is Mutation.Upsert -> "upsert ${m.type.name} origin=${m.origin}"
        is Mutation.Delete -> "delete ${m.type.name} pk=${m.pk} v=${m.version}"
        is Mutation.Revert -> "revert ${m.type.name} pk=${m.pk} to=${m.toEntity}"
    }

    @Test
    fun `exhaustive when over Mutation routes every variant`() {
        assertEquals(
            "upsert todo origin=LOCAL",
            routePlaceholder(Mutation.Upsert(type, todo, Origin.LOCAL)),
        )
        assertEquals(
            "delete todo pk=1 v=null",
            routePlaceholder(Mutation.Delete(type, 1L, null, Origin.FEED)),
        )
        assertEquals(
            "revert todo pk=1 to=null",
            routePlaceholder(Mutation.Revert(type, 1L, null, IllegalStateException("boom"))),
        )
    }
}
