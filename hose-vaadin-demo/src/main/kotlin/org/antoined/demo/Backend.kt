package org.antoined.demo

import org.antoined.core.EntityType
import org.antoined.core.Hose
import org.antoined.core.Versions
import org.antoined.core.entityType
import org.antoined.store.memory.InMemoryStore
import org.antoined.store.spi.StoreQuery
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/** The demo's domain class — note: no hose imports, no marker interface. */
data class Todo(
    val id: Long,
    val title: String,
    val done: Boolean,
    val updatedAt: Instant,
)

val todoType: EntityType<Todo, Long, Instant> = entityType<Todo, Long, Instant>("demo.todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

fun allTodos(): StoreQuery = StoreQuery.all(todoType.name)

/**
 * Process-wide backend: one observable in-memory store, one [Hose]. Every browser
 * session shares these, which is exactly what makes cross-session live propagation
 * observable in the demo.
 */
object DemoBackend {
    private val ids = AtomicLong(System.currentTimeMillis())

    val hose: Hose = Hose(InMemoryStore(observable = true), setOf(todoType))

    fun newId(): Long = ids.incrementAndGet()
}
