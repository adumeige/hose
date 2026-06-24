package org.antoined.core

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** Result of applying a mutation to an [EntityHandle]. */
sealed interface ApplyOutcome<out E> {
    /** The mutation took effect; [previous] is the value it replaced (revert material). */
    data class Applied<E>(val previous: E?) : ApplyOutcome<E>

    /** Incoming version strictly older than current — dropped. */
    data object RejectedStale : ApplyOutcome<Nothing>

    /** Version tie and value equality — the echo of an already-applied write; no emission. */
    data object AbsorbedEcho : ApplyOutcome<Nothing>
}

/**
 * The single shared, observable cell for one entity instance. All collectors of one
 * (type, pk) observe the same handle; the identity map guarantees uniqueness.
 *
 * ### The version guard
 * On upsert, incoming is compared to current via [EntityType.versionOrder]:
 * strictly older → dropped (+ logged); strictly newer → applied; **tie → equality
 * check** — equal absorbs as the echo of our own write, unequal applies (arrival
 * order arbitrates, well-defined under the spine's total order). With
 * [Versions.none] everything ties, so the guard degrades to equality-only echo
 * absorption with zero special-case code.
 *
 * Null state means absent: not yet loaded, deleted, or never existing.
 */
class EntityHandle<E : Any, K : Any, V : Any> internal constructor(
    val type: EntityType<E, K, V>,
) {
    private val state = MutableStateFlow<E?>(null)

    /** Read-only view; conflated like any [StateFlow] — slow collectors see the latest. */
    val flow: StateFlow<E?> = state.asStateFlow()

    /** Collector count, used by the identity map's grace-eviction watcher. */
    internal val subscriptionCount: StateFlow<Int> get() = state.subscriptionCount

    internal fun applyUpsert(incoming: E): ApplyOutcome<E> {
        while (true) {
            val current = state.value
            if (current != null) {
                val cmp = type.versionOrder.compare(type.version(incoming), type.version(current))
                if (cmp < 0) {
                    log.log(
                        System.Logger.Level.DEBUG,
                        "dropping stale upsert for ${type.name}/${type.pk(incoming)}: " +
                            "${type.version(incoming)} < ${type.version(current)}",
                    )
                    return ApplyOutcome.RejectedStale
                }
                if (cmp == 0 && incoming == current) return ApplyOutcome.AbsorbedEcho
            }
            if (state.compareAndSet(current, incoming)) return ApplyOutcome.Applied(current)
        }
    }

    internal fun applyDelete(version: V?): ApplyOutcome<E> {
        while (true) {
            val current = state.value ?: return ApplyOutcome.AbsorbedEcho
            if (version != null) {
                val cmp = type.versionOrder.compare(version, type.version(current))
                if (cmp < 0) {
                    log.log(
                        System.Logger.Level.DEBUG,
                        "dropping stale delete for ${type.name}/${type.pk(current)}: " +
                            "$version < ${type.version(current)}",
                    )
                    return ApplyOutcome.RejectedStale
                }
            }
            if (state.compareAndSet(current, null)) return ApplyOutcome.Applied(current)
        }
    }

    /** Restores a pre-write value after a failed persist. Bypasses the version guard. */
    internal fun applyRevert(toEntity: E?): ApplyOutcome<E> {
        while (true) {
            val current = state.value
            if (current == toEntity) return ApplyOutcome.AbsorbedEcho
            if (state.compareAndSet(current, toEntity)) return ApplyOutcome.Applied(current)
        }
    }

    /** Seeds the initial store load; never overwrites a value that arrived first. */
    internal fun seed(loaded: E?) {
        if (loaded != null) applyUpsert(loaded)
    }

    private companion object {
        val log: System.Logger = System.getLogger(EntityHandle::class.qualifiedName!!)
    }
}
