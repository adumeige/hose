package io.hose.core

/**
 * Delta-flow emission shape of a live set: a late subscriber receives a [Snapshot]
 * of the current membership first, then [Added]/[Removed] deltas. Events carry
 * **primary keys**; members resolve to shared entity handles via the identity map
 * (no entity duplication inside set events).
 */
sealed interface SetEvent<out K> {

    /** Current membership at subscription time; always the first event a subscriber sees. */
    data class Snapshot<out K>(val keys: Set<K>) : SetEvent<K>

    /** [key] started matching the set's query. */
    data class Added<out K>(val key: K) : SetEvent<K>

    /** [key] stopped matching (or was deleted). */
    data class Removed<out K>(val key: K) : SetEvent<K>
}
