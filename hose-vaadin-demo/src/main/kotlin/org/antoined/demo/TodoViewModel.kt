package org.antoined.demo

import org.antoined.core.Hose
import org.antoined.core.SetEvent
import java.time.Instant
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.scan
import kotlinx.coroutines.launch

/**
 * The demo's single source of UI truth, derived entirely from hose flows:
 *
 * - [todos] folds the live set's snapshot+deltas into a membership set, then combines
 *   the members' shared entity handles into one list — every layer of M1 in one flow.
 * - [detail] follows the selected member's handle.
 *
 * Intents (add/toggle/delete/select) are fire-and-forget optimistic writes.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TodoViewModel(private val hose: Hose = DemoBackend.hose) {

    private val intents = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    val selected = MutableStateFlow<Long?>(null)

    val todos: Flow<List<Todo>> = hose.liveSet(todoType, allTodos())
        .scan(emptySet<Long>()) { members, event ->
            when (event) {
                is SetEvent.Snapshot -> event.keys
                is SetEvent.Added -> members + event.key
                is SetEvent.Removed -> members - event.key
            }
        }
        .flatMapLatest { members ->
            if (members.isEmpty()) flowOf(emptyList())
            else combine(members.sorted().map { hose.entity(todoType, it) }) { todos ->
                todos.filterNotNull()
            }
        }

    val detail: Flow<Todo?> = selected.flatMapLatest { id ->
        if (id == null) flowOf(null) else hose.entity(todoType, id)
    }

    fun onAdd(title: String) {
        val trimmed = title.trim()
        if (trimmed.isEmpty()) return
        intents.launch {
            hose.upsert(todoType, Todo(DemoBackend.newId(), trimmed, done = false, updatedAt = Instant.now()))
        }
    }

    fun onToggle(todo: Todo) {
        intents.launch {
            hose.upsert(todoType, todo.copy(done = !todo.done, updatedAt = Instant.now()))
        }
    }

    fun onDelete(todo: Todo) {
        intents.launch {
            hose.delete(todoType, todo.id, todo.updatedAt)
        }
    }

    fun onSelect(id: Long?) {
        selected.value = id
    }
}
