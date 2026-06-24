package org.antoined.demo

import com.vaadin.flow.component.Component
import com.vaadin.flow.component.UI
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch

/**
 * Minimal Flow → Vaadin bridge: collection starts when [component] attaches, stops on
 * detach, and every emission is applied inside `UI.access` (server push delivers it).
 *
 * The plan names a `vaadin-stateflow` library for this role; no such artifact exists
 * on Maven Central, so the demo carries this small equivalent (DECISIONS.md #012).
 */
class UiFlowScope(component: Component) {

    private var scope: CoroutineScope? = null
    private var ui: UI? = null
    private val bindings = mutableListOf<suspend (UI) -> Unit>()

    init {
        component.addAttachListener { event -> start(event.ui) }
        component.addDetachListener { stop() }
    }

    /** Collects [flow] while attached, applying each value on the UI thread. */
    fun <T> bind(flow: Flow<T>, apply: (T) -> Unit) {
        val binding: suspend (UI) -> Unit = { target ->
            flow.collect { value -> target.access { apply(value) } }
        }
        bindings += binding
        val startedScope = scope
        val attachedUi = ui
        if (startedScope != null && attachedUi != null) {
            startedScope.launch { binding(attachedUi) }
        }
    }

    private fun start(attachedUi: UI) {
        if (scope != null) return
        ui = attachedUi
        val started = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        scope = started
        for (binding in bindings) {
            started.launch { binding(attachedUi) }
        }
    }

    private fun stop() {
        scope?.cancel()
        scope = null
        ui = null
    }
}
