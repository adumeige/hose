package io.hose.demo

import com.github.mvysny.karibudsl.v10.*
import com.vaadin.flow.component.Key
import com.vaadin.flow.component.HasComponents
import com.vaadin.flow.component.checkbox.Checkbox
import com.vaadin.flow.component.grid.Grid
import com.vaadin.flow.component.html.Span
import com.vaadin.flow.component.textfield.TextField
import com.vaadin.flow.router.Route

@Route("")
class TodoView : KComposite() {

    private val viewModel = TodoViewModel()
    private val flows = UiFlowScope(this)

    private lateinit var titleField: TextField
    private lateinit var todoGrid: Grid<Todo>
    private lateinit var detail: Span

    @Suppress("unused")
    private val root = ui {
        verticalLayout {
            setSizeFull()
            h2("hose todos")

            horizontalLayout {
                titleField = textField {
                    placeholder = "What needs doing?"
                    addKeyPressListener(Key.ENTER, { _ -> submit() })
                }
                button("Add") {
                    addThemeVariants(com.vaadin.flow.component.button.ButtonVariant.LUMO_PRIMARY)
                    onClick { submit() }
                }
            }

            todoGrid = grid<Todo> {
                setWidthFull()
                componentColumn({ todo ->
                    Checkbox(todo.done).apply {
                        addValueChangeListener {
                            if (it.isFromClient) viewModel.onToggle(todo)
                        }
                    }
                }) {
                    setHeader("Done")
                    setAutoWidth(true)
                    setFlexGrow(0)
                }
                columnFor(Todo::title) { setHeader("Title") }
                componentColumn({ todo ->
                    com.vaadin.flow.component.button.Button("Delete").apply {
                        onClick { viewModel.onDelete(todo) }
                    }
                }) {
                    setHeader("")
                    setAutoWidth(true)
                    setFlexGrow(0)
                }
                addSelectionListener { event ->
                    if (event.isFromClient) viewModel.onSelect(event.firstSelectedItem.map { it.id }.orElse(null))
                }
            }

            detail = detailPane()
        }
    }

    init {
        flows.bind(viewModel.todos) { todos -> todoGrid.setItems(todos) }
        flows.bind(viewModel.detail) { todo ->
            detail.text = when {
                todo == null -> "Select a todo to see its live detail."
                else -> "#${todo.id} · ${todo.title} · ${if (todo.done) "done" else "open"} · updated ${todo.updatedAt}"
            }
        }
    }

    private fun submit() {
        viewModel.onAdd(titleField.value.orEmpty())
        titleField.clear()
        titleField.focus()
    }
}

/** The single-entity live detail line, bound to one shared entity handle. */
@VaadinDsl
private fun (@VaadinDsl HasComponents).detailPane(block: Span.() -> Unit = {}): Span =
    init(Span("Select a todo to see its live detail."), block)
