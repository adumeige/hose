package io.hose.demo

import com.github.mvysny.kaributesting.v10.MockVaadin
import com.github.mvysny.kaributesting.v10.Routes
import com.github.mvysny.kaributesting.v10._get
import com.github.mvysny.kaributesting.v10._size
import com.vaadin.flow.component.grid.Grid
import java.time.Instant
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking

class TodoViewTest {

    @BeforeTest
    fun setUpVaadin() {
        MockVaadin.setup(Routes().autoDiscoverViews("io.hose.demo"))
    }

    @AfterTest
    fun tearDownVaadin() {
        MockVaadin.tearDown()
    }

    @Test
    fun `a programmatic upsert appears in the grid`() {
        val grid = _get<Grid<Todo>>()

        val todo = Todo(DemoBackend.newId(), "appears in the grid", false, Instant.now())
        runBlocking { DemoBackend.hose.upsert(todoType, todo) }

        awaitGridSize(grid, minimum = 1)
        val titles = (0 until grid._size()).map { grid._get(it).title }
        assertEquals(true, todo.title in titles, "expected '${todo.title}' among $titles")
    }

    private fun awaitGridSize(grid: Grid<Todo>, minimum: Int) {
        val deadline = System.currentTimeMillis() + 5_000
        while (true) {
            MockVaadin.clientRoundtrip() // run queued ui.access blocks
            if (grid._size() >= minimum) return
            check(System.currentTimeMillis() < deadline) {
                "grid never reached $minimum items (has ${grid._size()})"
            }
            Thread.sleep(20)
        }
    }
}
