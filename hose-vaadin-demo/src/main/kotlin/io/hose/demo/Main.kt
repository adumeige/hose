package io.hose.demo

import com.github.mvysny.vaadinboot.VaadinBoot
import com.vaadin.flow.component.page.AppShellConfigurator
import com.vaadin.flow.component.page.Push

/** Server push carries hose emissions to every open session. */
@Push
class AppShell : AppShellConfigurator

fun main() {
    VaadinBoot().run()
}
