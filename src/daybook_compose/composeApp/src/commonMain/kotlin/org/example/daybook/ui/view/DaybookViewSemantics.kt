package org.example.daybook.ui.view

object DaybookViewSemantics {
    const val ROOT = "daybook-view-root"

    fun node(id: String) = "daybook-view-node:$id"

    fun kind(kind: String) = "daybook-view-kind:$kind"

    fun button(id: String) = "daybook-view-button:$id"
}
