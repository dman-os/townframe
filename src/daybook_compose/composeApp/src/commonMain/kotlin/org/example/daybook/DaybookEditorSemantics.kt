package org.example.daybook

object DaybookEditorSemantics {
    const val Screen = "doc-editor-screen"
    const val EmptyState = "doc-editor-empty-state"
    const val Loading = "doc-editor-loading"
    const val Editor = "doc-editor"
    const val TitleField = "doc-editor-title"
    const val Details = "doc-editor-details"

    fun facetRow(facetKey: String): String = "doc-editor-facet-row:$facetKey"

    fun noteField(facetKey: String): String = "doc-editor-note-field:$facetKey"
}
