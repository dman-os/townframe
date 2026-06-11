package org.example.daybook

object DaybookEditorSemantics {
    const val Screen = "doc-editor-screen"
    const val EmptyState = "doc-editor-empty-state"
    const val Loading = "doc-editor-loading"
    const val Editor = "doc-editor"
    const val TitleField = "doc-editor-title"
    const val Details = "doc-editor-details"

    fun facetRow(facetKey: String): String = "doc-editor-facet-row:$facetKey"

    fun facetBlock(facetKey: String): String = "doc-editor-facet-block:$facetKey"

    fun blockActions(facetKey: String): String = "doc-editor-block-actions:$facetKey"

    fun makePrimaryAction(facetKey: String): String = "doc-editor-block-action-make-primary:$facetKey"

    fun moveUpAction(facetKey: String): String = "doc-editor-block-action-move-up:$facetKey"

    fun moveDownAction(facetKey: String): String = "doc-editor-block-action-move-down:$facetKey"

    fun addNoteAfterAction(facetKey: String): String = "doc-editor-block-action-add-note-after:$facetKey"

    fun pluginFacet(facetKey: String): String = "doc-editor-plugin-facet:$facetKey"

    fun pluginFacetState(facetKey: String): String = "doc-editor-plugin-facet-state:$facetKey"

    fun noteField(facetKey: String): String = "doc-editor-note-field:$facetKey"
}
