package org.example.daybook

object DaybookEditorSemantics {
    const val SCREEN = "doc-editor-screen"
    const val EMPTY_STATE = "doc-editor-empty-state"
    const val LOADING = "doc-editor-loading"
    const val EDITOR = "doc-editor"
    const val EDITOR_LIST = "doc-editor-list"
    const val TITLE_FIELD = "doc-editor-title"
    const val DETAILS = "doc-editor-details"

    fun facetRow(facetKey: String): String = "doc-editor-facet-row:$facetKey"

    fun facetBlock(facetKey: String): String = "doc-editor-facet-block:$facetKey"

    fun collapsedFacetBlock(facetKey: String): String = "doc-editor-facet-block-collapsed:$facetKey"

    fun blockActions(facetKey: String): String = "doc-editor-block-actions:$facetKey"

    fun toggleBlockCollapseAction(facetKey: String): String = "doc-editor-block-action-toggle-collapse:$facetKey"

    fun toggleBlockCollapseQuickAction(facetKey: String): String =
        "doc-editor-block-action-toggle-collapse-quick:$facetKey"

    fun addBlockAfterQuickAction(facetKey: String): String = "doc-editor-block-action-add-block-after-quick:$facetKey"

    fun makePrimaryAction(facetKey: String): String = "doc-editor-block-action-make-primary:$facetKey"

    fun moveUpAction(facetKey: String): String = "doc-editor-block-action-move-up:$facetKey"

    fun moveDownAction(facetKey: String): String = "doc-editor-block-action-move-down:$facetKey"

    fun addBlockAfterAction(facetKey: String): String = "doc-editor-block-action-add-block-after:$facetKey"

    const val ADD_BLOCK_DIALOG = "doc-editor-add-block-dialog"

    const val FOCUSED_NOTE_ACCESSORY_BAR = "doc-editor-focused-note-accessory-bar"

    fun focusedNoteAccessoryAddBlockAction(facetKey: String): String =
        "doc-editor-focused-note-accessory-add-block:$facetKey"

    fun focusedNoteAccessorySelectBlockAction(facetKey: String): String =
        "doc-editor-focused-note-accessory-select-block:$facetKey"

    const val BLOCK_SELECTION_ACTION_BAR = "doc-editor-block-selection-action-bar"

    fun selectionActionBarAction(actionId: String): String = "doc-editor-block-selection-action:$actionId"

    fun selectBlockQuickAction(facetKey: String): String = "doc-editor-block-action-select-quick:$facetKey"

    const val SELECTION_CANCEL_ACTION = "doc-editor-selection-cancel"

    const val SELECTION_SELECT_ALL_ACTION = "doc-editor-selection-select-all"

    const val ADD_BLOCK_SEARCH_FIELD = "doc-editor-add-block-search"

    fun addBlockOption(optionId: String): String = "doc-editor-add-block-option:$optionId"

    fun pluginFacet(facetKey: String): String = "doc-editor-plugin-facet:$facetKey"

    fun pluginFacetState(facetKey: String): String = "doc-editor-plugin-facet-state:$facetKey"

    fun noteField(facetKey: String): String = "doc-editor-note-field:$facetKey"
}
