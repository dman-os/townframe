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

    fun blockDetailsAction(facetKey: String): String = "doc-editor-block-action-details:$facetKey"

    const val ADD_BLOCK_DIALOG = "doc-editor-add-block-dialog"

    const val BLOCK_DETAILS_DIALOG = "doc-editor-block-details-dialog"

    const val BLOCK_DETAILS_BLOCK_SECTION = "doc-editor-block-details-block-section"

    const val BLOCK_DETAILS_SOURCE_SECTION = "doc-editor-block-details-source-section"

    fun blockDetailsMetadataRow(field: String): String = "doc-editor-block-details-metadata-row:$field"

    fun blockDetailsMetadataValue(field: String): String = "doc-editor-block-details-metadata-value:$field"

    const val BLOCK_DETAILS_FORMAT_SECTION = "doc-editor-block-details-format-section"

    const val BLOCK_DETAILS_FORMAT_PICKER_SECTION = "doc-editor-block-details-format-picker-section"

    const val BLOCK_DETAILS_CUSTOM_MIME_SECTION = "doc-editor-block-details-custom-mime-section"

    fun blockDetailsCurrentFormatSummary(facetKey: String): String =
        "doc-editor-block-details-current-format-summary:$facetKey"

    fun blockDetailsChangeFormatAction(facetKey: String): String = "doc-editor-block-details-change-format:$facetKey"

    fun blockDetailsFormatSearchField(facetKey: String): String = "doc-editor-block-details-format-search:$facetKey"

    fun blockDetailsSourceFacetCard(facetKey: String): String = "doc-editor-block-details-source-facet-card:$facetKey"

    fun blockDetailsFormatOption(facetKey: String, mime: String): String =
        "doc-editor-block-details-format-option:$facetKey:$mime"

    fun blockDetailsCustomMimeAction(facetKey: String): String = "doc-editor-block-details-custom-mime:$facetKey"

    fun blockDetailsCustomMimeInput(facetKey: String): String = "doc-editor-block-details-custom-mime-input:$facetKey"

    fun blockDetailsCustomMimeConfirmAction(facetKey: String): String =
        "doc-editor-block-details-custom-mime-confirm:$facetKey"

    fun blockDetailsCustomMimeError(facetKey: String): String = "doc-editor-block-details-custom-mime-error:$facetKey"

    fun blockDetailsFormatPickerBackAction(facetKey: String): String =
        "doc-editor-block-details-format-picker-back:$facetKey"

    const val FOCUSED_NOTE_ACCESSORY_BAR = "doc-editor-focused-note-accessory-bar"

    fun focusedNoteAccessoryAddBlockAction(facetKey: String): String =
        "doc-editor-focused-note-accessory-add-block:$facetKey"

    fun focusedNoteAccessorySelectBlockAction(facetKey: String): String =
        "doc-editor-focused-note-accessory-select-block:$facetKey"

    fun focusedNoteAccessoryDetailsAction(facetKey: String): String =
        "doc-editor-focused-note-accessory-details:$facetKey"

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
