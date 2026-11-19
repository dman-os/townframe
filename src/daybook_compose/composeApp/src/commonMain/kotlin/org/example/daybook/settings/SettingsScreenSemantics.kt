package org.example.daybook.settings

object SettingsScreenSemantics {
    const val ROOT = "settings-screen"
    const val SECTION_LIST = "settings-section-list"
    const val SETTINGS_BACK_BUTTON = "settings-back-button"

    fun sectionItem(sectionId: String): String = "settings-section-item:$sectionId"

    fun sectionDetail(sectionId: String): String = "settings-section-detail:$sectionId"

    const val PLUGS_LIST = "settings-plugs-list"
    const val PLUGS_ADD_BUTTON = "settings-plugs-add-button"
    const val PLUGS_ADD_DIALOG = "settings-plugs-add-dialog"
    const val PLUGS_IMPORT_PATH_FIELD = "settings-plugs-import-path-field"
    const val PLUGS_IMPORT_BROWSE_BUTTON = "settings-plugs-import-browse-button"
    const val PLUGS_IMPORT_CONFIRM_BUTTON = "settings-plugs-import-confirm-button"
    const val PLUGS_IMPORT_REVIEW = "settings-plugs-import-review"
    const val PLUGS_IMPORT_REVIEW_LOADING = "settings-plugs-import-review-loading"
    const val PLUGS_IMPORT_REVIEW_CHANGE_PATH_BUTTON = "settings-plugs-import-review-change-path-button"
    const val PLUGS_IMPORT_REVIEW_IMPORT_BUTTON = "settings-plugs-import-review-import-button"
    const val PLUGS_IMPORT_REVIEW_ERROR = "settings-plugs-import-review-error"
    const val PLUGS_IMPORT_REVIEW_PREVIEW = "settings-plugs-import-review-preview"
    const val PLUGS_IMPORT_REVIEW_PREVIEW_TITLE = "settings-plugs-import-review-preview-title"
    const val PLUGS_IMPORT_REVIEW_PREVIEW_ID = "settings-plugs-import-review-preview-id"
    const val PLUGS_IMPORT_REVIEW_PREVIEW_DESCRIPTION = "settings-plugs-import-review-preview-description"
    const val PLUGS_IMPORT_REVIEW_PREVIEW_COUNTS = "settings-plugs-import-review-preview-counts"
    const val PLUGS_IMPORT_IMPORT_BUTTON = "settings-plugs-import-import-button"
    const val PLUGS_IMPORT_ERROR = "settings-plugs-import-error"
    const val PLUGS_IMPORT_SUCCESS = "settings-plugs-import-success"
    const val PLUGS_IMPORT_CLOSE_BUTTON = "settings-plugs-import-close-button"

    fun plugRow(plugId: String): String = "settings-plug-row:$plugId"

    const val MLTOOLS_STATUS = "settings-mltools-status"
    const val MLTOOLS_PROVISION_BUTTON = "settings-mltools-provision-button"
    const val MLTOOLS_DOWNLOAD_TASKS = "settings-mltools-download-tasks"
}
