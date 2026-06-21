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

    fun plugRow(plugId: String): String = "settings-plug-row:$plugId"

    const val MLTOOLS_STATUS = "settings-mltools-status"
    const val MLTOOLS_PROVISION_BUTTON = "settings-mltools-provision-button"
    const val MLTOOLS_DOWNLOAD_TASKS = "settings-mltools-download-tasks"
}
