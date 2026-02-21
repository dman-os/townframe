package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.ProgressEventListener
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.core.FacetDisplayHint
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.ProgressEvent
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets
import org.example.daybook.uniffi.core.ProgressTask

data class ConfigError(val message: String, val exception: FfiException)

data class MltoolsBackendRow(val backend: String, val details: String)

data class MltoolsConfigSummary(
    val ocr: List<MltoolsBackendRow> = emptyList(),
    val embed: List<MltoolsBackendRow> = emptyList(),
    val llm: List<MltoolsBackendRow> = emptyList()
)

sealed interface MltoolsProvisionState {
    data object Idle : MltoolsProvisionState

    data object Running : MltoolsProvisionState

    data class Failed(val message: String) : MltoolsProvisionState

    data object Succeeded : MltoolsProvisionState
}

class ConfigViewModel(
    val configRepo: ConfigRepoFfi,
    private val progressRepo: ProgressRepoFfi
) : ViewModel() {
    private val _error = MutableStateFlow<ConfigError?>(null)
    val error = _error.asStateFlow()

    private val _metaTableKeyConfigs = MutableStateFlow<Map<String, FacetDisplayHint>>(emptyMap())
    val metaTableKeyConfigs = _metaTableKeyConfigs.asStateFlow()

    private val _mltoolsConfig = MutableStateFlow(MltoolsConfigSummary())
    val mltoolsConfig = _mltoolsConfig.asStateFlow()

    private val _mltoolsDownloadTasks = MutableStateFlow<List<ProgressTask>>(emptyList())
    val mltoolsDownloadTasks = _mltoolsDownloadTasks.asStateFlow()

    private val _mltoolsProvisionState =
        MutableStateFlow<MltoolsProvisionState>(MltoolsProvisionState.Idle)
    val mltoolsProvisionState = _mltoolsProvisionState.asStateFlow()

    private var configListenerRegistration: ListenerRegistration? = null
    private var progressListenerRegistration: ListenerRegistration? = null

    init {
        viewModelScope.launch {
            try {
                configListenerRegistration =
                    configRepo.ffiRegisterListener(
                        object : org.example.daybook.uniffi.ConfigEventListener {
                            override fun onConfigEvent(event: org.example.daybook.uniffi.core.ConfigEvent) {
                                loadAllSettings()
                            }
                        }
                    )
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to register config listener: ${e.message}", e)
            }
        }

        viewModelScope.launch {
            try {
                progressListenerRegistration =
                    progressRepo.ffiRegisterListener(
                        object : ProgressEventListener {
                            override fun onProgressEvent(event: ProgressEvent) {
                                when (event) {
                                    is ProgressEvent.ListChanged,
                                    is ProgressEvent.TaskRemoved,
                                    is ProgressEvent.TaskUpserted,
                                    is ProgressEvent.UpdateAdded -> refreshMltoolsDownloadTasks()
                                }
                            }
                        }
                    )
            } catch (e: FfiException) {
                _error.value =
                    ConfigError("Failed to register progress listener: ${e.message}", e)
            }
        }

        loadAllSettings()
    }

    override fun onCleared() {
        configListenerRegistration?.unregister()
        progressListenerRegistration?.unregister()
        super.onCleared()
    }

    fun clearError() {
        _error.value = null
    }

    private fun loadAllSettings() {
        viewModelScope.launch {
            try {
                _metaTableKeyConfigs.value = configRepo.listDisplayHints()
                _mltoolsConfig.value = parseMltoolsConfig(configRepo.getMltoolsConfigJson())
                refreshMltoolsDownloadTasks()
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to load settings: ${e.message}", e)
            }
        }
    }

    suspend fun setFacetDisplayHint(key: String, config: FacetDisplayHint) {
        try {
            configRepo.setFacetDisplayHint(key, config)
            loadAllSettings()
        } catch (e: FfiException) {
            _error.value = ConfigError("Failed to save config for $key: ${e.message}", e)
        }
    }

    fun provisionMobileDefaultMltools() {
        viewModelScope.launch {
            _mltoolsProvisionState.value = MltoolsProvisionState.Running
            try {
                configRepo.provisionMobileDefaultMltools(progressRepo)
                _mltoolsProvisionState.value = MltoolsProvisionState.Succeeded
                _mltoolsConfig.value = parseMltoolsConfig(configRepo.getMltoolsConfigJson())
                refreshMltoolsDownloadTasks()
            } catch (e: FfiException) {
                _mltoolsProvisionState.value =
                    MltoolsProvisionState.Failed(e.message ?: "unknown error")
                _error.value =
                    ConfigError("Failed to provision mobile_default models: ${e.message}", e)
                refreshMltoolsDownloadTasks()
            }
        }
    }

    fun refreshMltoolsDownloadTasks() {
        viewModelScope.launch {
            try {
                val tasks = progressRepo.listByTagPrefix("/mltools/model")
                _mltoolsDownloadTasks.value = tasks
                _mltoolsProvisionState.value =
                    reconcileMltoolsProvisionState(_mltoolsProvisionState.value, tasks)
            } catch (e: FfiException) {
                _error.value =
                    ConfigError("Failed to load MLTools download tasks: ${e.message}", e)
            }
        }
    }

    private fun parseMltoolsConfig(configJson: String): MltoolsConfigSummary {
        val root = Json.parseToJsonElement(configJson).jsonObject
        return MltoolsConfigSummary(
            ocr = parseBackendRows(root, "ocr"),
            embed = parseBackendRows(root, "embed"),
            llm = parseBackendRows(root, "llm")
        )
    }

    private fun parseBackendRows(root: JsonObject, section: String): List<MltoolsBackendRow> {
        val sectionObj = root[section]?.jsonObject ?: return emptyList()
        val backends = sectionObj["backends"]?.jsonArray ?: return emptyList()
        return backends.map { backend ->
            val backendObj = backend.jsonObject
            val (backendType, backendValue) = backendObj.entries.firstOrNull()
                ?: return@map MltoolsBackendRow("Unknown", "")
            MltoolsBackendRow(
                backend = backendType,
                details = backendSummary(backendValue)
            )
        }
    }

    private fun backendSummary(value: JsonElement): String {
        val obj = value as? JsonObject ?: return value.toString().trim('"')
        val fields =
            obj.entries.take(3).joinToString(" | ") { (key, jsonValue) ->
                val text =
                    when (jsonValue) {
                        is JsonArray -> "${jsonValue.size} items"
                        is JsonObject -> "{...}"
                        else -> jsonValue.toString().trim('"')
                    }
                "$key=$text"
            }
        return if (fields.isEmpty()) "configured" else fields
    }

    private fun reconcileMltoolsProvisionState(
        current: MltoolsProvisionState,
        tasks: List<ProgressTask>
    ): MltoolsProvisionState {
        if (tasks.any { it.state == ProgressTaskState.ACTIVE }) {
            return MltoolsProvisionState.Running
        }

        val latestFailed = tasks
            .filter { it.state == ProgressTaskState.FAILED }
            .firstOrNull()
        if (latestFailed != null) {
            val failedMessage =
                (latestFailed.latestUpdate?.update?.deets as? ProgressUpdateDeets.Completed)
                    ?.message
                    ?: "model provisioning failed"
            return MltoolsProvisionState.Failed(failedMessage)
        }

        if (tasks.any { it.state == ProgressTaskState.SUCCEEDED }) {
            return MltoolsProvisionState.Succeeded
        }

        return if (current is MltoolsProvisionState.Running) {
            MltoolsProvisionState.Idle
        } else {
            current
        }
    }
}
