package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.core.FacetDisplayHint
import org.example.daybook.uniffi.core.ListenerRegistration

data class ConfigError(val message: String, val exception: FfiException)

class ConfigViewModel(val configRepo: ConfigRepoFfi) : ViewModel() {
    // Error state for showing snackbar
    private val _error = MutableStateFlow<ConfigError?>(null)
    val error = _error.asStateFlow()

    // Meta table key configs
    private val _metaTableKeyConfigs = MutableStateFlow<Map<String, FacetDisplayHint>>(emptyMap())
    val metaTableKeyConfigs = _metaTableKeyConfigs.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    init {
        // Register listener for config changes first
        viewModelScope.launch {
            try {
                listenerRegistration =
                    configRepo.ffiRegisterListener(
                        object : org.example.daybook.uniffi.ConfigEventListener {
                            override fun onConfigEvent(
                                event: org.example.daybook.uniffi.core.ConfigEvent
                            ) {
                                // Reload all settings when config changes
                                loadAllSettings()
                            }
                        }
                    )
                // Load initial values after listener is registered
                loadAllSettings()
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to register config listener: ${e.message}", e)
                // Still try to load settings even if listener registration fails
                loadAllSettings()
            }
        }
    }

    override fun onCleared() {
        // Clean up registration
        listenerRegistration?.unregister()
        super.onCleared()
    }

    fun clearError() {
        _error.value = null
    }

    private fun loadAllSettings() {
        viewModelScope.launch {
            try {
                val configs = configRepo.listDisplayHints()
                _metaTableKeyConfigs.value = configs
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to load settings: ${e.message}", e)
            }
        }
    }

    suspend fun setFacetDisplayHint(key: String, config: FacetDisplayHint) {
        try {
            configRepo.setFacetDisplayHint(key, config)
            // Reload to get updated configs
            loadAllSettings()
        } catch (e: FfiException) {
            _error.value = ConfigError("Failed to save config for $key: ${e.message}", e)
        }
    }
}
