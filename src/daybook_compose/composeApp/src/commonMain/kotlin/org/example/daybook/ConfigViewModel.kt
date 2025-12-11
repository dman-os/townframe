package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.LayoutWindowConfig

data class ConfigError(val message: String, val exception: FfiException)

class ConfigViewModel(
    val configRepo: ConfigRepoFfi
) : ViewModel() {
    // Layout config state
    private val _layoutConfig = MutableStateFlow<LayoutWindowConfig?>(null)
    val layoutConfig = _layoutConfig.asStateFlow()
    
    // Error state for showing snackbar
    private val _error = MutableStateFlow<ConfigError?>(null)
    val error = _error.asStateFlow()
    
    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null
    
    init {
        // Register listener for config changes first
        viewModelScope.launch {
            try {
                listenerRegistration = configRepo.ffiRegisterListener(object : org.example.daybook.uniffi.ConfigEventListener {
                    override fun onConfigEvent(event: org.example.daybook.uniffi.core.ConfigEvent) {
                        // Reload all settings when config changes
                        loadAllSettings()
                    }
                })
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
                _layoutConfig.value = configRepo.getLayout()
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to load settings: ${e.message}", e)
            }
        }
    }
    
    fun setLayout(value: LayoutWindowConfig) {
        viewModelScope.launch {
            try {
                configRepo.setLayout(value)
                _layoutConfig.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set layout: ${e.message}", e)
            }
        }
    }
}
