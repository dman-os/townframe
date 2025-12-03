package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.SidebarMode
import org.example.daybook.uniffi.core.SidebarPosition
import org.example.daybook.uniffi.core.SidebarVisibility
import org.example.daybook.uniffi.core.TabListVisibility
import org.example.daybook.uniffi.core.TableViewMode

data class ConfigError(val message: String, val exception: FfiException)

class ConfigViewModel(
    val configRepo: ConfigRepoFfi
) : ViewModel() {
    // Tab list visibility states
    private val _tabListVisExpanded = MutableStateFlow<TabListVisibility?>(null)
    val tabListVisExpanded = _tabListVisExpanded.asStateFlow()
    
    // Table view mode state
    private val _tableViewModeCompact = MutableStateFlow<TableViewMode?>(null)
    val tableViewModeCompact = _tableViewModeCompact.asStateFlow()
    
    // Table rail visibility states
    private val _tableRailVisCompact = MutableStateFlow<TabListVisibility?>(null)
    val tableRailVisCompact = _tableRailVisCompact.asStateFlow()
    
    private val _tableRailVisExpanded = MutableStateFlow<TabListVisibility?>(null)
    val tableRailVisExpanded = _tableRailVisExpanded.asStateFlow()
    
    // Sidebar visibility states
    private val _sidebarVisExpanded = MutableStateFlow<SidebarVisibility?>(null)
    val sidebarVisExpanded = _sidebarVisExpanded.asStateFlow()
    
    // Sidebar position states
    private val _sidebarPosExpanded = MutableStateFlow<SidebarPosition?>(null)
    val sidebarPosExpanded = _sidebarPosExpanded.asStateFlow()
    
    // Sidebar mode states
    private val _sidebarModeExpanded = MutableStateFlow<org.example.daybook.uniffi.core.SidebarMode?>(null)
    val sidebarModeExpanded = _sidebarModeExpanded.asStateFlow()
    
    // Sidebar auto-hide states
    private val _sidebarAutoHideExpanded = MutableStateFlow<Boolean?>(null)
    val sidebarAutoHideExpanded = _sidebarAutoHideExpanded.asStateFlow()
    
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
                _tabListVisExpanded.value = configRepo.getTabListVisExpanded()
                _tableViewModeCompact.value = configRepo.getTableViewModeCompact()
                _tableRailVisCompact.value = configRepo.getTableRailVisCompact()
                _tableRailVisExpanded.value = configRepo.getTableRailVisExpanded()
                _sidebarVisExpanded.value = configRepo.getSidebarVisExpanded()
                _sidebarPosExpanded.value = configRepo.getSidebarPosExpanded()
                _sidebarModeExpanded.value = configRepo.getSidebarModeExpanded()
                _sidebarAutoHideExpanded.value = configRepo.getSidebarAutoHideExpanded()
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to load settings: ${e.message}", e)
            }
        }
    }
    
    fun setTabListVisExpanded(value: TabListVisibility) {
        viewModelScope.launch {
            try {
                configRepo.setTabListVisExpanded(value)
                _tabListVisExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set tab list visibility (expanded): ${e.message}", e)
            }
        }
    }
    
    fun setTableViewModeCompact(value: TableViewMode) {
        viewModelScope.launch {
            try {
                configRepo.setTableViewModeCompact(value)
                _tableViewModeCompact.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set table view mode: ${e.message}", e)
            }
        }
    }
    
    fun setTableRailVisCompact(value: TabListVisibility) {
        viewModelScope.launch {
            try {
                configRepo.setTableRailVisCompact(value)
                _tableRailVisCompact.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set table rail visibility (compact): ${e.message}", e)
            }
        }
    }
    
    fun setTableRailVisExpanded(value: TabListVisibility) {
        viewModelScope.launch {
            try {
                configRepo.setTableRailVisExpanded(value)
                _tableRailVisExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set table rail visibility (expanded): ${e.message}", e)
            }
        }
    }
    
    fun setSidebarVisExpanded(value: SidebarVisibility) {
        viewModelScope.launch {
            try {
                configRepo.setSidebarVisExpanded(value)
                _sidebarVisExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set sidebar visibility (expanded): ${e.message}", e)
            }
        }
    }
    
    fun setSidebarPosExpanded(value: SidebarPosition) {
        viewModelScope.launch {
            try {
                configRepo.setSidebarPosExpanded(value)
                _sidebarPosExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set sidebar position (expanded): ${e.message}", e)
            }
        }
    }
    
    fun setSidebarModeExpanded(value: SidebarMode) {
        viewModelScope.launch {
            try {
                configRepo.setSidebarModeExpanded(value)
                _sidebarModeExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set sidebar mode (expanded): ${e.message}", e)
            }
        }
    }
    
    fun setSidebarAutoHideExpanded(value: Boolean) {
        viewModelScope.launch {
            try {
                configRepo.setSidebarAutoHideExpanded(value)
                _sidebarAutoHideExpanded.value = value
            } catch (e: FfiException) {
                _error.value = ConfigError("Failed to set sidebar auto-hide (expanded): ${e.message}", e)
            }
        }
    }
}
