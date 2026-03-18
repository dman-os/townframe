@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

// FIXME: remove usage of Result

package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.FastOutSlowInEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.ArrowBack
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.FolderOpen
import androidx.compose.material.icons.filled.CreateNewFolder
import androidx.compose.material.icons.filled.QrCodeScanner
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ElevatedCard
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LargeTopAppBar
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import daybook.composeapp.generated.resources.Res
import daybook.composeapp.generated.resources.compose_multiplatform
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import io.github.vinceglb.filekit.dialogs.compose.rememberDirectoryPickerLauncher
import io.github.vinceglb.filekit.path
import org.example.daybook.capture.CameraCaptureContext
import org.example.daybook.capture.ProvideCameraCaptureContext
import org.example.daybook.capture.data.CameraOverlay
import org.example.daybook.capture.data.CameraPreviewQrBridge
import org.example.daybook.capture.data.CameraQrOverlayBridge
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.capture.ui.DaybookCameraViewport
import org.example.daybook.drawer.DrawerScreen
import org.example.daybook.progress.ProgressList
import org.example.daybook.progress.ProgressAmountBlock
import org.example.daybook.settings.SettingsScreen
import org.example.daybook.tables.CompactLayout
import org.example.daybook.tables.ExpandedLayout
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.CameraQrAnalyzerFfi
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.DispatchRepoFfi
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.AppFfiCtx
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.RtFfi
import org.example.daybook.uniffi.SyncRepoFfi
import org.example.daybook.uniffi.CloneBootstrapInfo
import org.example.daybook.uniffi.TablesEventListener
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.KnownRepoEntry
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.Panel
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets
import org.example.daybook.uniffi.core.Tab
import org.example.daybook.uniffi.core.Table
import org.example.daybook.uniffi.core.TablesEvent
import org.example.daybook.uniffi.core.Uuid
import org.example.daybook.uniffi.core.Window
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

enum class DaybookNavigationType {
    BOTTOM_NAVIGATION,
    NAVIGATION_RAIL,
    PERMANENT_NAVIGATION_DRAWER
}

enum class DaybookContentType {
    LIST_ONLY,
    LIST_AND_DETAIL
}

val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val requestAllPermissions: () -> Unit = {}
) {
    val hasAll = hasCamera and hasNotifications and hasMicrophone and hasOverlay
}

data class AppContainer(
    val ffiCtx: FfiCtx,
    val drawerRepo: DrawerRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val dispatchRepo: DispatchRepoFfi,
    val progressRepo: ProgressRepoFfi,
    val rtFfi: RtFfi,
    val plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi,
    val configRepo: ConfigRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val syncRepo: SyncRepoFfi,
    val cameraPreviewFfi: CameraPreviewFfi
)

val LocalContainer =
    staticCompositionLocalOf<AppContainer> {
        error("no AppContainer provided")
    }

data class AppConfig(val theme: ThemeConfig = ThemeConfig.Dark)

enum class AppScreens {
    Home,
    Capture,
    Tables,
    Progress,
    Settings,
    Drawer
}

private sealed interface AppInitState {
    data object Loading : AppInitState

    data class Welcome(val repos: List<KnownRepoEntry>) : AppInitState

    data class OpeningRepo(val repoPath: String) : AppInitState

    data class Ready(val container: AppContainer) : AppInitState

    data class Error(val throwable: Throwable) : AppInitState
}

private sealed interface CloneUiState {
    data class UrlInput(
        val urlInput: String = "",
        val isResolving: Boolean = false,
        val errorMessage: String? = null
    ) : CloneUiState

    data class Scanner(
        val currentUrlInput: String,
        val errorMessage: String? = null
    ) : CloneUiState

    data class PickingLocation(
        val sourceUrl: String,
        val info: CloneBootstrapInfo,
        val destinationPath: String,
        val isCloning: Boolean = false,
        val errorMessage: String? = null,
        val destinationWarning: String? = null
    ) : CloneUiState

    data class Syncing(
        val sourceUrl: String
    ) : CloneUiState
}

sealed interface TablesState {
    data class Data(
        val windows: Map<Uuid, Window>,
        val tabs: Map<Uuid, Tab>,
        val panels: Map<Uuid, Panel>,
        val tables: Map<Uuid, Table>
    ) : TablesState {
        // Convenience properties for UI
        val windowsList: List<Window> get() = windows.values.toList()
        val tabsList: List<Tab> get() = tabs.values.toList()
        val panelsList: List<Panel> get() = panels.values.toList()
        val tablesList: List<Table> get() = tables.values.toList()
    }

    data class Error(val error: FfiException) : TablesState

    object Loading : TablesState
}

private data class TablesRefreshIntent(
    val refreshAll: Boolean = false,
    val windows: Set<Uuid> = emptySet(),
    val tabs: Set<Uuid> = emptySet(),
    val panels: Set<Uuid> = emptySet(),
    val tables: Set<Uuid> = emptySet()
) {
    fun merge(other: TablesRefreshIntent): TablesRefreshIntent =
        TablesRefreshIntent(
            refreshAll = refreshAll || other.refreshAll,
            windows = windows + other.windows,
            tabs = tabs + other.tabs,
            panels = panels + other.panels,
            tables = tables + other.tables
        )

    companion object {
        val Full = TablesRefreshIntent(refreshAll = true)
    }
}

class TablesViewModel(val tablesRepo: TablesRepoFfi) : ViewModel() {
    private val _tablesState = MutableStateFlow(TablesState.Loading as TablesState)
    val tablesState = _tablesState.asStateFlow()

    // Selected table state
    private val _selectedTableId = MutableStateFlow<Uuid?>(null)
    val selectedTableId = _selectedTableId.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    private val refreshRunner =
        CoalescingIntentRunner<TablesRefreshIntent>(
            scope = viewModelScope,
            debounceMs = 60,
            merge = { left: TablesRefreshIntent, right: TablesRefreshIntent -> left.merge(right) },
            onIntent = { intent: TablesRefreshIntent -> applyRefreshIntent(intent) }
        )

    // Listener instance implemented on Kotlin side
    private val listener =
        object : TablesEventListener {
            override fun onTablesEvent(event: TablesEvent) {
                when (event) {
                    is TablesEvent.ListChanged -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.WindowAdded -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.WindowChanged ->
                        refreshRunner.submit(TablesRefreshIntent(windows = setOf(event.id)))

                    is TablesEvent.TabAdded -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.TabChanged ->
                        refreshRunner.submit(TablesRefreshIntent(tabs = setOf(event.id)))

                    is TablesEvent.PanelAdded -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.PanelChanged ->
                        refreshRunner.submit(TablesRefreshIntent(panels = setOf(event.id)))

                    is TablesEvent.TableAdded -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.TableChanged ->
                        refreshRunner.submit(TablesRefreshIntent(tables = setOf(event.id)))
                    is TablesEvent.WindowDeleted -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.TabDeleted -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.PanelDeleted -> refreshRunner.submit(TablesRefreshIntent.Full)
                    is TablesEvent.TableDeleted -> refreshRunner.submit(TablesRefreshIntent.Full)
                }
            }
        }

    init {
        refreshRunner.submit(TablesRefreshIntent.Full)
        // Register listener
        viewModelScope.launch {
            listenerRegistration = tablesRepo.ffiRegisterListener(listener)
        }
    }

    private suspend fun applyRefreshIntent(intent: TablesRefreshIntent) {
        val hasTargetedUpdates =
            intent.windows.isNotEmpty() ||
                intent.tabs.isNotEmpty() ||
                intent.panels.isNotEmpty() ||
                intent.tables.isNotEmpty()

        if (intent.refreshAll || _tablesState.value !is TablesState.Data || !hasTargetedUpdates) {
            refreshTables()
            return
        }

        intent.windows.forEach { updateWindow(it) }
        intent.tabs.forEach { updateTab(it) }
        intent.panels.forEach { updatePanel(it) }
        intent.tables.forEach { updateTable(it) }
    }

    private suspend fun refreshTables() {
        val hadData = _tablesState.value is TablesState.Data
        if (!hadData) {
            _tablesState.value = TablesState.Loading
        }
        try {
            val windows = tablesRepo.listWindows()
            val tabs = tablesRepo.listTabs()
            val panels = tablesRepo.listPanels()
            val tables = tablesRepo.listTables()
            _tablesState.value =
                TablesState.Data(
                    windows = windows.associateBy { it.id },
                    tabs = tabs.associateBy { it.id },
                    panels = panels.associateBy { it.id },
                    tables = tables.associateBy { it.id }
                )

            // Auto-select first table if none is selected
            if (_selectedTableId.value == null && tables.isNotEmpty()) {
                _selectedTableId.value = tables.first().id
            }
        } catch (err: FfiException) {
            _tablesState.value = TablesState.Error(err)
        }
    }

    // Targeted update methods for efficient updates
    private suspend fun updateWindow(windowId: Uuid) {
        try {
            val currentState = _tablesState.value
            if (currentState is TablesState.Data) {
                val updatedWindow = tablesRepo.getWindow(windowId)
                val updatedWindows = currentState.windows.toMutableMap()

                if (updatedWindow != null) {
                    updatedWindows[windowId] = updatedWindow
                } else {
                    // Window was deleted, remove from map
                    updatedWindows.remove(windowId)
                }

                _tablesState.value = currentState.copy(windows = updatedWindows)
            }
        } catch (e: FfiException) {
            _tablesState.value = TablesState.Error(e)
        }
    }

    private suspend fun updateTab(tabId: Uuid) {
        try {
            val currentState = _tablesState.value
            if (currentState is TablesState.Data) {
                val updatedTab = tablesRepo.getTab(tabId)
                val updatedTabs = currentState.tabs.toMutableMap()

                if (updatedTab != null) {
                    updatedTabs[tabId] = updatedTab
                } else {
                    // Tab was deleted, remove from map
                    updatedTabs.remove(tabId)
                }

                _tablesState.value = currentState.copy(tabs = updatedTabs)
            }
        } catch (e: FfiException) {
            _tablesState.value = TablesState.Error(e)
        }
    }

    private suspend fun updatePanel(panelId: Uuid) {
        try {
            val currentState = _tablesState.value
            if (currentState is TablesState.Data) {
                val updatedPanel = tablesRepo.getPanel(panelId)
                val updatedPanels = currentState.panels.toMutableMap()

                if (updatedPanel != null) {
                    updatedPanels[panelId] = updatedPanel
                } else {
                    // Panel was deleted, remove from map
                    updatedPanels.remove(panelId)
                }

                _tablesState.value = currentState.copy(panels = updatedPanels)
            }
        } catch (e: FfiException) {
            _tablesState.value = TablesState.Error(e)
        }
    }

    private suspend fun updateTable(tableId: Uuid) {
        try {
            val currentState = _tablesState.value
            if (currentState is TablesState.Data) {
                val updatedTable = tablesRepo.getTable(tableId)
                val updatedTables = currentState.tables.toMutableMap()

                if (updatedTable != null) {
                    updatedTables[tableId] = updatedTable
                } else {
                    // Table was deleted, remove from map
                    updatedTables.remove(tableId)
                }

                _tablesState.value = currentState.copy(tables = updatedTables)
            }
        } catch (e: FfiException) {
            _tablesState.value = TablesState.Error(e)
        }
    }

    fun loadTables() {
        viewModelScope.launch {
            refreshTables()
        }
    }

    fun selectTable(tableId: Uuid) {
        _selectedTableId.value = tableId
    }

    fun selectTab(tabId: Uuid) {
        viewModelScope.launch {
            val currentState = _tablesState.value
            val selectedTableId = _selectedTableId.value
            if (currentState is TablesState.Data && selectedTableId != null) {
                val table = currentState.tables[selectedTableId]
                if (table != null) {
                    val updatedTable = table.copy(selectedTab = tabId)
                    try {
                        // Persist selection to repo
                        tablesRepo.setTable(selectedTableId, updatedTable)
                        // Update local state optimistically
                        val updatedTables = currentState.tables.toMutableMap()
                        updatedTables[selectedTableId] = updatedTable
                        _tablesState.value = currentState.copy(tables = updatedTables)
                    } catch (e: FfiException) {
                        _tablesState.value = TablesState.Error(e)
                    }
                }
            }
        }
    }

    suspend fun getSelectedTable(): Table? {
        val currentState = _tablesState.value
        val selectedId = _selectedTableId.value
        return if (currentState is TablesState.Data && selectedId != null) {
            currentState.tables[selectedId]
        } else {
            tablesRepo.getSelectedTable()
        }
    }

    suspend fun getTabsForSelectedTable(): List<Tab> {
        val selectedTable = getSelectedTable()
        val currentState = _tablesState.value
        return if (selectedTable != null && currentState is TablesState.Data) {
            selectedTable.tabs.mapNotNull { tabId -> currentState.tabs[tabId] }
        } else {
            emptyList()
        }
    }

    suspend fun initializeFirstTime(): Result<Unit> = try {
        // The auto-creation logic is now handled in the Rust code
        // Just trigger a refresh to ensure we have data
        refreshTables()
        Result.success(Unit)
    } catch (err: FfiException) {
        Result.failure(err)
    }

    suspend fun createNewTable(): Result<Uuid> = try {
        val newTableId = tablesRepo.createNewTable()
        // Select the new table
        _selectedTableId.value = newTableId
        Result.success(newTableId)
    } catch (err: FfiException) {
        Result.failure(err)
    }

    suspend fun createNewTab(tableId: Uuid): Result<Uuid> = try {
        val newTabId = tablesRepo.createNewTab(tableId)
        Result.success(newTabId)
    } catch (err: FfiException) {
        Result.failure(err)
    }

    suspend fun removeTab(tabId: Uuid): Result<Unit> = try {
        tablesRepo.removeTab(tabId)
        Result.success(Unit)
    } catch (err: FfiException) {
        Result.failure(err)
    }

    override fun onCleared() {
        refreshRunner.cancel()
        // Clean up registration
        listenerRegistration?.unregister()
        super.onCleared()
    }
}

@Composable
@Preview
fun App(
    config: AppConfig = AppConfig(),
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController = rememberNavController()
) {
    var initAttempt by remember { mutableStateOf(0) }
    var initState by remember { mutableStateOf<AppInitState>(AppInitState.Loading) }
    var pendingOpenRepoPath by remember { mutableStateOf<String?>(null) }
    var cloneUiState by remember { mutableStateOf<CloneUiState?>(null) }
    var cloneSourceUrlPendingOpen by remember { mutableStateOf<String?>(null) }
    var cloneInitRequest by remember { mutableStateOf<Pair<String, String>?>(null) }
    var selectedWelcomeRepo by remember { mutableStateOf<KnownRepoEntry?>(null) }
    var pendingForgetRepoId by remember { mutableStateOf<String?>(null) }
    val cloneCameraPreviewFfi = remember { CameraPreviewFfi.load() }

    androidx.compose.runtime.DisposableEffect(Unit) {
        onDispose {
            cloneCameraPreviewFfi.close()
        }
    }

    LaunchedEffect(initAttempt) {
        initState = AppInitState.Loading
        selectedWelcomeRepo = null
        try {
            val globalsCtx = AppFfiCtx.init()
            val repoConfig = globalsCtx.getRepoConfig()
            val knownRepos = repoConfig.knownRepos
            val lastUsedRepo =
                repoConfig.lastUsedRepoId?.let { lastUsedRepoId ->
                    knownRepos.find { repo -> repo.id == lastUsedRepoId }
                }
            val shouldOpenLastUsedRepo = lastUsedRepo != null && globalsCtx.isRepoUsable(lastUsedRepo.path)
            globalsCtx.close()

            if (shouldOpenLastUsedRepo) {
                pendingOpenRepoPath = lastUsedRepo.path
                initState = AppInitState.OpeningRepo(repoPath = lastUsedRepo.path)
            } else {
                initState = AppInitState.Welcome(repos = knownRepos)
            }
        } catch (throwable: Throwable) {
            initState = AppInitState.Error(throwable)
        }
    }

    LaunchedEffect(pendingOpenRepoPath) {
        val repoPath = pendingOpenRepoPath ?: return@LaunchedEffect
        try {
            initState = AppInitState.OpeningRepo(repoPath = repoPath)
            val gcx = AppFfiCtx.init()
            val fcx = FfiCtx.init(repoPath, gcx)
            gcx.close()
            val tablesRepo = TablesRepoFfi.load(fcx = fcx)
            val blobsRepo =
                org.example.daybook.uniffi.BlobsRepoFfi
                    .load(fcx = fcx)
            val plugsRepo =
                org.example.daybook.uniffi.PlugsRepoFfi
                    .load(fcx = fcx, blobsRepo = blobsRepo)
            val drawerRepo = DrawerRepoFfi.load(fcx = fcx, plugsRepo = plugsRepo)
            val configRepo = ConfigRepoFfi.load(fcx = fcx, plugRepo = plugsRepo)
            val dispatchRepo = DispatchRepoFfi.load(fcx = fcx)
            val progressRepo = ProgressRepoFfi.load(fcx = fcx)
            val syncRepo =
                SyncRepoFfi.load(
                    fcx = fcx,
                    configRepo = configRepo,
                    blobsRepo = blobsRepo,
                    drawerRepo = drawerRepo,
                    progressRepo = progressRepo
                )
            val rtFfi =
                RtFfi.load(
                    fcx = fcx,
                    drawerRepo = drawerRepo,
                    plugsRepo = plugsRepo,
                    dispatchRepo = dispatchRepo,
                    progressRepo = progressRepo,
                    blobsRepo = blobsRepo,
                    configRepo = configRepo,
                    deviceId = "compose-client"
                )
            val cameraPreviewFfi = CameraPreviewFfi.load()

            val tablesViewModel = TablesViewModel(tablesRepo)
            tablesViewModel.initializeFirstTime()

            initState =
                AppInitState.Ready(
                    AppContainer(
                        ffiCtx = fcx,
                        drawerRepo = drawerRepo,
                        tablesRepo = tablesRepo,
                        dispatchRepo = dispatchRepo,
                        progressRepo = progressRepo,
                        rtFfi = rtFfi,
                        plugsRepo = plugsRepo,
                        configRepo = configRepo,
                        blobsRepo = blobsRepo,
                        syncRepo = syncRepo,
                        cameraPreviewFfi = cameraPreviewFfi
                    )
                )
        } catch (throwable: Throwable) {
            initState = AppInitState.Error(throwable)
        } finally {
            pendingOpenRepoPath = null
        }
    }

    LaunchedEffect(pendingForgetRepoId) {
        val repoId = pendingForgetRepoId ?: return@LaunchedEffect
        try {
            val gcx = AppFfiCtx.init()
            gcx.forgetKnownRepo(repoId)
            val repoConfig = gcx.getRepoConfig()
            gcx.close()
            selectedWelcomeRepo = null
            initState = AppInitState.Welcome(repos = repoConfig.knownRepos)
        } catch (throwable: Throwable) {
            initState = AppInitState.Error(throwable)
        } finally {
            pendingForgetRepoId = null
        }
    }

    DaybookTheme(themeConfig = config.theme) {
        when (val state = initState) {
            is AppInitState.Loading -> {
                LoadingScreen()
            }

            is AppInitState.Welcome -> {
                val repoDetail = selectedWelcomeRepo
                if (repoDetail == null) {
                    WelcomeScreen(
                        repos = state.repos,
                        onOpenRepo = { selectedRepoPath ->
                            pendingOpenRepoPath = selectedRepoPath
                        },
                        onInspectRepo = { repo ->
                            selectedWelcomeRepo = repo
                        },
                        onStartClone = { cloneUiState = CloneUiState.UrlInput() }
                    )
                } else {
                    WelcomeRepoDetailScreen(
                        repo = repoDetail,
                        onBack = { selectedWelcomeRepo = null },
                        onOpen = { pendingOpenRepoPath = repoDetail.path },
                        onForget = { pendingForgetRepoId = repoDetail.id },
                        forgetting = pendingForgetRepoId == repoDetail.id
                    )
                }
            }

            is AppInitState.OpeningRepo -> {
                LoadingScreen(message = "Opening repo: ${state.repoPath}")
            }

            is AppInitState.Error -> {
                ErrorScreen(
                    title = "Failed to initialize",
                    message = state.throwable.message ?: "Unknown error",
                    onRetry = { initAttempt += 1 }
                )
            }

            is AppInitState.Ready -> {
                val appContainer = state.container

                // Ensure FFI resources are closed when the composition leaves
                androidx.compose.runtime.DisposableEffect(appContainer) {
                    onDispose {
                        appContainer.drawerRepo.close()
                        appContainer.tablesRepo.close()
                        appContainer.dispatchRepo.close()
                        appContainer.progressRepo.close()
                        appContainer.rtFfi.close()
                        appContainer.plugsRepo.close()
                        appContainer.configRepo.close()
                        appContainer.syncRepo.close()
                        appContainer.cameraPreviewFfi.close()
                        appContainer.ffiCtx.close()
                    }
                }

                CompositionLocalProvider(
                    LocalContainer provides appContainer
                ) {
                    if (cloneUiState is CloneUiState.Syncing) {
                        CloneSyncScreen(
                            onSyncInBackground = { cloneUiState = null },
                            onRetry = {
                                val sourceUrl = (cloneUiState as CloneUiState.Syncing).sourceUrl
                                cloneSourceUrlPendingOpen = sourceUrl
                            }
                        )
                    } else {
                        // Provide camera capture context for coordination between camera and bottom bar
                        val cameraCaptureContext = remember { CameraCaptureContext() }
                        val chromeStateManager = remember { ChromeStateManager() }
                        ProvideCameraCaptureContext(cameraCaptureContext) {
                            CompositionLocalProvider(
                                LocalChromeStateManager provides chromeStateManager
                            ) {
                                AdaptiveAppLayout(
                                    modifier = surfaceModifier,
                                    navController = navController,
                                    extraAction = extraAction
                                )
                            }
                        }
                    }
                }
            }
        }

        when (val state = cloneUiState) {
            null -> {}
            is CloneUiState.UrlInput -> {
                CloneUrlScreen(
                    state = state,
                    onBack = { cloneUiState = null },
                    onUrlChange = { next ->
                        cloneUiState = state.copy(urlInput = next, errorMessage = null)
                    },
                    onOpenScanner = {
                        cloneUiState =
                            CloneUiState.Scanner(
                                currentUrlInput = state.urlInput
                            )
                    },
                    onContinue = { sourceUrl ->
                        cloneUiState = state.copy(isResolving = true, errorMessage = null)
                        cloneSourceUrlPendingOpen = sourceUrl
                    }
                )
                if (state.isResolving) {
                    val sourceUrl = cloneSourceUrlPendingOpen
                    if (sourceUrl != null) {
                        LaunchedEffect(sourceUrl) {
                            try {
                                val gcx = AppFfiCtx.init()
                                val info = gcx.resolveCloneUrl(sourceUrl)
                                val defaultParent =
                                    runCatching { gcx.defaultCloneParentDir() }
                                        .getOrDefault("")
                                gcx.close()
                                val initialRepoName =
                                    if (info.repoName.isNotBlank()) info.repoName else "daybook-repo"
                                cloneUiState =
                                    CloneUiState.PickingLocation(
                                        sourceUrl = sourceUrl,
                                        info = info,
                                        destinationPath =
                                            if (defaultParent.isBlank()) {
                                                initialRepoName
                                            } else {
                                                "$defaultParent/$initialRepoName"
                                            }
                                    )
                            } catch (error: Throwable) {
                                cloneUiState =
                                    state.copy(
                                        isResolving = false,
                                        errorMessage = "Resolve failed: ${describeThrowable(error)}"
                                    )
                            } finally {
                                cloneSourceUrlPendingOpen = null
                            }
                        }
                    }
                }
            }
            is CloneUiState.Scanner -> {
                CloneQrScannerScreen(
                    cameraPreviewFfi = cloneCameraPreviewFfi,
                    onBack = {
                        cloneUiState = CloneUiState.UrlInput(urlInput = state.currentUrlInput)
                    },
                    onDetectedUrl = { detectedUrl ->
                        cloneUiState =
                            CloneUiState.UrlInput(
                                urlInput = detectedUrl,
                                errorMessage = null
                            )
                    }
                )
            }
            is CloneUiState.PickingLocation -> {
                CloneLocationScreen(
                    state = state,
                    onBack = {
                        cloneUiState =
                            CloneUiState.UrlInput(
                                urlInput = state.sourceUrl
                            )
                    },
                    onDestinationChange = { next ->
                        cloneUiState =
                            state.copy(
                                destinationPath = next,
                                errorMessage = null,
                                destinationWarning = null
                            )
                    },
                    onContinue = { destinationPath ->
                        cloneUiState =
                            state.copy(
                                destinationPath = destinationPath,
                                isCloning = true,
                                errorMessage = null
                            )
                        val sourceUrl = state.sourceUrl
                        cloneInitRequest = sourceUrl to destinationPath
                    }
                )
                LaunchedEffect(state.destinationPath) {
                    val destination = state.destinationPath.trim()
                    if (destination.isBlank()) {
                        cloneUiState = state.copy(destinationWarning = null)
                        return@LaunchedEffect
                    }
                    try {
                        val gcx = AppFfiCtx.init()
                        val check = gcx.checkCloneDestination(destination)
                        gcx.close()
                        val warning =
                            when {
                                !check.exists -> null
                                !check.isDir -> "Destination exists and is not a directory."
                                !check.isEmpty -> "Destination directory is not empty."
                                else -> null
                            }
                        cloneUiState = state.copy(destinationWarning = warning)
                    } catch (_: Throwable) {
                        cloneUiState = state.copy(destinationWarning = null)
                    }
                }
                if (state.isCloning) {
                    val request = cloneInitRequest
                    if (request != null) {
                        LaunchedEffect(request.first, request.second) {
                            try {
                                val gcx = AppFfiCtx.init()
                                val preflight = gcx.checkCloneDestination(request.second)
                                if (preflight.exists && preflight.isDir && !preflight.isEmpty) {
                                    gcx.close()
                                    cloneUiState =
                                        state.copy(
                                            isCloning = false,
                                            errorMessage = "Destination directory is not empty. Choose an empty directory.",
                                            destinationWarning = "Destination directory is not empty."
                                        )
                                    return@LaunchedEffect
                                }
                                val out = gcx.cloneRepoInitFromUrl(request.first, request.second)
                                gcx.close()
                                cloneSourceUrlPendingOpen = request.first
                                pendingOpenRepoPath = out.repoPath
                            } catch (error: Throwable) {
                                cloneUiState =
                                    state.copy(
                                        isCloning = false,
                                        errorMessage = "Clone initialization failed: ${describeThrowable(error)}"
                                    )
                            } finally {
                                cloneInitRequest = null
                            }
                        }
                    }
                }
            }
            is CloneUiState.Syncing -> {}
        }
    }

    LaunchedEffect(initState, cloneSourceUrlPendingOpen) {
        val ready = initState as? AppInitState.Ready ?: return@LaunchedEffect
        val sourceUrl = cloneSourceUrlPendingOpen ?: return@LaunchedEffect
        cloneUiState = CloneUiState.Syncing(sourceUrl = sourceUrl)
        cloneSourceUrlPendingOpen = null
        try {
            ready.container.syncRepo.connectUrl(sourceUrl)
        } catch (_: Throwable) {
            // Retry is available on the sync screen.
        }
    }
}

@Composable
fun AdaptiveAppLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null
) {
    val platform = getPlatform()
    val screenWidth = platform.getScreenWidthDp()

    val navigationType: DaybookNavigationType
    val contentType: DaybookContentType

    when {
        screenWidth.value < 600f -> {
            // Compact screens (phones in portrait)
            navigationType = DaybookNavigationType.BOTTOM_NAVIGATION
            contentType = DaybookContentType.LIST_ONLY
        }

        screenWidth.value < 840f -> {
            // Medium screens (phones in landscape, small tablets)
            navigationType = DaybookNavigationType.NAVIGATION_RAIL
            contentType = DaybookContentType.LIST_ONLY
        }

        else -> {
            // Expanded screens (tablets, desktop)
            navigationType = DaybookNavigationType.PERMANENT_NAVIGATION_DRAWER
            contentType = DaybookContentType.LIST_AND_DETAIL
        }
    }

    DaybookHomeScreen(
        navigationType = navigationType,
        contentType = contentType,
        navController = navController,
        extraAction = extraAction,
        modifier = modifier
    )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun DaybookHomeScreen(
    navigationType: DaybookNavigationType,
    contentType: DaybookContentType,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    modifier: Modifier = Modifier
) {
    when (navigationType) {
        DaybookNavigationType.PERMANENT_NAVIGATION_DRAWER -> {
            ExpandedLayout(
                modifier = modifier,
                navController = navController,
                extraAction = extraAction,
                contentType = contentType
            )
        }

        DaybookNavigationType.NAVIGATION_RAIL -> {
            ExpandedLayout(
                modifier = modifier,
                navController = navController,
                extraAction = extraAction,
                contentType = contentType
            )
        }

        DaybookNavigationType.BOTTOM_NAVIGATION -> {
            CompactLayout(
                modifier = modifier,
                navController = navController,
                extraAction = extraAction,
                contentType = contentType
            )
        }
    }
}

@Composable
fun TablesScreen(modifier: Modifier = Modifier) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm =
        viewModel {
            TablesViewModel(tablesRepo = tablesRepo)
        }

    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    // Get selected table from state instead of calling suspend function
    val selectedTable =
        if (tablesState is TablesState.Data && selectedTableId != null) {
            tablesState.tables[selectedTableId]
        } else {
            null
        }

    val tabsForSelectedTable =
        if (selectedTable != null && tablesState is TablesState.Data) {
            selectedTable.tabs.mapNotNull { tabId -> tablesState.tabs[tabId] }
        } else {
            emptyList()
        }

    // Create chrome state with feature buttons
    val chromeState =
        remember(selectedTableId, tablesState) {
            ChromeState(
                additionalFeatureButtons =
                    listOf(
                        // Prominent button for creating new table
                        AdditionalFeatureButton(
                            key = "tables_new_table",
                            icon = {
                                Icon(
                                    imageVector = Icons.Default.Add,
                                    contentDescription = "New Table"
                                )
                            },
                            label = { Text("New Table") },
                            prominent = true,
                            onClick = {
                                vm.viewModelScope.launch {
                                    vm.createNewTable()
                                }
                            }
                        ),
                        // Prominent button for creating new tab (if table is selected)
                        if (selectedTableId != null) {
                            AdditionalFeatureButton(
                                key = "tables_new_tab",
                                icon = {
                                    Icon(
                                        imageVector = Icons.Default.Description,
                                        contentDescription = "New Tab"
                                    )
                                },
                                label = { Text("New Tab") },
                                prominent = true,
                                onClick = {
                                    vm.viewModelScope.launch {
                                        selectedTableId?.let { tableId ->
                                            vm.createNewTab(tableId)
                                        }
                                    }
                                }
                            )
                        } else {
                            null
                        },
                        // Non-prominent button for table settings
                        AdditionalFeatureButton(
                            key = "tables_settings",
                            icon = {
                                Icon(
                                    imageVector = Icons.Default.Settings,
                                    contentDescription = "Table Settings"
                                )
                            },
                            label = { Text("Table Settings") },
                            prominent = false,
                            onClick = {
                                // TODO: Open table settings
                            }
                        )
                    ).filterNotNull()
            )
        }

    ProvideChromeState(chromeState) {
        when (tablesState) {
            is TablesState.Error -> {
                Column(
                    modifier = modifier,
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Text("Error loading tables: ${tablesState.error.message()}")
                }
            }

            is TablesState.Loading -> {
                Column(
                    modifier = modifier,
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    CircularProgressIndicator()
                    Text("Loading tables...")
                }
            }

            is TablesState.Data -> {
                Column(
                    modifier = modifier.padding(16.dp)
                ) {
                    // Selected Table Info
                    if (selectedTable != null) {
                        Text(
                            text = "Selected Table: ${selectedTable.title}",
                            modifier = Modifier.padding(bottom = 16.dp)
                        )

                        Text(
                            text = "Tabs in this table: ${tabsForSelectedTable.size}",
                            modifier = Modifier.padding(bottom = 8.dp)
                        )

                        // Show tabs for selected table
                        tabsForSelectedTable.forEach { tab ->
                            Text(
                                text = "  • ${tab.title} (${tab.panels.size} panels)",
                                modifier = Modifier.padding(start = 16.dp, bottom = 4.dp)
                            )
                        }

                        Spacer(modifier = Modifier.height(24.dp))
                    }

                    // Overall State Summary
                    Text(
                        text = "Overall State:",
                        modifier = Modifier.padding(bottom = 8.dp)
                    )
                    Text("  • Windows: ${tablesState.windows.size}")
                    Text("  • Tables: ${tablesState.tables.size}")
                    Text("  • Tabs: ${tablesState.tabs.size}")
                    Text("  • Panels: ${tablesState.panels.size}")

                    Spacer(modifier = Modifier.height(24.dp))

                    // All Tables List
                    Text(
                        text = "All Tables:",
                        modifier = Modifier.padding(bottom = 8.dp)
                    )
                    tablesState.tablesList.forEach { table ->
                        val isSelected = table.id == selectedTableId
                        Text(
                            text = "  ${if (isSelected) "→" else "•"} ${table.title} (${table.tabs.size} tabs)",
                            modifier = Modifier.padding(start = 16.dp, bottom = 4.dp)
                        )
                    }
                }
            }
        }
    }
}

@Composable
fun Routes(
    modifier: Modifier = Modifier,
    contentType: DaybookContentType,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController
) {
    NavHost(
        startDestination = AppScreens.Home.name,
        navController = navController
    ) {
        composable(route = AppScreens.Capture.name) {
            // CaptureScreen provides its own chrome state internally
            CaptureScreen(modifier = modifier)
        }
        composable(route = AppScreens.Tables.name) {
            TablesScreen(modifier = modifier)
        }
        composable(route = AppScreens.Progress.name) {
            ProvideChromeState(ChromeState(title = "Progress")) {
                ProgressList(modifier = modifier)
            }
        }
        composable(route = AppScreens.Settings.name) {
            ProvideChromeState(ChromeState(title = "Settings")) {
                SettingsScreen(modifier = modifier)
            }
        }
        composable(route = AppScreens.Drawer.name) {
            ProvideChromeState(ChromeState(title = "Drawer")) {
                DrawerScreen(modifier = modifier, contentType = contentType)
            }
        }
        composable(route = AppScreens.Home.name) {
            ProvideChromeState(ChromeState.Empty) {
                var showContent by remember { mutableStateOf(false) }
                Column(
                    modifier = modifier,
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Button(onClick = {
                        showContent = !showContent
                        extraAction?.invoke()
                    }) {
                        Text("Click me!")
                    }
                    run {
                        val permCtx = LocalPermCtx.current
                        if (permCtx != null) {
                            if (permCtx.hasAll) {
                                Text("All permissions avail")
                            } else {
                                Button(onClick = {
                                    permCtx.requestAllPermissions()
                                }) {
                                    Text("Ask for permissions")
                                }
                            }
                        }
                    }
                    AnimatedVisibility(showContent) {
                        val greeting = remember { Greeting().greet() }
                        Column(
                            Modifier.fillMaxWidth(),
                            horizontalAlignment = Alignment.CenterHorizontally
                        ) {
                            Image(painterResource(Res.drawable.compose_multiplatform), null)
                            Text("Compose: $greeting")
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun LoadingScreen(message: String = "Preparing Daybook…") {
    val infiniteTransition = rememberInfiniteTransition(label = "loading_transition")
    val scale by infiniteTransition.animateFloat(
        initialValue = 1f,
        targetValue = 1.1f,
        animationSpec =
            infiniteRepeatable(
                animation = tween(600, easing = FastOutSlowInEasing),
                repeatMode = RepeatMode.Reverse
            ),
        label = "loading_scale"
    )

    Box(
        modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center
        ) {
            Text(
                text = "📖",
                fontSize = 80.sp,
                modifier = Modifier.scale(scale)
            )
            Spacer(Modifier.height(24.dp))
            Text(
                message,
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.8f)
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun WelcomeFlowScaffold(
    title: String,
    subtitle: String? = null,
    onBack: (() -> Unit)? = null,
    content: @Composable () -> Unit
) {
    Scaffold(
        topBar = {
            LargeTopAppBar(
                title = {
                    Column {
                        Text(title)
                        if (!subtitle.isNullOrBlank()) {
                            Text(
                                subtitle,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f)
                            )
                        }
                    }
                },
                navigationIcon = {
                    if (onBack != null) {
                        IconButton(onClick = onBack) {
                            Icon(Icons.Default.ArrowBack, contentDescription = "Back")
                        }
                    }
                }
            )
        }
    ) { innerPadding ->
        Box(
            modifier =
                Modifier
                    .fillMaxSize()
                    .background(MaterialTheme.colorScheme.surface)
                    .padding(innerPadding)
        ) {
            content()
        }
    }
}

@Composable
private fun WelcomeScreen(
    repos: List<KnownRepoEntry>,
    onOpenRepo: (String) -> Unit,
    onInspectRepo: (KnownRepoEntry) -> Unit,
    onStartClone: () -> Unit
) {
    val openRepoLauncher = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        onOpenRepo(selectedPath)
    }

    val createRepoLauncher = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        onOpenRepo(selectedPath)
    }

    val isDesktop = getPlatform().getScreenWidthDp().value >= 1000f
    WelcomeFlowScaffold(
        title = "Welcome to Daybook",
        subtitle = "Select a repository to continue"
    ) {
        if (isDesktop) {
            Row(
                modifier = Modifier.fillMaxSize().padding(24.dp),
                horizontalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                ElevatedCard(modifier = Modifier.width(360.dp).fillMaxHeight()) {
                    Column(
                        modifier = Modifier.fillMaxWidth().padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        Button(onClick = { createRepoLauncher.launch() }, modifier = Modifier.fillMaxWidth()) {
                            Icon(Icons.Default.CreateNewFolder, contentDescription = null)
                            Spacer(Modifier.width(8.dp))
                            Text("Create New Repo")
                        }
                        Button(onClick = { openRepoLauncher.launch() }, modifier = Modifier.fillMaxWidth()) {
                            Icon(Icons.Default.FolderOpen, contentDescription = null)
                            Spacer(Modifier.width(8.dp))
                            Text("Open Directory")
                        }
                        Button(onClick = onStartClone, modifier = Modifier.fillMaxWidth()) {
                            Icon(Icons.Default.Description, contentDescription = null)
                            Spacer(Modifier.width(8.dp))
                            Text("Clone Repo")
                        }
                    }
                }
                ElevatedCard(modifier = Modifier.weight(1f).fillMaxHeight()) {
                    Column(
                        modifier = Modifier.fillMaxSize().padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        if (repos.isEmpty()) {
                            Text(
                                text = "No known repositories yet.",
                                style = MaterialTheme.typography.bodyMedium,
                                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.6f)
                            )
                        } else {
                            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                                items(repos, key = { repo -> repo.id }) { repo ->
                                    Surface(
                                        modifier = Modifier.fillMaxWidth().clickable { onInspectRepo(repo) },
                                        shape = MaterialTheme.shapes.medium,
                                        tonalElevation = 2.dp
                                    ) {
                                        Column(modifier = Modifier.fillMaxWidth().padding(12.dp)) {
                                            Text(
                                                text = if (repo.name.isNotBlank()) repo.name else repo.path,
                                                style = MaterialTheme.typography.bodyLarge
                                            )
                                            Text(
                                                text = repo.path,
                                                style = MaterialTheme.typography.bodySmall,
                                                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f)
                                            )
                                            Text(
                                                text = "Last opened: ${repo.lastOpenedAtUnixSecs}",
                                                style = MaterialTheme.typography.bodySmall,
                                                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f)
                                            )
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            Column(
                modifier = Modifier.fillMaxSize().padding(24.dp),
                verticalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                Button(onClick = { createRepoLauncher.launch() }, modifier = Modifier.fillMaxWidth()) {
                    Icon(Icons.Default.CreateNewFolder, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("Create New Repo")
                }
                Button(onClick = { openRepoLauncher.launch() }, modifier = Modifier.fillMaxWidth()) {
                    Icon(Icons.Default.FolderOpen, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("Open Directory")
                }
                Button(onClick = onStartClone, modifier = Modifier.fillMaxWidth()) {
                    Icon(Icons.Default.Description, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("Clone Repo")
                }

                HorizontalDivider()

                if (repos.isEmpty()) {
                    Text(
                        text = "No known repositories yet.",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.6f)
                    )
                } else {
                    LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        items(repos, key = { repo -> repo.id }) { repo ->
                            Surface(
                                modifier = Modifier.fillMaxWidth().clickable { onInspectRepo(repo) },
                                shape = MaterialTheme.shapes.medium,
                                tonalElevation = 2.dp
                            ) {
                                Column(modifier = Modifier.fillMaxWidth().padding(12.dp)) {
                                    Text(
                                        text = if (repo.name.isNotBlank()) repo.name else repo.path,
                                        style = MaterialTheme.typography.bodyLarge
                                    )
                                    Text(
                                        text = repo.path,
                                        style = MaterialTheme.typography.bodySmall,
                                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f)
                                    )
                                    Text(
                                        text = "Last opened: ${repo.lastOpenedAtUnixSecs}",
                                        style = MaterialTheme.typography.bodySmall,
                                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f)
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun WelcomeRepoDetailScreen(
    repo: KnownRepoEntry,
    onBack: () -> Unit,
    onOpen: () -> Unit,
    onForget: () -> Unit,
    forgetting: Boolean
) {
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    WelcomeFlowScaffold(
        title = "Repository Details",
        subtitle = "Review before opening",
        onBack = onBack
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
                verticalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                ElevatedCard(modifier = Modifier.fillMaxWidth()) {
                    Column(
                        modifier = Modifier.fillMaxWidth().padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        Text(
                            text = if (repo.name.isBlank()) repo.path else repo.name,
                            style = MaterialTheme.typography.headlineSmall
                        )
                        Text(
                            text = repo.path,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f)
                        )
                        Text(
                            text = "Created: ${repo.createdAtUnixSecs}",
                            style = MaterialTheme.typography.bodySmall
                        )
                        Text(
                            text = "Last opened: ${repo.lastOpenedAtUnixSecs}",
                            style = MaterialTheme.typography.bodySmall
                        )
                    }
                }
                Column(
                    modifier = Modifier.fillMaxWidth(),
                    verticalArrangement = Arrangement.spacedBy(12.dp)
                ) {
                    Button(
                        onClick = onOpen,
                        modifier = Modifier.fillMaxWidth(),
                        enabled = !forgetting
                    ) {
                        Text("Open Repo")
                    }
                    OutlinedButton(
                        onClick = onForget,
                        modifier = Modifier.fillMaxWidth(),
                        enabled = !forgetting
                    ) {
                        Text(if (forgetting) "Forgetting..." else "Forget Repo")
                    }
                }
            }
        }
    }
}

@Composable
private fun CloneUrlScreen(
    state: CloneUiState.UrlInput,
    onBack: () -> Unit,
    onUrlChange: (String) -> Unit,
    onOpenScanner: () -> Unit,
    onContinue: (String) -> Unit
) {
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    WelcomeFlowScaffold(
        title = "Clone Repo",
        subtitle = "Enter a URL or scan a code",
        onBack = onBack
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                ElevatedCard {
                    Column(
                        modifier = Modifier.fillMaxWidth().padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        OutlinedTextField(
                            value = state.urlInput,
                            onValueChange = onUrlChange,
                            label = { Text("Clone URL") },
                            modifier = Modifier.fillMaxWidth(),
                            enabled = !state.isResolving
                        )
                        HorizontalDivider()
                        Button(
                            onClick = onOpenScanner,
                            enabled = !state.isResolving,
                            modifier = Modifier.fillMaxWidth()
                        ) {
                            Icon(Icons.Default.QrCodeScanner, contentDescription = null)
                            Spacer(Modifier.width(8.dp))
                            Text("Scan QR Code")
                        }
                        if (state.isResolving) {
                            Row(
                                horizontalArrangement = Arrangement.spacedBy(8.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp))
                                Text("Resolving clone URL…", style = MaterialTheme.typography.bodySmall)
                            }
                        }
                        if (!state.errorMessage.isNullOrBlank()) {
                            Text(
                                state.errorMessage,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.error
                            )
                        }
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Button(
                        onClick = { onContinue(state.urlInput.trim()) },
                        enabled = state.urlInput.trim().isNotBlank() && !state.isResolving
                    ) {
                        Text("Continue")
                    }
                }
            }
        }
    }
}

@Composable
private fun CloneQrScannerScreen(
    cameraPreviewFfi: CameraPreviewFfi,
    onBack: () -> Unit,
    onDetectedUrl: (String) -> Unit
) {
    val useNativePreviewQr = remember(cameraPreviewFfi) { cameraPreviewFfi.supportsNativeQrAnalysis() }
    val analyzer = remember { CameraQrAnalyzerFfi.load() }
    var userVisibleError by remember { mutableStateOf<String?>(null) }
    var hasCompleted by remember { mutableStateOf(false) }
    val frameBridge =
        remember(analyzer) {
            CameraQrOverlayBridge(
                analyzer = analyzer,
                onDetectedText = { rawText ->
                    if (hasCompleted) return@CameraQrOverlayBridge
                    val candidate = rawText.trim()
                    val looksLikeUrl = candidate.contains("://")
                    if (!looksLikeUrl) {
                        userVisibleError = "Detected QR is not a URL."
                        return@CameraQrOverlayBridge
                    }
                    hasCompleted = true
                    onDetectedUrl(candidate)
                }
            )
        }
    val previewBridge =
        remember(cameraPreviewFfi) {
            CameraPreviewQrBridge(
                cameraPreviewFfi = cameraPreviewFfi,
                onDetectedText = { rawText ->
                    if (hasCompleted) return@CameraPreviewQrBridge
                    val candidate = rawText.trim()
                    val looksLikeUrl = candidate.contains("://")
                    if (!looksLikeUrl) {
                        userVisibleError = "Detected QR is not a URL."
                        return@CameraPreviewQrBridge
                    }
                    hasCompleted = true
                    onDetectedUrl(candidate)
                }
            )
        }
    val overlayState by (if (useNativePreviewQr) previewBridge.state else frameBridge.state).collectAsState()

    androidx.compose.runtime.DisposableEffect(analyzer, frameBridge, previewBridge, useNativePreviewQr) {
        if (useNativePreviewQr) {
            previewBridge.start()
        } else {
            frameBridge.start()
        }
        onDispose {
            previewBridge.stop()
            frameBridge.stop()
            analyzer.close()
        }
    }

    WelcomeFlowScaffold(
        title = "Scan Clone URL",
        subtitle = "Point camera at a QR code",
        onBack = onBack
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxSize(),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                ElevatedCard(modifier = Modifier.fillMaxWidth().weight(1f)) {
                    DaybookCameraViewport(
                        cameraPreviewFfi = cameraPreviewFfi,
                        modifier = Modifier.fillMaxSize(),
                        overlays =
                            if (overlayState.overlays.isEmpty()) {
                                listOf(CameraOverlay.Grid)
                            } else {
                                overlayState.overlays
                            },
                        onFrameAvailable = if (useNativePreviewQr) null else frameBridge::submitFrame
                    )
                }
                val errorText = userVisibleError ?: overlayState.latestError
                if (!errorText.isNullOrBlank()) {
                    Text(
                        text = errorText,
                        color = MaterialTheme.colorScheme.error,
                        style = MaterialTheme.typography.bodySmall
                    )
                } else {
                    Text(
                        text = "Scanning… detected URLs auto-fill the clone form.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.72f)
                    )
                }
            }
        }
    }
}

@Composable
private fun CloneLocationScreen(
    state: CloneUiState.PickingLocation,
    onBack: () -> Unit,
    onDestinationChange: (String) -> Unit,
    onContinue: (String) -> Unit
) {
    val picker = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        val repoName = state.info.repoName.ifBlank { destinationPathBasename(state.destinationPath) }
        onDestinationChange("$selectedPath/$repoName")
    }

    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    WelcomeFlowScaffold(
        title = "Clone Destination",
        subtitle = "Resolve complete",
        onBack = onBack
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                ElevatedCard {
                    Column(
                        modifier = Modifier.fillMaxWidth().padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        Text("Repo: ${state.info.repoName}")
                        Text("Repo ID: ${state.info.repoId}")
                        Text("Endpoint: ${state.info.endpointId}")
                        OutlinedTextField(
                            value = state.destinationPath,
                            onValueChange = onDestinationChange,
                            label = { Text("Clone Destination") },
                            modifier = Modifier.fillMaxWidth(),
                            enabled = !state.isCloning
                        )
                        Button(
                            onClick = { picker.launch() },
                            enabled = !state.isCloning
                        ) {
                            Text("Browse")
                        }
                        if (state.isCloning) {
                            Row(
                                horizontalArrangement = Arrangement.spacedBy(8.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp))
                                Text("Initializing clone…", style = MaterialTheme.typography.bodySmall)
                            }
                        }
                        if (!state.errorMessage.isNullOrBlank()) {
                            Text(
                                state.errorMessage,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.error
                            )
                        }
                        if (!state.destinationWarning.isNullOrBlank()) {
                            Text(
                                state.destinationWarning,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.error
                            )
                        }
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Button(
                        onClick = { onContinue(state.destinationPath.trim()) },
                        enabled =
                            state.destinationPath.isNotBlank() &&
                                !state.isCloning &&
                                state.destinationWarning.isNullOrBlank()
                    ) {
                        Text("Continue")
                    }
                }
            }
        }
    }
}

@Composable
private fun CloneSyncScreen(
    onSyncInBackground: () -> Unit,
    onRetry: () -> Unit
) {
    val container = LocalContainer.current
    var statusMessage by remember { mutableStateOf("Sync in progress…") }
    var syncTasks by remember { mutableStateOf(emptyList<ProgressTask>()) }

    LaunchedEffect(Unit) {
        while (true) {
            try {
                syncTasks = container.progressRepo.listByTagPrefix("/sync/full")
                val active = syncTasks.count { it.state == ProgressTaskState.ACTIVE }
                statusMessage =
                    if (syncTasks.isEmpty()) {
                        "Connected. Waiting for sync tasks…"
                    } else {
                        "Sync tasks running: $active active / ${syncTasks.size} total"
                    }
            } catch (_: Throwable) {
                statusMessage = "Unable to read sync progress right now."
            }
            kotlinx.coroutines.delay(1000)
        }
    }

    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.7f else 1f
    WelcomeFlowScaffold(
        title = "Sync",
        subtitle = "Clone ongoing"
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Text(statusMessage, style = MaterialTheme.typography.bodyMedium)
                LazyColumn(
                    modifier = Modifier.fillMaxWidth().weight(1f),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    items(syncTasks, key = { it.id }) { task ->
                        ElevatedCard(modifier = Modifier.fillMaxWidth()) {
                            Column(
                                modifier = Modifier.fillMaxWidth().padding(12.dp),
                                verticalArrangement = Arrangement.spacedBy(6.dp)
                            ) {
                                Text(
                                    text = task.title ?: task.id,
                                    style = MaterialTheme.typography.titleSmall
                                )
                                Text(
                                    text = task.latestUpdate?.let { "Update #${it.sequence}" } ?: "No updates yet",
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f)
                                )
                                when (val deets = task.latestUpdate?.update?.deets) {
                                    is ProgressUpdateDeets.Amount ->
                                        ProgressAmountBlock(deets, modifier = Modifier.fillMaxWidth())
                                    is ProgressUpdateDeets.Status ->
                                        Text(deets.message, style = MaterialTheme.typography.bodySmall)
                                    is ProgressUpdateDeets.Completed ->
                                        Text(
                                            deets.message ?: deets.state.name.lowercase(),
                                            style = MaterialTheme.typography.bodySmall
                                        )
                                    null -> {}
                                }
                            }
                        }
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Button(onClick = onSyncInBackground) {
                        Text("Sync in background")
                    }
                    TextButton(onClick = onRetry) {
                        Text("Retry connection")
                    }
                }
            }
        }
    }
}

private fun destinationPathBasename(path: String): String {
    val trimmed = path.trim().trimEnd('/', '\\')
    if (trimmed.isBlank()) return "daybook-repo"
    val unix = trimmed.substringAfterLast('/')
    return unix.substringAfterLast('\\').ifBlank { "daybook-repo" }
}

private fun describeThrowable(error: Throwable): String {
    val parts = mutableListOf<String>()
    var current: Throwable? = error
    var depth = 0
    while (current != null && depth < 4) {
        val className = current::class.simpleName ?: current::class.qualifiedName ?: "Throwable"
        val message = current.message?.takeIf { it.isNotBlank() }
        val piece =
            when {
                message != null -> "$className: $message"
                else -> current.toString()
            }
        if (piece.isNotBlank()) {
            parts += piece
        }
        current = current.cause
        depth += 1
    }
    return parts.distinct().joinToString(" | ").ifBlank { error.toString() }
}

@Composable
private fun ErrorScreen(title: String, message: String, onRetry: () -> Unit) {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text(title)
            Spacer(Modifier.height(8.dp))
            Text(message)
            Spacer(Modifier.height(16.dp))
            Button(onClick = onRetry) { Text("Retry") }
        }
    }
}
