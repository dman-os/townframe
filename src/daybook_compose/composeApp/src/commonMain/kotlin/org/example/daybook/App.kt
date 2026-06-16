@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

// FIXME: remove usage of Result

package org.example.daybook

import androidx.compose.animation.core.FastOutSlowInEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
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
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.rememberSaveableStateHolderNavEntryDecorator
import androidx.navigation3.ui.NavDisplay
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.drawer.DocEditorScreen
import org.example.daybook.drawer.DrawerScreen
import org.example.daybook.home.HomeIcon
import org.example.daybook.home.HomeMenuWidgetConfig
import org.example.daybook.home.HomeScreen
import org.example.daybook.home.HomeScreenConfig
import org.example.daybook.home.MenuNavItem
import org.example.daybook.home.WipPermissionsWidgetConfig
import org.example.daybook.layouts.CompactLayout
import org.example.daybook.layouts.DaybookScaffold
import org.example.daybook.layouts.ExpandedLayout
import org.example.daybook.layouts.ProvideScreenChromeSpec
import org.example.daybook.navigation.DaybookNavKey
import org.example.daybook.navigation.DaybookNavigationState
import org.example.daybook.progress.ProgressList
import org.example.daybook.settings.SettingsScreen
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.DispatchRepoFfi
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.InitRepoFfi
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.RtFfi
import org.example.daybook.uniffi.SqliteLocalStateRepoFfi
import org.example.daybook.uniffi.SyncRepoFfi
import org.example.daybook.uniffi.TablesEventListener
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.KnownRepoEntry
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.Panel
import org.example.daybook.uniffi.core.Tab
import org.example.daybook.uniffi.core.Table
import org.example.daybook.uniffi.core.TablesEvent
import org.example.daybook.uniffi.core.Uuid
import org.example.daybook.uniffi.core.Window
import kotlin.time.TimeSource

enum class DaybookNavigationType {
    BOTTOM_NAVIGATION,
    NAVIGATION_RAIL,
    PERMANENT_NAVIGATION_DRAWER,
}

enum class DaybookContentType {
    LIST_ONLY,
    LIST_AND_DETAIL,
}

val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }
val LocalAppExitRequest = compositionLocalOf<(() -> Unit)?> { null }

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val hasStorageRead: Boolean = false,
    val hasStorageWrite: Boolean = false,
    val requestPermissions: (PermissionRequest) -> Unit = {},
) {
    val hasAll =
        hasCamera and hasNotifications and hasMicrophone and hasOverlay and hasStorageRead and
            hasStorageWrite
}

data class PermissionRequest(
    val camera: Boolean = false,
    val notifications: Boolean = false,
    val microphone: Boolean = false,
    val overlay: Boolean = false,
    val storageRead: Boolean = false,
    val storageWrite: Boolean = false,
)

data class AppContainer(
    val ffiCtx: FfiCtx,
    val drawerRepo: DrawerRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val dispatchRepo: DispatchRepoFfi,
    val progressRepo: ProgressRepoFfi,
    val initRepo: InitRepoFfi,
    val sqliteLsRepo: SqliteLocalStateRepoFfi,
    val rtFfi: RtFfi?,
    val plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi,
    val configRepo: ConfigRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val syncRepo: SyncRepoFfi?,
    val cameraPreviewFfi: CameraPreviewFfi,
)

val LocalContainer =
    staticCompositionLocalOf<AppContainer> {
        error("no AppContainer provided")
    }

val LocalDrawerViewModel =
    staticCompositionLocalOf<DrawerViewModel> {
        error("no DrawerViewModel provided")
    }

val LocalDocEditorStore =
    staticCompositionLocalOf<DocEditorStoreViewModel> {
        error("no DocEditorStoreViewModel provided")
    }

data class AppConfig(val theme: ThemeConfig = ThemeConfig.Dark)

sealed interface TablesState {
    data class Data(
        val windows: Map<Uuid, Window>,
        val tabs: Map<Uuid, Tab>,
        val panels: Map<Uuid, Panel>,
        val tables: Map<Uuid, Table>,
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
    val tables: Set<Uuid> = emptySet(),
) {
    fun merge(other: TablesRefreshIntent): TablesRefreshIntent = TablesRefreshIntent(
        refreshAll = refreshAll || other.refreshAll,
        windows = windows + other.windows,
        tabs = tabs + other.tabs,
        panels = panels + other.panels,
        tables = tables + other.tables,
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
            onIntent = { intent: TablesRefreshIntent -> applyRefreshIntent(intent) },
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
                    tables = tables.associateBy { it.id },
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

private fun defaultHomeScreenConfig(navState: DaybookNavigationState, onShowCloneShare: () -> Unit): HomeScreenConfig =
    HomeScreenConfig(
        widgets =
        listOf(
            WipPermissionsWidgetConfig(),
            HomeMenuWidgetConfig(
                items =
                listOf(
                    MenuNavItem(
                        id = "settings",
                        label = "settings",
                        icon = HomeIcon.Settings,
                        onClick = {
                            navState.navigate(DaybookNavKey.Settings)
                        },
                    ),
                    MenuNavItem(
                        id = "clone",
                        label = "clone",
                        icon = HomeIcon.Clone,
                        onClick = onShowCloneShare,
                    ),
                    MenuNavItem(
                        id = "new_doc",
                        label = "new doc",
                        icon = HomeIcon.NewDoc,
                        onClick = {
                            navState.navigate(DaybookNavKey.Capture)
                        },
                    ),
                    MenuNavItem(
                        id = "camera",
                        label = "camera",
                        icon = HomeIcon.Camera,
                        onClick = {
                            navState.navigate(DaybookNavKey.Capture)
                        },
                    ),
                    MenuNavItem(
                        id = "mic",
                        label = "mic",
                        icon = HomeIcon.Mic,
                        onClick = {
                            navState.navigate(DaybookNavKey.Capture)
                        },
                    ),
                    MenuNavItem(
                        id = "drawer",
                        label = "drawer",
                        icon = HomeIcon.Drawer,
                        onClick = {
                            navState.navigate(DaybookNavKey.Drawer)
                        },
                    ),
                ),
            ),
        ),
    )

@Composable
@Preview
fun App(
    config: AppConfig = AppConfig(),
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    shutdownRequested: Boolean = false,
    onShutdownCompleted: (() -> Unit)? = null,
    autoShutdownOnDispose: Boolean = true,
    onExitRequest: (() -> Unit)? = null,
) {
    val permCtx = LocalPermCtx.current
    val appStartMark = remember { TimeSource.Monotonic.markNow() }
    var cloneUiState by remember { mutableStateOf<CloneUiState?>(null) }
    var createRepoUiState by remember { mutableStateOf<CreateRepoUiState?>(null) }
    var cloneSourceUrlPendingOpen by remember { mutableStateOf<String?>(null) }
    var cloneInitRequest by remember { mutableStateOf<Pair<String, String>?>(null) }
    var createRepoInitRequest by remember { mutableStateOf<String?>(null) }
    var selectedWelcomeRepo by remember { mutableStateOf<KnownRepoEntry?>(null) }
    var cloneCameraPreviewFfi by remember { mutableStateOf<CameraPreviewFfi?>(null) }
    val ffiServices = rememberAppFfiServices()
    val runtimeVm: AppRuntimeViewModel =
        viewModel(key = "appRuntimeVm") { AppRuntimeViewModel(ffiServices) }
    val runtimeState by runtimeVm.state.collectAsState()

    LaunchedEffect(runtimeVm) {
        runtimeVm.start { appStartMark.elapsedNow().inWholeMilliseconds }
    }

    LaunchedEffect(runtimeState, cloneCameraPreviewFfi) {
        // Defer optional camera preload until after initial app bootstrap to avoid FFI init races.
        val canPreloadCamera =
            runtimeState is FfiRuntimeState.Welcome || runtimeState is FfiRuntimeState.Ready
        if (!canPreloadCamera || cloneCameraPreviewFfi != null) return@LaunchedEffect
        runCatching {
            withContext(Dispatchers.IO) { CameraPreviewFfi.load() }
        }.onSuccess { loaded ->
            cloneCameraPreviewFfi = loaded
        }.onFailure { error ->
            println("[APP_INIT] CameraPreview preload failed: ${error.message}")
        }
    }

    androidx.compose.runtime.DisposableEffect(cloneCameraPreviewFfi) {
        onDispose {
            cloneCameraPreviewFfi?.close()
        }
    }
    val currentRuntimeState = runtimeState
    LaunchedEffect(currentRuntimeState, cloneSourceUrlPendingOpen) {
        val ready = currentRuntimeState as? FfiRuntimeState.Ready ?: return@LaunchedEffect
        val sourceUrl = cloneSourceUrlPendingOpen ?: return@LaunchedEffect
        val syncRepo = ready.container.syncRepo
        if (syncRepo == null) {
            cloneUiState =
                CloneUiState.Syncing(
                    sourceUrl = sourceUrl,
                    initialSyncComplete = false,
                    phaseMessage = "Starting sync services…",
                    errorMessage = null,
                )
            return@LaunchedEffect
        }
        cloneUiState =
            CloneUiState.Syncing(
                sourceUrl = sourceUrl,
                initialSyncComplete = false,
                phaseMessage = "Pulling required docs…",
                errorMessage = null,
            )
        try {
            withContext(Dispatchers.IO) {
                syncRepo.connectUrl(sourceUrl)
            }
            val current = cloneUiState as? CloneUiState.Syncing
            if (current != null && current.sourceUrl == sourceUrl) {
                cloneUiState =
                    current.copy(
                        initialSyncComplete = true,
                        phaseMessage = "Required docs synced. Remaining sync is running.",
                        errorMessage = null,
                    )
            }
        } catch (error: Throwable) {
            if (error is CancellationException) {
                return@LaunchedEffect
            }
            val current = cloneUiState as? CloneUiState.Syncing
            if (current != null && current.sourceUrl == sourceUrl) {
                cloneUiState =
                    current.copy(
                        initialSyncComplete = false,
                        phaseMessage = "Failed while pulling required docs.",
                        errorMessage = "Connect failed: ${describeThrowable(error)}",
                    )
            }
        } finally {
            if (cloneSourceUrlPendingOpen == sourceUrl) {
                cloneSourceUrlPendingOpen = null
            }
        }
    }

    DaybookTheme(themeConfig = config.theme) {
        when (val state = currentRuntimeState) {
            is FfiRuntimeState.Loading -> {
                LoadingScreen()
            }

            is FfiRuntimeState.Welcome -> {
                WelcomeFlowNavHost(
                    repos = state.repos,
                    permCtx = permCtx,
                    cameraPreviewFfi = cloneCameraPreviewFfi,
                    selectedWelcomeRepo = selectedWelcomeRepo,
                    cloneUiState = cloneUiState,
                    createRepoUiState = createRepoUiState,
                    cloneSourceUrlPendingOpen = cloneSourceUrlPendingOpen,
                    cloneInitRequest = cloneInitRequest,
                    createRepoInitRequest = createRepoInitRequest,
                    onSelectedWelcomeRepoChange = { selectedWelcomeRepo = it },
                    onCloneUiStateChange = { cloneUiState = it },
                    onCreateRepoUiStateChange = { createRepoUiState = it },
                    onCloneSourceUrlPendingOpenChange = { cloneSourceUrlPendingOpen = it },
                    onCloneInitRequestChange = { cloneInitRequest = it },
                    onCreateRepoInitRequestChange = { createRepoInitRequest = it },
                    onPendingOpenRepoPath = {
                        runtimeVm.openRepo(it) { appStartMark.elapsedNow().inWholeMilliseconds }
                    },
                    onForgetRepo = { runtimeVm.forgetRepo(it) },
                    onExitRequest = { onExitRequest?.invoke() },
                )
            }

            is FfiRuntimeState.OpeningRepo -> {
                val syncingState = cloneUiState as? CloneUiState.Syncing
                if (syncingState != null) {
                    CloneSyncScreen(
                        progressRepo = null,
                        state = syncingState,
                        onSyncInBackground = {},
                        onRetry = {
                            cloneSourceUrlPendingOpen = syncingState.sourceUrl
                            runtimeVm.openRepo(state.repoPath) {
                                appStartMark.elapsedNow().inWholeMilliseconds
                            }
                        },
                    )
                } else {
                    LoadingScreen(message = "Opening repo: ${state.repoPath}")
                }
            }

            is FfiRuntimeState.Error -> {
                ErrorScreen(
                    title = "Failed to initialize",
                    message = state.throwable.message ?: "Unknown error",
                    onRetry = {
                        selectedWelcomeRepo = null
                        runtimeVm.start { appStartMark.elapsedNow().inWholeMilliseconds }
                    },
                )
            }

            is FfiRuntimeState.Ready -> {
                val appContainer = state.container
                val containerKey = "container:${appContainer.ffiCtx}"
                val drawerVm: DrawerViewModel =
                    viewModel(key = "drawerVm:$containerKey") {
                        DrawerViewModel(appContainer.drawerRepo)
                    }
                val docEditorStore: DocEditorStoreViewModel =
                    viewModel(key = "docEditorStoreVm:$containerKey") {
                        DocEditorStoreViewModel(appContainer.drawerRepo)
                    }
                var shutdownDone by remember(appContainer.ffiCtx) { mutableStateOf(false) }

                LaunchedEffect(shutdownRequested, appContainer.ffiCtx, shutdownDone) {
                    if (shutdownRequested && !shutdownDone) {
                        runtimeVm.shutdownReadyContainer()
                        shutdownDone = true
                        onShutdownCompleted?.invoke()
                    }
                }

                androidx.compose.runtime.DisposableEffect(appContainer.ffiCtx) {
                    onDispose {
                        if (!autoShutdownOnDispose) return@onDispose
                        if (!shutdownDone) {
                            runBlocking(Dispatchers.IO) {
                                runtimeVm.shutdownReadyContainer()
                            }
                        }
                    }
                }

                Box(modifier = Modifier.fillMaxSize()) {
                    val syncingState = cloneUiState as? CloneUiState.Syncing
                    if (syncingState != null) {
                        CloneSyncScreen(
                            progressRepo = appContainer.progressRepo,
                            state = syncingState,
                            onSyncInBackground = {
                                if (syncingState.initialSyncComplete) {
                                    cloneUiState = null
                                }
                            },
                            onRetry = {
                                cloneSourceUrlPendingOpen = syncingState.sourceUrl
                            },
                        )
                    } else {
                        DaybookAppReadyContent(
                            args =
                            DaybookAppReadyContentArgs(
                                appContainer = appContainer,
                                drawerVm = drawerVm,
                                docEditorStore = docEditorStore,
                                extraAction = extraAction,
                                onExitRequest = onExitRequest,
                            ),
                            surfaceModifier = surfaceModifier,
                        )
                    }
                    if (shutdownRequested && !shutdownDone) {
                        Surface(
                            modifier = Modifier.fillMaxSize(),
                            color = MaterialTheme.colorScheme.surface,
                        ) {
                            Box(
                                modifier = Modifier.fillMaxSize(),
                                contentAlignment = Alignment.Center,
                            ) {
                                Column(
                                    horizontalAlignment = Alignment.CenterHorizontally,
                                    verticalArrangement = Arrangement.spacedBy(12.dp),
                                ) {
                                    CircularProgressIndicator()
                                    Text(
                                        "Shutting down…",
                                        style = MaterialTheme.typography.bodyLarge,
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
@Suppress("UnusedParameter", "FunctionNaming")
fun AdaptiveAppLayout(
    modifier: Modifier = Modifier,
    navState: DaybookNavigationState,
    extraAction: (() -> Unit)? = null,
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

    Box(modifier = modifier.fillMaxSize()) {
        when (navigationType) {
            DaybookNavigationType.PERMANENT_NAVIGATION_DRAWER -> {
                ExpandedLayout(
                    modifier = Modifier.fillMaxSize(),
                    navState = navState,
                    contentType = contentType,
                    onShowCloneShare = { navState.navigate(DaybookNavKey.CloneShare) },
                )
            }

            DaybookNavigationType.NAVIGATION_RAIL -> {
                ExpandedLayout(
                    modifier = Modifier.fillMaxSize(),
                    navState = navState,
                    contentType = contentType,
                    onShowCloneShare = { navState.navigate(DaybookNavKey.CloneShare) },
                )
            }

            DaybookNavigationType.BOTTOM_NAVIGATION -> {
                CompactLayout(
                    modifier = Modifier.fillMaxSize(),
                    navState = navState,
                    contentType = contentType,
                    onShowCloneShare = { navState.navigate(DaybookNavKey.CloneShare) },
                )
            }
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
                                contentDescription = "New Table",
                            )
                        },
                        label = { Text("New Table") },
                        prominent = true,
                        onClick = {
                            vm.viewModelScope.launch {
                                vm.createNewTable()
                            }
                        },
                    ),
                    // Prominent button for creating new tab (if table is selected)
                    if (selectedTableId != null) {
                        AdditionalFeatureButton(
                            key = "tables_new_tab",
                            icon = {
                                Icon(
                                    imageVector = Icons.Default.Description,
                                    contentDescription = "New Tab",
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
                            },
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
                                contentDescription = "Table Settings",
                            )
                        },
                        label = { Text("Table Settings") },
                        prominent = false,
                        onClick = {
                            // TODO: Open table settings
                        },
                    ),
                ).filterNotNull(),
            )
        }

    DaybookScaffold(
        modifier = modifier,
    ) { scaffoldPadding ->
        ProvideChromeState(chromeState) {
            when (tablesState) {
                is TablesState.Error -> {
                    Column(
                        modifier = Modifier.padding(scaffoldPadding),
                        horizontalAlignment = Alignment.CenterHorizontally,
                    ) {
                        Text("Error loading tables: ${tablesState.error.message()}")
                    }
                }

                is TablesState.Loading -> {
                    Column(
                        modifier = Modifier.padding(scaffoldPadding),
                        horizontalAlignment = Alignment.CenterHorizontally,
                    ) {
                        CircularProgressIndicator()
                        Text("Loading tables...")
                    }
                }

                is TablesState.Data -> {
                    Column(
                        modifier = Modifier.padding(scaffoldPadding).padding(16.dp),
                    ) {
                        // Selected Table Info
                        if (selectedTable != null) {
                            Text(
                                text = "Selected Table: ${selectedTable.title}",
                                modifier = Modifier.padding(bottom = 16.dp),
                            )

                            Text(
                                text = "Tabs in this table: ${tabsForSelectedTable.size}",
                                modifier = Modifier.padding(bottom = 8.dp),
                            )

                            // Show tabs for selected table
                            tabsForSelectedTable.forEach { tab ->
                                Text(
                                    text = "  • ${tab.title} (${tab.panels.size} panels)",
                                    modifier = Modifier.padding(start = 16.dp, bottom = 4.dp),
                                )
                            }

                            Spacer(modifier = Modifier.height(24.dp))
                        }

                        // Overall State Summary
                        Text(
                            text = "Overall State:",
                            modifier = Modifier.padding(bottom = 8.dp),
                        )
                        Text("  • Windows: ${tablesState.windows.size}")
                        Text("  • Tables: ${tablesState.tables.size}")
                        Text("  • Tabs: ${tablesState.tabs.size}")
                        Text("  • Panels: ${tablesState.panels.size}")

                        Spacer(modifier = Modifier.height(24.dp))

                        // All Tables List
                        Text(
                            text = "All Tables:",
                            modifier = Modifier.padding(bottom = 8.dp),
                        )
                        tablesState.tablesList.forEach { table ->
                            val isSelected = table.id == selectedTableId
                            Text(
                                text = "  ${if (isSelected) "→" else "•"} ${table.title} (${table.tabs.size} tabs)",
                                modifier = Modifier.padding(start = 16.dp, bottom = 4.dp),
                            )
                        }
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
    onShowCloneShare: () -> Unit = {},
    navState: DaybookNavigationState,
) {
    val drawerVm = LocalDrawerViewModel.current
    val exitRequest = LocalAppExitRequest.current
    val screenWidth = getPlatform().getScreenWidthDp()
    val narrowScreen = screenWidth.value < 600f
    val bigDialogStrategy =
        remember(screenWidth) {
            BigDialogSceneStrategy<NavKey>(narrowScreen = narrowScreen)
        }
    val onBack: () -> Unit = {
        if (navState.backStack.size > 1) {
            navState.pop()
            Unit
        } else {
            exitRequest?.invoke()
            Unit
        }
    }
    BigDialogHost(
        narrowScreen = narrowScreen,
        modifier = modifier,
    ) {
        NavDisplay(
            backStack = navState.backStack,
            onBack = onBack,
            sceneStrategies = listOf(bigDialogStrategy),
            transitionSpec = {
                slideInHorizontally(
                    animationSpec = tween(240),
                    initialOffsetX = { fullWidth -> fullWidth },
                ) togetherWith slideOutHorizontally(
                    animationSpec = tween(240),
                    targetOffsetX = { fullWidth -> -fullWidth },
                )
            },
            popTransitionSpec = {
                slideInHorizontally(
                    animationSpec = tween(240),
                    initialOffsetX = { fullWidth -> -fullWidth },
                ) togetherWith slideOutHorizontally(
                    animationSpec = tween(240),
                    targetOffsetX = { fullWidth -> fullWidth },
                )
            },
            predictivePopTransitionSpec = {
                slideInHorizontally(
                    animationSpec = tween(240),
                    initialOffsetX = { fullWidth -> -fullWidth },
                ) togetherWith slideOutHorizontally(
                    animationSpec = tween(240),
                    targetOffsetX = { fullWidth -> fullWidth },
                )
            },
            entryDecorators = listOf(
                rememberSaveableStateHolderNavEntryDecorator(),
                rememberViewModelStoreNavEntryDecorator(),
            ),
            modifier = Modifier.fillMaxSize(),
            entryProvider = entryProvider {
                entry<DaybookNavKey.Home> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Home,
                            onBack = onBack,
                        ),
                    ) {
                        HomeScreen(
                            config = defaultHomeScreenConfig(
                                navState = navState,
                                onShowCloneShare = onShowCloneShare,
                            ),
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.CloneShare>(
                    metadata = bigDialog(),
                ) {
                    CloneShareDialogContent(onClose = { navState.pop() })
                }
                entry<DaybookNavKey.Capture> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Capture,
                            onBack = onBack,
                        ),
                    ) {
                        CaptureScreen(
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.Tables> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Tables,
                            onBack = onBack,
                        ),
                    ) {
                        TablesScreen(
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.Progress> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Progress,
                            onBack = onBack,
                        ),
                    ) {
                        ProgressList(
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.Settings> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Settings,
                            onBack = onBack,
                        ),
                    ) {
                        SettingsScreen(
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.Drawer> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.Drawer,
                            onBack = onBack,
                        ),
                    ) {
                        DrawerScreen(
                            drawerVm = drawerVm,
                            onOpenDoc = {
                                navState.navigate(DaybookNavKey.DocEditor)
                            },
                            modifier = modifier,
                        )
                    }
                }
                entry<DaybookNavKey.DocEditor> {
                    ProvideScreenChromeSpec(
                        navState.chromeSpecFor(
                            destination = DaybookNavKey.DocEditor,
                            onBack = onBack,
                        ),
                    ) {
                        DocEditorScreen(
                            contentType = contentType,
                            modifier = modifier,
                        )
                    }
                }
            },
        )
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
            repeatMode = RepeatMode.Reverse,
        ),
        label = "loading_scale",
    )

    Box(
        modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface),
        contentAlignment = Alignment.Center,
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center,
        ) {
            Text(
                text = "🌞",
                fontSize = 80.sp,
                modifier = Modifier.scale(scale),
            )
            Spacer(Modifier.height(24.dp))
            Text(
                message,
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.8f),
            )
        }
    }
}

@Composable
private fun ErrorScreen(title: String, message: String, onRetry: () -> Unit) {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center,
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Text(title)
            Spacer(Modifier.height(8.dp))
            Text(message)
            Spacer(Modifier.height(16.dp))
            Button(onClick = onRetry) { Text("Retry") }
        }
    }
}
