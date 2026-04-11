@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

// FIXME: remove usage of Result

package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.EnterTransition
import androidx.compose.animation.ExitTransition
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
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
import androidx.compose.runtime.rememberCoroutineScope
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
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import daybook.composeapp.generated.resources.Res
import daybook.composeapp.generated.resources.compose_multiplatform
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.time.Clock
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import io.github.vinceglb.filekit.dialogs.compose.rememberDirectoryPickerLauncher
import io.github.vinceglb.filekit.path
import org.example.daybook.capture.CameraCaptureContext
import org.example.daybook.capture.ProvideCameraCaptureContext
import org.example.daybook.capture.data.CameraOverlay
import org.example.daybook.capture.data.CameraPreviewQrBridge
import org.example.daybook.capture.data.CameraQrOverlayBridge
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.capture.ui.DaybookCameraViewport
import org.example.daybook.drawer.DocEditorScreen
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
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.RtFfi
import org.example.daybook.uniffi.SyncRepoFfi
import org.example.daybook.uniffi.CloneBootstrapInfo
import org.example.daybook.uniffi.TablesEventListener
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.KnownRepoEntry
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.Panel
import org.example.daybook.uniffi.core.CreateProgressTaskArgs
import org.example.daybook.uniffi.core.ProgressFinalState
import org.example.daybook.uniffi.core.ProgressRetentionPolicy
import org.example.daybook.uniffi.core.ProgressSeverity
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUnit
import org.example.daybook.uniffi.core.ProgressUpdate
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
    val hasStorageRead: Boolean = false,
    val hasStorageWrite: Boolean = false,
    val requestPermissions: (PermissionRequest) -> Unit = {}
) {
    val hasAll =
        hasCamera and hasNotifications and hasMicrophone and hasOverlay and hasStorageRead and hasStorageWrite
}

data class PermissionRequest(
    val camera: Boolean = false,
    val notifications: Boolean = false,
    val microphone: Boolean = false,
    val overlay: Boolean = false,
    val storageRead: Boolean = false,
    val storageWrite: Boolean = false
)

data class AppContainer(
    val ffiCtx: FfiCtx,
    val drawerRepo: DrawerRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val dispatchRepo: DispatchRepoFfi,
    val progressRepo: ProgressRepoFfi,
    val rtFfi: RtFfi?,
    val plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi,
    val configRepo: ConfigRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val syncRepo: SyncRepoFfi?,
    val cameraPreviewFfi: CameraPreviewFfi
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

enum class AppScreens {
    Home,
    Capture,
    Tables,
    Progress,
    Settings,
    Drawer,
    DocEditor
}

private sealed interface AppInitState {
    data object Loading : AppInitState

    data class Welcome(val repos: List<KnownRepoEntry>) : AppInitState

    data class OpeningRepo(val repoPath: String) : AppInitState

    data class Ready(val container: AppContainer) : AppInitState

    data class Error(val throwable: Throwable) : AppInitState
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

private suspend fun warmUpTablesRepo(tablesRepo: TablesRepoFfi) {
    tablesRepo.listWindows()
    tablesRepo.listTabs()
    tablesRepo.listPanels()
    tablesRepo.listTables()
}

private const val STARTUP_PROGRESS_TASK_ID = "app/init/startup"

private class StartupProgressTask(
    private val progressRepo: ProgressRepoFfi,
    val taskId: String,
    private val appElapsedMillis: () -> Long,
    private val totalStages: Int
) {
    private var doneStages = 0

    suspend fun begin(repoPath: String, startupElapsed: String, phaseId: String) {
        progressRepo.upsertTask(
            CreateProgressTaskArgs(
                id = taskId,
                tags = listOf("/app/init", "/app/init/open-repo"),
                retention = ProgressRetentionPolicy.UserDismissable
            )
        )
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Status(
                        severity = ProgressSeverity.INFO,
                        message =
                            "startup phase begin phase=$phaseId (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Status(
                        severity = ProgressSeverity.INFO,
                        message =
                            "opening repo $repoPath (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
    }

    suspend fun stageStart(stage: String, startupElapsed: String) {
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Status(
                        severity = ProgressSeverity.INFO,
                        message =
                            "starting $stage (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
    }

    suspend fun stageDone(stage: String, stageElapsed: String, startupElapsed: String) {
        doneStages += 1
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Amount(
                        severity = ProgressSeverity.INFO,
                        done = doneStages.toULong(),
                        total = totalStages.toULong(),
                        unit = ProgressUnit.Generic("stage"),
                        message =
                            "$stage done in $stageElapsed (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
    }

    suspend fun fail(message: String, startupElapsed: String) {
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Completed(
                        state = ProgressFinalState.FAILED,
                        message =
                            "$message (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
    }

    suspend fun complete(startupElapsed: String) {
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                    ProgressUpdateDeets.Completed(
                        state = ProgressFinalState.SUCCEEDED,
                        message =
                            "startup complete (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})"
                    )
            )
        )
    }
}

private suspend fun <T> withStartupStage(
    stage: String,
    startupMark: TimeMark,
    progress: StartupProgressTask?,
    block: suspend () -> T
): T {
    val stageMark = TimeSource.Monotonic.markNow()
    progress?.stageStart(stage, startupMark.elapsedNow().toString())
    try {
        val out = block()
        progress?.stageDone(
            stage,
            stageMark.elapsedNow().toString(),
            startupMark.elapsedNow().toString()
        )
        return out
    } catch (t: Throwable) {
        progress?.fail(
            "stage $stage failed: ${t.message ?: "unknown error"}",
            startupMark.elapsedNow().toString()
        )
        throw t
    }
}

@Composable
@Preview
fun App(
    config: AppConfig = AppConfig(),
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController = rememberNavController(),
    shutdownRequested: Boolean = false,
    onShutdownCompleted: (() -> Unit)? = null,
    autoShutdownOnDispose: Boolean = true
) {
    val permCtx = LocalPermCtx.current
    val appStartMark = remember { TimeSource.Monotonic.markNow() }
    var initAttempt by remember { mutableStateOf(0) }
    var initState by remember { mutableStateOf<AppInitState>(AppInitState.Loading) }
    var pendingOpenRepoPath by remember { mutableStateOf<String?>(null) }
    var cloneUiState by remember { mutableStateOf<CloneUiState?>(null) }
    var createRepoUiState by remember { mutableStateOf<CreateRepoUiState?>(null) }
    var cloneSourceUrlPendingOpen by remember { mutableStateOf<String?>(null) }
    var cloneInitRequest by remember { mutableStateOf<Pair<String, String>?>(null) }
    var createRepoInitRequest by remember { mutableStateOf<String?>(null) }
    var selectedWelcomeRepo by remember { mutableStateOf<KnownRepoEntry?>(null) }
    var pendingForgetRepoId by remember { mutableStateOf<String?>(null) }
    var cloneCameraPreviewFfi by remember { mutableStateOf<CameraPreviewFfi?>(null) }
    val ffiServices = rememberAppFfiServices()

    LaunchedEffect(initState, cloneCameraPreviewFfi) {
        // Defer optional camera preload until after initial app bootstrap to avoid FFI init races.
        val canPreloadCamera =
            initState is AppInitState.Welcome || initState is AppInitState.Ready
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

    LaunchedEffect(initAttempt) {
        initState = AppInitState.Loading
        selectedWelcomeRepo = null
        try {
            println("[APP_INIT] stage=getRepoConfig start")
            val repoConfig =
                withTimeout(20_000) {
                    withContext(Dispatchers.IO) { ffiServices.getRepoConfig() }
                }
            println("[APP_INIT] stage=getRepoConfig done")
            val knownRepos = repoConfig.knownRepos
            val lastUsedRepo =
                repoConfig.lastUsedRepoId?.let { lastUsedRepoId ->
                    knownRepos.find { repo -> repo.id == lastUsedRepoId }
                }
            val shouldOpenLastUsedRepo =
                if (lastUsedRepo == null) {
                    false
                } else {
                    val lastUsedRepoPath = lastUsedRepo.path
                    println("[APP_INIT] stage=isRepoUsable start path=$lastUsedRepoPath")
                    val usable =
                        withTimeout(20_000) {
                            withContext(Dispatchers.IO) { ffiServices.isRepoUsable(lastUsedRepoPath) }
                        }
                    println("[APP_INIT] stage=isRepoUsable done usable=$usable")
                    usable
                }

            if (shouldOpenLastUsedRepo && lastUsedRepo != null) {
                pendingOpenRepoPath = lastUsedRepo.path
                initState = AppInitState.OpeningRepo(repoPath = lastUsedRepo.path)
            } else {
                initState = AppInitState.Welcome(repos = knownRepos)
            }
        } catch (throwable: Throwable) {
            if (throwable is CancellationException) throw throwable
            cloneSourceUrlPendingOpen = null
            println("[APP_INIT] stage=bootstrap failed err=${throwable.message}")
            initState = AppInitState.Error(throwable)
        }
    }

    LaunchedEffect(pendingOpenRepoPath) {
        val repoPath = pendingOpenRepoPath ?: return@LaunchedEffect
        val startupMark = TimeSource.Monotonic.markNow()
        val startupPhaseId = Clock.System.now().toEpochMilliseconds().toString()
        val startupStageCount = 7
        try {
            initState = AppInitState.OpeningRepo(repoPath = repoPath)
            val container = withContext(Dispatchers.IO) {
                var fcx: FfiCtx? = null
                var tablesRepo: TablesRepoFfi? = null
                var blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi? = null
                var plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi? = null
                var drawerRepo: DrawerRepoFfi? = null
                var configRepo: ConfigRepoFfi? = null
                var dispatchRepo: DispatchRepoFfi? = null
                var progressRepo: ProgressRepoFfi? = null
                var cameraPreviewFfi: CameraPreviewFfi? = null
                var startupProgress: StartupProgressTask? = null
                try {
                    fcx =
                        withStartupStage("ffiServices.openRepoFfiCtx", startupMark, null) {
                            ffiServices.openRepoFfiCtx(repoPath)
                        }
                    val fcxReady = fcx ?: error("ffi context initialization failed")
                    progressRepo =
                        withStartupStage("ProgressRepoFfi.load", startupMark, null) {
                            ProgressRepoFfi.load(fcx = fcxReady)
                        }
                    startupProgress =
                        StartupProgressTask(
                            progressRepo = progressRepo ?: error("progress repo failed to load"),
                            taskId = STARTUP_PROGRESS_TASK_ID,
                            appElapsedMillis = { appStartMark.elapsedNow().inWholeMilliseconds },
                            totalStages = startupStageCount
                        )
                    startupProgress.begin(repoPath, startupMark.elapsedNow().toString(), startupPhaseId)
                    tablesRepo =
                        withStartupStage("TablesRepoFfi.load", startupMark, startupProgress) {
                            TablesRepoFfi.load(fcx = fcxReady)
                        }
                    blobsRepo =
                        withStartupStage("BlobsRepoFfi.load", startupMark, startupProgress) {
                            org.example.daybook.uniffi.BlobsRepoFfi.load(fcx = fcxReady)
                        }
                    plugsRepo =
                        withStartupStage("PlugsRepoFfi.load", startupMark, startupProgress) {
                            org.example.daybook.uniffi.PlugsRepoFfi
                                .load(fcx = fcxReady, blobsRepo = blobsRepo ?: error("blobs repo failed to load"))
                        }
                    drawerRepo =
                        withStartupStage("DrawerRepoFfi.load", startupMark, startupProgress) {
                            DrawerRepoFfi.load(fcx = fcxReady, plugsRepo = plugsRepo ?: error("plugs repo failed to load"))
                        }
                    configRepo =
                        withStartupStage("ConfigRepoFfi.load", startupMark, startupProgress) {
                            ConfigRepoFfi.load(fcx = fcxReady, plugRepo = plugsRepo ?: error("plugs repo failed to load"))
                        }
                    dispatchRepo =
                        withStartupStage("DispatchRepoFfi.load", startupMark, startupProgress) {
                            DispatchRepoFfi.load(fcx = fcxReady)
                        }
                    cameraPreviewFfi =
                        withStartupStage("CameraPreviewFfi.load", startupMark, startupProgress) {
                            CameraPreviewFfi.load()
                        }
                    withStartupStage("warmUpTablesRepo", startupMark, startupProgress) {
                        warmUpTablesRepo(tablesRepo ?: error("tables repo failed to load"))
                    }
                    startupProgress.complete(startupMark.elapsedNow().toString())
                    AppContainer(
                        ffiCtx = fcxReady,
                        drawerRepo = drawerRepo ?: error("drawer repo failed to load"),
                        tablesRepo = tablesRepo ?: error("tables repo failed to load"),
                        dispatchRepo = dispatchRepo ?: error("dispatch repo failed to load"),
                        progressRepo = progressRepo ?: error("progress repo failed to load"),
                        rtFfi = null,
                        plugsRepo = plugsRepo ?: error("plugs repo failed to load"),
                        configRepo = configRepo ?: error("config repo failed to load"),
                        blobsRepo = blobsRepo ?: error("blobs repo failed to load"),
                        syncRepo = null,
                        cameraPreviewFfi = cameraPreviewFfi ?: error("camera preview ffi failed to load")
                    )
                } catch (throwable: Throwable) {
                    if (throwable is CancellationException) throw throwable
                    val startupErrorMessage = throwable.message ?: throwable::class.simpleName ?: "unknown error"
                    startupProgress?.fail(startupErrorMessage, startupMark.elapsedNow().toString())
                    cameraPreviewFfi?.close()
                    progressRepo?.close()
                    dispatchRepo?.close()
                    drawerRepo?.close()
                    tablesRepo?.close()
                    plugsRepo?.close()
                    configRepo?.close()
                    blobsRepo?.close()
                    fcx?.close()
                    throw throwable
                }
            }
            initState =
                AppInitState.Ready(container)
        } catch (throwable: Throwable) {
            if (throwable is CancellationException) throw throwable
            cloneSourceUrlPendingOpen = null
            initState = AppInitState.Error(throwable)
        } finally {
            pendingOpenRepoPath = null
        }
    }

    LaunchedEffect(pendingForgetRepoId) {
        val repoId = pendingForgetRepoId ?: return@LaunchedEffect
        try {
            val repoConfig = withContext(Dispatchers.IO) { ffiServices.forgetKnownRepo(repoId) }
            selectedWelcomeRepo = null
            initState = AppInitState.Welcome(repos = repoConfig.knownRepos)
        } catch (throwable: Throwable) {
            initState = AppInitState.Error(throwable)
        } finally {
            pendingForgetRepoId = null
        }
    }

    LaunchedEffect(initState) {
        val ready = initState as? AppInitState.Ready ?: return@LaunchedEffect
        val current = ready.container
        if (current.syncRepo != null && current.rtFfi != null) return@LaunchedEffect

        try {
            println("[APP_INIT] stage=deferred SyncRepoFfi.load start")
            val syncRepo = withContext(Dispatchers.IO) {
                SyncRepoFfi.load(
                    fcx = current.ffiCtx,
                    configRepo = current.configRepo,
                    blobsRepo = current.blobsRepo,
                    drawerRepo = current.drawerRepo,
                    progressRepo = current.progressRepo
                )
            }
            println("[APP_INIT] stage=deferred SyncRepoFfi.load done")

            println("[APP_INIT] stage=deferred RtFfi.load start")
            val rtFfi = withContext(Dispatchers.IO) {
                RtFfi.load(
                    fcx = current.ffiCtx,
                    drawerRepo = current.drawerRepo,
                    plugsRepo = current.plugsRepo,
                    dispatchRepo = current.dispatchRepo,
                    progressRepo = current.progressRepo,
                    blobsRepo = current.blobsRepo,
                    configRepo = current.configRepo,
                    deviceId = "compose-client",
                    startupProgressTaskId = STARTUP_PROGRESS_TASK_ID
                )
            }
            println("[APP_INIT] stage=deferred RtFfi.load done")
            initState = AppInitState.Ready(current.copy(syncRepo = syncRepo, rtFfi = rtFfi))
        } catch (throwable: Throwable) {
            if (throwable is CancellationException) throw throwable
            initState = AppInitState.Error(throwable)
        }
    }

    DaybookTheme(themeConfig = config.theme) {
        when (val state = initState) {
            is AppInitState.Loading -> {
                LoadingScreen()
            }

            is AppInitState.Welcome -> {
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
                    pendingForgetRepoId = pendingForgetRepoId,
                    onSelectedWelcomeRepoChange = { selectedWelcomeRepo = it },
                    onCloneUiStateChange = { cloneUiState = it },
                    onCreateRepoUiStateChange = { createRepoUiState = it },
                    onCloneSourceUrlPendingOpenChange = { cloneSourceUrlPendingOpen = it },
                    onCloneInitRequestChange = { cloneInitRequest = it },
                    onCreateRepoInitRequestChange = { createRepoInitRequest = it },
                    onPendingOpenRepoPath = { pendingOpenRepoPath = it },
                    onPendingForgetRepoId = { pendingForgetRepoId = it }
                )
            }

            is AppInitState.OpeningRepo -> {
                val syncingState = cloneUiState as? CloneUiState.Syncing
                if (syncingState != null) {
                    CloneSyncScreen(
                        progressRepo = null,
                        state = syncingState,
                        onSyncInBackground = {},
                        onRetry = {
                            cloneSourceUrlPendingOpen = syncingState.sourceUrl
                            pendingOpenRepoPath = state.repoPath
                        }
                    )
                } else {
                    LoadingScreen(message = "Opening repo: ${state.repoPath}")
                }
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
                val drawerVm: DrawerViewModel = viewModel { DrawerViewModel(appContainer.drawerRepo) }
                val docEditorStore: DocEditorStoreViewModel =
                    viewModel { DocEditorStoreViewModel(appContainer.drawerRepo) }
                var shutdownDone by remember(appContainer.ffiCtx) { mutableStateOf(false) }

                LaunchedEffect(shutdownRequested, appContainer.ffiCtx, shutdownDone) {
                    if (shutdownRequested && !shutdownDone) {
                        shutdownAppContainer(appContainer)
                        shutdownDone = true
                        onShutdownCompleted?.invoke()
                    }
                }

                // Ensure FFI resources are closed when the composition leaves
                androidx.compose.runtime.DisposableEffect(appContainer.ffiCtx) {
                    onDispose {
                        if (!autoShutdownOnDispose) {
                            return@onDispose
                        }
                        if (!shutdownDone) {
                            runBlocking(Dispatchers.IO) {
                                shutdownAppContainer(appContainer)
                            }
                        }
                    }
                }

                Box(modifier = Modifier.fillMaxSize()) {
                    CompositionLocalProvider(
                        LocalContainer provides appContainer,
                        LocalDrawerViewModel provides drawerVm,
                        LocalDocEditorStore provides docEditorStore
                    ) {
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
                                    val bigDialogState = remember { BigDialogState() }
                                    AdaptiveAppLayout(
                                        modifier = surfaceModifier,
                                        navController = navController,
                                        extraAction = extraAction,
                                        bigDialogState = bigDialogState
                                    )
                                }
                            }
                        }
                    }
                    if (shutdownRequested && !shutdownDone) {
                        Surface(
                            modifier = Modifier.fillMaxSize(),
                            color = MaterialTheme.colorScheme.surface
                        ) {
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Column(
                                    horizontalAlignment = Alignment.CenterHorizontally,
                                    verticalArrangement = Arrangement.spacedBy(12.dp)
                                ) {
                                    CircularProgressIndicator()
                                    Text("Shutting down…", style = MaterialTheme.typography.bodyLarge)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // FIXME: does this really need to live at the top level App composable??
    LaunchedEffect(initState, cloneSourceUrlPendingOpen) {
        val ready = initState as? AppInitState.Ready ?: return@LaunchedEffect
        val sourceUrl = cloneSourceUrlPendingOpen ?: return@LaunchedEffect
        val syncRepo = ready.container.syncRepo
        if (syncRepo == null) {
            cloneUiState =
                CloneUiState.Syncing(
                    sourceUrl = sourceUrl,
                    initialSyncComplete = false,
                    phaseMessage = "Starting sync services…",
                    errorMessage = null
                )
            return@LaunchedEffect
        }
        cloneUiState =
            CloneUiState.Syncing(
                sourceUrl = sourceUrl,
                initialSyncComplete = false,
                phaseMessage = "Pulling required docs…",
                errorMessage = null
            )
        try {
            syncRepo.connectUrl(sourceUrl)
            val current = cloneUiState as? CloneUiState.Syncing
            if (current != null && current.sourceUrl == sourceUrl) {
                cloneUiState =
                    current.copy(
                        initialSyncComplete = true,
                        phaseMessage = "Required docs synced. Remaining sync is running.",
                        errorMessage = null
                    )
            }
        } catch (error: Throwable) {
            // FIXME: connect URL goes through FFI, does it also
            // drop the rust future?
            if (error is CancellationException) {
                // Normal during route/state transitions; not a sync failure.
                return@LaunchedEffect
            }
            val current = cloneUiState as? CloneUiState.Syncing
            if (current != null && current.sourceUrl == sourceUrl) {
                cloneUiState =
                    current.copy(
                        initialSyncComplete = false,
                        phaseMessage = "Failed while pulling required docs.",
                        errorMessage = "Connect failed: ${describeThrowable(error)}"
                    )
            }
        } finally {
            if (cloneSourceUrlPendingOpen == sourceUrl) {
                cloneSourceUrlPendingOpen = null
            }
        }
    }
}

private suspend fun shutdownAppContainer(appContainer: AppContainer) {
    println("[APP_SHUTDOWN] flushing to disk: begin")
    val syncRepo = appContainer.syncRepo
    if (syncRepo != null) {
        withContext(Dispatchers.IO) {
            println("[APP_SHUTDOWN] flushing to disk: stopping sync repo")
            syncRepo.stop()
        }
    }
    withContext(Dispatchers.IO) {
        println("[APP_SHUTDOWN] flushing to disk: stopping progress repo")
        appContainer.progressRepo.stop()
    }
    println("[APP_SHUTDOWN] flushing to disk: closing repo handles")
    appContainer.drawerRepo.close()
    appContainer.tablesRepo.close()
    appContainer.dispatchRepo.close()
    appContainer.progressRepo.close()
    appContainer.rtFfi?.close()
    appContainer.plugsRepo.close()
    appContainer.configRepo.close()
    appContainer.blobsRepo.close()
    syncRepo?.close()
    appContainer.cameraPreviewFfi.close()
    appContainer.ffiCtx.close()
    println("[APP_SHUTDOWN] flushing to disk: complete")
}

@Composable
fun AdaptiveAppLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    bigDialogState: BigDialogState
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
        bigDialogState = bigDialogState,
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
    bigDialogState: BigDialogState,
    modifier: Modifier = Modifier
) {
    Box(modifier = modifier.fillMaxSize()) {
        when (navigationType) {
            DaybookNavigationType.PERMANENT_NAVIGATION_DRAWER -> {
                ExpandedLayout(
                    modifier = Modifier.fillMaxSize(),
                    navController = navController,
                    extraAction = extraAction,
                    contentType = contentType,
                    onShowCloneShare = { bigDialogState.show() }
                )
            }

            DaybookNavigationType.NAVIGATION_RAIL -> {
                ExpandedLayout(
                    modifier = Modifier.fillMaxSize(),
                    navController = navController,
                    extraAction = extraAction,
                    contentType = contentType,
                    onShowCloneShare = { bigDialogState.show() }
                )
            }

            DaybookNavigationType.BOTTOM_NAVIGATION -> {
                CompactLayout(
                    modifier = Modifier.fillMaxSize(),
                    navController = navController,
                    extraAction = extraAction,
                    contentType = contentType,
                    onShowCloneShare = { bigDialogState.show() }
                )
            }
        }

        BigDialogHost(
            state = bigDialogState,
            narrowScreen = navigationType == DaybookNavigationType.BOTTOM_NAVIGATION,
            modifier = Modifier.fillMaxSize()
        ) {
            CloneShareDialogContent(onClose = { bigDialogState.dismiss() })
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
    val drawerVm = LocalDrawerViewModel.current

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
        composable(
            route = AppScreens.Drawer.name,
            enterTransition = { EnterTransition.None },
            exitTransition = { ExitTransition.None },
            popEnterTransition = { EnterTransition.None },
            popExitTransition = { ExitTransition.None }
        ) {
            ProvideChromeState(ChromeState(title = "Drawer")) {
                DrawerScreen(
                    drawerVm = drawerVm,
                    onOpenDoc = {
                        navController.navigate(AppScreens.DocEditor.name) { launchSingleTop = true }
                    },
                    modifier = modifier,
                )
            }
        }
        composable(
            route = AppScreens.DocEditor.name,
            enterTransition = {
                slideInHorizontally(
                    animationSpec = tween(240),
                    initialOffsetX = { fullWidth -> fullWidth }
                )
            },
            exitTransition = { ExitTransition.None },
            popEnterTransition = { EnterTransition.None },
            popExitTransition = {
                slideOutHorizontally(
                    animationSpec = tween(240),
                    targetOffsetX = { fullWidth -> fullWidth }
                )
            }
        ) {
            ProvideChromeState(
                ChromeState(
                    title = "Doc Editor",
                    onBack = { navController.popBackStack() }
                )
            ) {
                DocEditorScreen(
                    contentType = contentType,
                    modifier = modifier
                )
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
                                    permCtx.requestPermissions(
                                        PermissionRequest(
                                            camera = true,
                                            notifications = true,
                                            microphone = true,
                                            overlay = true,
                                            storageRead = true,
                                            storageWrite = true
                                        )
                                    )
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
                text = "🌞",
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
