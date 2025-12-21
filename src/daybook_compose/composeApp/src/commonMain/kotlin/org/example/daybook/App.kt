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
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.Text
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
import org.example.daybook.AdditionalFeatureButton
import org.example.daybook.ChromeState
import org.example.daybook.ProvideChromeState
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
import org.example.daybook.capture.CameraCaptureContext
import org.example.daybook.capture.ProvideCameraCaptureContext
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.documents.DocumentsScreen
import org.example.daybook.settings.SettingsScreen
import org.example.daybook.tables.CompactLayout
import org.example.daybook.tables.ExpandedLayout
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.TablesEventListener
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.Panel
import org.example.daybook.uniffi.core.Tab
import org.example.daybook.uniffi.core.Table
import org.example.daybook.uniffi.core.TablesEvent
import org.example.daybook.uniffi.core.Uuid
import org.example.daybook.uniffi.core.Window
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

enum class DaybookNavigationType {
    BOTTOM_NAVIGATION, NAVIGATION_RAIL, PERMANENT_NAVIGATION_DRAWER
}

enum class DaybookContentType {
    LIST_ONLY, LIST_AND_DETAIL
}

val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val requestAllPermissions: () -> Unit = {},
) {
    val hasAll = hasCamera and hasNotifications and hasMicrophone and hasOverlay
}

data class AppContainer(
    val ffiCtx: FfiCtx, 
    val drawerRepo: DrawerRepoFfi, 
    val tablesRepo: TablesRepoFfi,
    val configRepo: ConfigRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi
)

val LocalContainer = staticCompositionLocalOf<AppContainer> {
    error("no AppContainer provided")
}

data class AppConfig(
    val theme: ThemeConfig = ThemeConfig.Dark,
)

enum class AppScreens {
    Home, Capture, Tables, Settings, Documents
}

private sealed interface AppInitState {
    data object Loading : AppInitState
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

class TablesViewModel(
    val tablesRepo: TablesRepoFfi
) : ViewModel() {
    private val _tablesState = MutableStateFlow(TablesState.Loading as TablesState)
    val tablesState = _tablesState.asStateFlow()
    
    // Selected table state
    private val _selectedTableId = MutableStateFlow<Uuid?>(null)
    val selectedTableId = _selectedTableId.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    // Listener instance implemented on Kotlin side
    private val listener = object : TablesEventListener {
        override fun onTablesEvent(event: TablesEvent) {
            // Ensure UI updates happen on main thread
            viewModelScope.launch {
                when (event) {
                    is TablesEvent.ListChanged -> {
                        // Full refresh when list structure changes
                        refreshTables()
                    }

                    is TablesEvent.WindowChanged -> {
                        // Targeted refresh for specific window
                        updateWindow(event.id)
                    }

                    is TablesEvent.TabChanged -> {
                        // Targeted refresh for specific tab
                        updateTab(event.id)
                    }

                    is TablesEvent.PanelChanged -> {
                        // Targeted refresh for specific panel
                        updatePanel(event.id)
                    }

                    is TablesEvent.TableChanged -> {
                        // Targeted refresh for specific table
                        updateTable(event.id)
                    }
                }
            }
        }
    }

    init {
        // Initial load
        loadTables()
        // Register listener
        viewModelScope.launch {
            listenerRegistration = tablesRepo.ffiRegisterListener(listener)
        }
    }

    private suspend fun refreshTables() {
        _tablesState.value = TablesState.Loading
        try {
            val windows = tablesRepo.listWindows()
            val tabs = tablesRepo.listTabs()
            val panels = tablesRepo.listPanels()
            val tables = tablesRepo.listTables()
            _tablesState.value = TablesState.Data(
                windows = windows.associateBy { it.id },
                tabs = tabs.associateBy { it.id },
                panels = panels.associateBy { it.id },
                tables = tables.associateBy { it.id })
            
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
        } else emptyList()
    }

    suspend fun initializeFirstTime(): Result<Unit> {
        return try {
            // The auto-creation logic is now handled in the Rust code
            // Just trigger a refresh to ensure we have data
            refreshTables()
            Result.success(Unit)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    suspend fun createNewTable(): Result<Uuid> {
        return try {
            val newTableId = tablesRepo.createNewTable()
            // Select the new table
            _selectedTableId.value = newTableId
            Result.success(newTableId)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    suspend fun createNewTab(tableId: Uuid): Result<Uuid> {
        return try {
            val newTabId = tablesRepo.createNewTab(tableId)
            Result.success(newTabId)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    suspend fun removeTab(tabId: Uuid): Result<Unit> {
        return try {
            tablesRepo.removeTab(tabId)
            Result.success(Unit)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    override fun onCleared() {
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
    navController: NavHostController = rememberNavController(),
) {
    var initAttempt by remember { mutableStateOf(0) }
    var initState by remember { mutableStateOf<AppInitState>(AppInitState.Loading) }

    LaunchedEffect(initAttempt) {
        initState = AppInitState.Loading
        val fcx = FfiCtx.forFfi()
        val drawerRepo = DrawerRepoFfi.load(fcx = fcx)
        val tablesRepo = TablesRepoFfi.load(fcx = fcx)
        val configRepo = ConfigRepoFfi.load(fcx = fcx)
        val blobsRepo = org.example.daybook.uniffi.BlobsRepoFfi.load(fcx = fcx)
        
        // Initialize first-time data if needed
        val tablesViewModel = TablesViewModel(tablesRepo)
        tablesViewModel.initializeFirstTime()
        
        initState = AppInitState.Ready(
            AppContainer(
                ffiCtx = fcx, 
                drawerRepo = drawerRepo, 
                tablesRepo = tablesRepo,
                configRepo = configRepo,
                blobsRepo = blobsRepo
            )
        )
    }

    DaybookTheme(themeConfig = config.theme) {
        when (val state = initState) {
            is AppInitState.Loading -> {
                LoadingScreen()
            }

            is AppInitState.Error -> {
                ErrorScreen(
                    title = "Failed to initialize",
                    message = state.throwable.message ?: "Unknown error",
                    onRetry = { initAttempt += 1 })
            }

            is AppInitState.Ready -> {
                val appContainer = state.container

                // Ensure FFI resources are closed when the composition leaves
                androidx.compose.runtime.DisposableEffect(appContainer) {
                    onDispose {
                        appContainer.drawerRepo.close()
                        appContainer.tablesRepo.close()
                        appContainer.configRepo.close()
                        appContainer.ffiCtx.close()
                    }
                }
    
                CompositionLocalProvider(
                    LocalContainer provides appContainer,
                ) {
                    // Provide camera capture context for coordination between camera and bottom bar
                    val cameraCaptureContext = remember { CameraCaptureContext() }
                    val chromeStateManager = remember { ChromeStateManager() }
                    ProvideCameraCaptureContext(cameraCaptureContext) {
                        CompositionLocalProvider(LocalChromeStateManager provides chromeStateManager) {
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
                modifier = modifier, navController = navController, extraAction = extraAction, contentType = contentType
            )
        }

        DaybookNavigationType.NAVIGATION_RAIL -> {
            ExpandedLayout(
                modifier = modifier, navController = navController, extraAction = extraAction, contentType = contentType
            )
        }

        DaybookNavigationType.BOTTOM_NAVIGATION -> {
            CompactLayout(
                modifier = modifier, navController = navController, extraAction = extraAction, contentType = contentType
            )
        }
    }
}


@Composable
fun TablesScreen(
    modifier: Modifier = Modifier
) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel {
        TablesViewModel(tablesRepo = tablesRepo)
    }

    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value
    
    // Get selected table from state instead of calling suspend function
    val selectedTable = if (tablesState is TablesState.Data && selectedTableId != null) {
        tablesState.tables[selectedTableId]
    } else null
    
    val tabsForSelectedTable = if (selectedTable != null && tablesState is TablesState.Data) {
        selectedTable.tabs.mapNotNull { tabId -> tablesState.tabs[tabId] }
    } else emptyList()
    
    // Create chrome state with feature buttons
    val chromeState = remember(selectedTableId, tablesState) {
        ChromeState(
            additionalFeatureButtons = listOf(
                // Prominent button for creating new table
                AdditionalFeatureButton(
                    key = "tables_new_table",
                    icon = { Text("➕") },
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
                        icon = { Text("📄") },
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
                } else null,
                // Non-prominent button for table settings
                AdditionalFeatureButton(
                    key = "tables_settings",
                    icon = { Text("⚙️") },
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
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                Text("Error loading tables: ${tablesState.error.message()}")
            }
        }

        is TablesState.Loading -> {
            Column(
                modifier = modifier,
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                CircularProgressIndicator()
                Text("Loading tables...")
            }
        }

        is TablesState.Data -> {
            Column(
                modifier = modifier.padding(16.dp),
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
                    text = "Overall State:", modifier = Modifier.padding(bottom = 8.dp)
                )
                Text("  • Windows: ${tablesState.windows.size}")
                Text("  • Tables: ${tablesState.tables.size}")
                Text("  • Tabs: ${tablesState.tabs.size}")
                Text("  • Panels: ${tablesState.panels.size}")
                
                Spacer(modifier = Modifier.height(24.dp))
                
                // All Tables List
                Text(
                    text = "All Tables:", modifier = Modifier.padding(bottom = 8.dp)
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
    navController: NavHostController,
) {
    NavHost(
        startDestination = AppScreens.Home.name,
        navController = navController,
    ) {
        composable(route = AppScreens.Capture.name) {
            // CaptureScreen provides its own chrome state internally
            CaptureScreen(modifier = modifier)
        }
        composable(route = AppScreens.Tables.name) {
            TablesScreen(modifier = modifier)
        }
        composable(route = AppScreens.Settings.name) {
            ProvideChromeState(ChromeState(title = "Settings")) {
                SettingsScreen(modifier = modifier)
            }
        }
        composable(route = AppScreens.Documents.name) {
            ProvideChromeState(ChromeState(title = "Documents")) {
                DocumentsScreen(modifier = modifier, contentType = contentType)
            }
        }
        composable(route = AppScreens.Home.name) {
            ProvideChromeState(ChromeState.Empty) {
            var showContent by remember { mutableStateOf(false) }
            Column(
                modifier = modifier,
                horizontalAlignment = Alignment.CenterHorizontally,
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
                        Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally
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
private fun LoadingScreen() {
    val infiniteTransition = rememberInfiniteTransition(label = "loading_transition")
    val scale by infiniteTransition.animateFloat(
        initialValue = 1f, targetValue = 1.1f, animationSpec = infiniteRepeatable(
            animation = tween(600, easing = FastOutSlowInEasing), repeatMode = RepeatMode.Reverse
        ), label = "loading_scale"
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
                text = "📖", fontSize = 80.sp, modifier = Modifier.scale(scale)
            )
            Spacer(Modifier.height(24.dp))
            Text(
                "Preparing Daybook…",
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.8f)
            )
        }
    }
}

@Composable
private fun ErrorScreen(
    title: String,
    message: String,
    onRetry: () -> Unit,
) {
    Box(
        modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center
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
