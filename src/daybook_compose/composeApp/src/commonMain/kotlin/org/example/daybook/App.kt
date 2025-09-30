
@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.PermanentDrawerSheet
import androidx.compose.material3.PermanentNavigationDrawer
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
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
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.DocsRepo
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.TablesRepo
import org.example.daybook.uniffi.Window
import org.example.daybook.uniffi.Tab
import org.example.daybook.uniffi.Panel
import org.example.daybook.uniffi.Table
import org.example.daybook.uniffi.TablesEvent
import org.example.daybook.uniffi.TablesEventListener
import org.example.daybook.uniffi.ListenerRegistration
import org.example.daybook.uniffi.Uuid
import org.example.daybook.uniffi.FfiException
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val requestAllPermissions: () -> Unit = {},
) {
    val hasAll = hasCamera and
            hasNotifications and
            hasMicrophone and
            hasOverlay
}

data class AppContainer(
    val ffiCtx: FfiCtx,
    val docsRepo: DocsRepo,
    val tablesRepo: TablesRepo
)

val LocalContainer = staticCompositionLocalOf<AppContainer> {
    error("no AppContainer provided")
}

data class AppConfig(
    val theme: ThemeConfig = ThemeConfig.Dark,
)

enum class AppScreens {
    Home,
    Capture,
    Tables
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
    val tablesRepo: TablesRepo
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
            val windows = tablesRepo.ffiListWindows()
            val tabs = tablesRepo.ffiListTabs()
            val panels = tablesRepo.ffiListPanels()
            val tables = tablesRepo.ffiListTables()
            _tablesState.value = TablesState.Data(
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
                val updatedWindow = tablesRepo.ffiGetWindow(windowId)
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
                val updatedTab = tablesRepo.ffiGetTab(tabId)
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
                val updatedPanel = tablesRepo.ffiGetPanel(panelId)
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
                val updatedTable = tablesRepo.ffiGetTable(tableId)
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
    
    suspend fun getSelectedTable(): Table? {
        val currentState = _tablesState.value
        val selectedId = _selectedTableId.value
        return if (currentState is TablesState.Data && selectedId != null) {
            currentState.tables[selectedId]
        } else {
            // If no explicit selection, try to get the selected table from the repo
            try {
                tablesRepo.ffiGetSelectedTable()
            } catch (e: FfiException) {
                null
            }
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

    suspend fun createNewTable(): Result<Table> {
        return try {
            val newTable = tablesRepo.ffiCreateNewTable()
            // Select the new table
            _selectedTableId.value = newTable.id
            Result.success(newTable)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    suspend fun createNewTab(tableId: Uuid): Result<Tab> {
        return try {
            val newTab = tablesRepo.ffiCreateNewTab(tableId)
            Result.success(newTab)
        } catch (err: FfiException) {
            Result.failure(err)
        }
    }

    suspend fun removeTab(tabId: Uuid): Result<Unit> {
        return try {
            tablesRepo.ffiRemoveTab(tabId)
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
        print("XXXX here")
        val fcx = FfiCtx.forFfi()
        val docsRepo = DocsRepo.forFfi(fcx = fcx)
        val tablesRepo = TablesRepo.forFfi(fcx = fcx)
        
        // Initialize first-time data if needed
        val tablesViewModel = TablesViewModel(tablesRepo)
        tablesViewModel.initializeFirstTime()
        
        initState = AppInitState.Ready(
            AppContainer(
                ffiCtx = fcx,
                docsRepo = docsRepo,
                tablesRepo = tablesRepo
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
                    message = state.throwable.message ?: "Unknown error",
                    onRetry = { initAttempt += 1 }
                )
            }

            is AppInitState.Ready -> {
                val appContainer = state.container

                // Ensure FFI resources are closed when the composition leaves
                androidx.compose.runtime.DisposableEffect(appContainer) {
                    onDispose {
                        appContainer.docsRepo.close()
                        appContainer.tablesRepo.close()
                        appContainer.ffiCtx.close()
                    }
                }

                CompositionLocalProvider(
                    LocalContainer provides appContainer,
                ) {
                    AppScaffold(
                        modifier = surfaceModifier,
                        navController = navController
                    ) { innerPadding ->
                        Routes(
                            modifier = Modifier.padding(innerPadding),
                            extraAction = extraAction,
                            navController = navController
                        )
                    }
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun AppScaffold(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    content: @Composable (innerPadding: PaddingValues) -> Unit
) {
    Scaffold(
        modifier = modifier,
        topBar = {
            TopAppBar(
                title = { Text("Daybook") }
            )
        }
    ) { innerPadding ->
        Row(modifier = Modifier.fillMaxSize().padding(innerPadding)) {
            // Left Navigation Rail for Tables
            LeftTableNavigationRail()
            
            // Center Navigation Drawer for Tabs
            PermanentNavigationDrawer(
                drawerContent = {
                    PermanentDrawerSheet(
                        modifier = Modifier.padding(10.dp),
                    ) {
                        Column {
                            // Show all tabs from the selected table
                            TablesTabsList()
                        }
                    }
                }
            ) {
                Box(modifier = Modifier.weight(1f)) {
                    content(innerPadding)
                }
            }
            
            // Right Navigation Rail for Features
            RightFeaturesNavigationRail()
        }
    }
}

@Composable
fun LeftTableNavigationRail() {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value
    
    NavigationRail(
        modifier = Modifier.width(80.dp)
    ) {
        Spacer(modifier = Modifier.height(16.dp))
        
        // Add Table Button
        FloatingActionButton(
            onClick = { 
                vm.viewModelScope.launch {
                    vm.createNewTable()
                }
            },
            modifier = Modifier.size(48.dp)
        ) {
            Text("+")
        }
        
        Spacer(modifier = Modifier.height(16.dp))
        
        // Table List
        when (tablesState) {
            is TablesState.Data -> {
                tablesState.tablesList.forEach { table ->
                    NavigationRailItem(
                        selected = selectedTableId == table.id,
                        onClick = { vm.selectTable(table.id) },
                        icon = {
                            Text("📁")
                        },
                        label = { Text(table.title) }
                    )
                }
            }
            is TablesState.Loading -> {
                CircularProgressIndicator(modifier = Modifier.size(24.dp))
            }
            is TablesState.Error -> {
                Text("Error")
            }
        }
    }
}

@Composable
fun RightFeaturesNavigationRail() {
    NavigationRail(
        modifier = Modifier.width(80.dp)
    ) {
        Spacer(modifier = Modifier.height(16.dp))
        
        // Placeholder feature buttons
        NavigationRailItem(
            selected = false,
            onClick = { /* TODO: Settings */ },
            icon = {
                Text("⚙️")
            },
            label = { Text("Settings") }
        )
        
        NavigationRailItem(
            selected = false,
            onClick = { /* TODO: Features */ },
            icon = {
                Text("⚙️")
            },
            label = { Text("Features") }
        )
    }
}

@Composable
fun TablesTabsList() {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState()
    
    LaunchedEffect(tablesState.value) {
        if (tablesState.value is TablesState.Data) {
            val selectedTable = vm.getSelectedTable()
            if (selectedTable != null) {
                vm.selectTable(selectedTable.id)
            }
        }
    }
    
    val currentState = tablesState.value
    val selectedTableId = vm.selectedTableId.collectAsState()
    
    when (currentState) {
        is TablesState.Data -> {
            val selectedTable = selectedTableId.value?.let { currentState.tables[it] }
            if (selectedTable != null) {
                Column {
                    Row(
                        modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "Tabs in ${selectedTable.title}",
                            modifier = Modifier.weight(1f)
                        )
                        // Add new tab button
                        FloatingActionButton(
                            onClick = { 
                                vm.viewModelScope.launch {
                                    vm.createNewTab(selectedTable.id)
                                }
                            },
                            modifier = Modifier.size(32.dp)
                        ) {
                            Text("+", fontSize = 12.sp)
                        }
                    }
                    
                    selectedTable.tabs.mapNotNull { tabId -> currentState.tabs[tabId] }.forEach { tab ->
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            NavigationDrawerItem(
                                selected = false, // TODO: Track selected tab
                                onClick = { /* TODO: Select tab */ },
                                icon = {
                                    Text("📄")
                                },
                                badge = {
                                    // Close tab button
                                    FloatingActionButton(
                                        onClick = { 
                                            vm.viewModelScope.launch {
                                                vm.removeTab(tab.id)
                                            }
                                        },
                                        modifier = Modifier.size(24.dp).padding(end = 8.dp)
                                    ) {
                                        Text("×", fontSize = 10.sp)
                                    }
                                },
                                label = { Text(tab.title) },
                                modifier = Modifier.weight(1f)
                            )
                        }
                    }
                }
            } else {
                Text(
                    text = "No table selected",
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                )
            }
        }
        is TablesState.Loading -> {
            Text(
                text = "Loading...",
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
            )
        }
        is TablesState.Error -> {
            Text(
                text = "Error loading tables",
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
            )
        }
    }
}

@Composable
fun TablesScreen() {
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

    when (tablesState) {
        is TablesState.Error -> {
            Column(
                modifier = Modifier
                    .safeContentPadding()
                    .fillMaxSize(),
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                Text("Error loading tables: ${tablesState.error.message()}")
            }
        }
        is TablesState.Loading -> {
            Column(
                modifier = Modifier
                    .safeContentPadding()
                    .fillMaxSize(),
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                CircularProgressIndicator()
                Text("Loading tables...")
            }
        }
        is TablesState.Data -> {
            Column(
                modifier = Modifier
                    .safeContentPadding()
                    .fillMaxSize()
                    .padding(16.dp),
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

@Composable
fun Routes(
    modifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController,
) {

    NavHost(
        startDestination = AppScreens.Home.name,
        navController = navController,
        modifier = modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
    ) {
        composable(route = AppScreens.Capture.name) {
            CaptureScreen()
        }
        composable(route = AppScreens.Tables.name) {
            TablesScreen()
        }
        composable(route = AppScreens.Home.name) {
            var showContent by remember { mutableStateOf(false) }
            Column(
                modifier = Modifier
                    .safeContentPadding()
                    .fillMaxSize(),
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

@Composable
private fun LoadingScreen() {
    Box(
        modifier = Modifier
            .fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            CircularProgressIndicator()
            Spacer(Modifier.height(16.dp))
            Text("Preparing Daybook…")
        }
    }
}

@Composable
private fun ErrorScreen(
    message: String,
    onRetry: () -> Unit,
) {
    Box(
        modifier = Modifier
            .fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text("Failed to initialize")
            Spacer(Modifier.height(8.dp))
            Text(message)
            Spacer(Modifier.height(16.dp))
            Button(onClick = onRetry) { Text("Retry") }
        }
    }
}
