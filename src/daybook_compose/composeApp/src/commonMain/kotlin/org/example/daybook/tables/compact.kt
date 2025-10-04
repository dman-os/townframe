@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.BottomAppBar
// Use our custom reveal scaffold
import org.example.daybook.tables.RevealBottomSheetScaffold
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SheetState
import androidx.compose.material3.SheetValue
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.rememberBottomSheetScaffoldState
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.sizeIn
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.height
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import kotlinx.coroutines.Job
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.asStateFlow
import org.example.daybook.LocalContainer
import org.example.daybook.Routes
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.uniffi.Tab
import org.example.daybook.uniffi.Table
import org.example.daybook.uniffi.Uuid

// ViewModel-based hover-hold controller for abstracting the hover-to-create pattern
class HoverHoldControllerViewModel : androidx.lifecycle.ViewModel() {
    private val _isHovering = kotlinx.coroutines.flow.MutableStateFlow(false)
    val isHovering: kotlinx.coroutines.flow.StateFlow<Boolean> = _isHovering.asStateFlow()
    
    private val _ready = kotlinx.coroutines.flow.MutableStateFlow(false)
    val ready = _ready.asStateFlow()
    // Optional label for logging / debugging
    var label: String = "unknown"
    
    var targetRect: Rect? = null
    private var job: Job? = null
    private var leaveJob: Job? = null
    private val delayMs = 500L // 0.5 seconds
    // private val leaveGraceMs = 5L

    fun update(windowPos: Offset?) {
        val rect = targetRect
        if (rect != null && windowPos != null && rect.contains(windowPos)) {
            // entered target rect
            leaveJob?.cancel()
            leaveJob = null
            if (!_isHovering.value) {
                _isHovering.value = true
                job?.cancel()
                job = viewModelScope.launch {
                    kotlinx.coroutines.delay(delayMs)
                    if (_isHovering.value) {
                        _ready.value = true
                        // debug: ready
                    }
                }
                // debug: start hover
            }
        } else {
            // exited target rect; start a short grace timer before canceling to avoid jitter
            if (_isHovering.value) {
                leaveJob?.cancel()
                leaveJob = viewModelScope.launch {
                    // kotlinx.coroutines.delay(leaveGraceMs)
                    if (rect == targetRect) {
                        // still outside
                        _isHovering.value = false
                        job?.cancel()
                        job = null
                        _ready.value = false
                        // debug: cancel hover
                    }
                }
            }
        }
    }

    fun cancel() {
        job?.cancel()
        job = null
        _isHovering.value = false
        _ready.value = false
        // debug: cancel called
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun CompactLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null
) {
    var showFeaturesMenu by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    val density = LocalDensity.current
    val revealSheetState = rememberRevealBottomSheetState(initiallyVisible = false)
    var sheetContent by remember { mutableStateOf(SheetContent.TABS) }
    // sheet content collapsed to tabs only (we use nav rail for table switching)
    var isSheetManuallyOpened by rememberSaveable { mutableStateOf(false) }

    var tabItemLayouts by remember { mutableStateOf(mapOf<Uuid, Rect>()) }
    var tableItemLayouts by remember { mutableStateOf(mapOf<Uuid, Rect>()) }
    var highlightedTable by remember { mutableStateOf<Uuid?>(null) }
    var highlightedTab by remember { mutableStateOf<Uuid?>(null) }
    var isDragging by remember { mutableStateOf(false) }
    var addButtonWindowRect by remember { mutableStateOf<Rect?>(null) }
    var lastDragWindowPos by remember { mutableStateOf<Offset?>(null) }
    // Hover-hold controllers (abstracted) -----------------------------
    // Use distinct ViewModel instances for tab vs table controllers by supplying keys
    val addTabController = viewModel<HoverHoldControllerViewModel>(key = "addTab")
    addTabController.label = "addTab"
    val addTableController = viewModel<HoverHoldControllerViewModel>(key = "addTable")
    addTableController.label = "addTable"
    val addTabReadyState = addTabController.ready.collectAsState()
    val addTableReadyState = addTableController.ready.collectAsState()
    var addTableButtonWindowRect by remember { mutableStateOf<Rect?>(null) }

    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    var pendingTabSelection by remember { mutableStateOf<Uuid?>(null) }

    // Clear cached tab layout rects whenever the selected table or sheet content changes
    // FIXME:
    LaunchedEffect(vm.tablesState.value) {
        tabItemLayouts = mapOf()
        tableItemLayouts = mapOf()
        highlightedTab = null
        highlightedTable = null
    }

    LaunchedEffect(isSheetManuallyOpened) {
        if (isSheetManuallyOpened) revealSheetState.show(tween(0)) else revealSheetState.hide(tween(0))
    }

    val centerNavBarContent: @Composable RowScope.() -> Unit = {
        // When sheet is open, show controls (add button). When closed, show current tab title.
        if (isSheetManuallyOpened) {
            // Add-tab button expands to fill the center area
            Button(
                onClick = {
                    scope.launch {
                        val sel = vm.getSelectedTable()
                        if (sel != null) {
                            val res = vm.createNewTab(sel.id)
                            if (res.isSuccess) {
                                val newTab = res.getOrNull()
                                if (newTab != null) {
                                    isSheetManuallyOpened = !isSheetManuallyOpened
                                    vm.selectTab(newTab.id)
                                }
                            }
                        }
                    }
                },
                modifier = Modifier
                    .fillMaxWidth()
                    .weight(1f)
                    .onGloballyPositioned {
                        val r = it.boundsInWindow()
                        if (r.width > 0f && r.height > 0f) {
                            addButtonWindowRect = r
                            // debug: addButtonWindowRect set to $r
                        } else {
                            // debug: addButtonWindowRect ignored empty rect $r
                        }
                    },
                colors = if (addTabReadyState.value) ButtonDefaults.filledTonalButtonColors() else ButtonDefaults.buttonColors()
            ) {
                if (addTabReadyState.value) Text("Release to Add") else Text("Add Tab")
            }
        } else {
            val tablesRepo = LocalContainer.current.tablesRepo
            val vmLocal = viewModel { TablesViewModel(tablesRepo) }
            val selectedTableId = vmLocal.selectedTableId.collectAsState().value
            val tablesState = vmLocal.tablesState.collectAsState().value

            val currentTabTitle = if (selectedTableId != null && tablesState is TablesState.Data) {
                val selectedTable = tablesState.tables[selectedTableId]
                if (selectedTable != null && selectedTable.selectedTab != null) {
                    tablesState.tabs[selectedTable.selectedTab]?.title ?: "No Tab"
                } else "No Tab"
            } else "No Tab"

            Box(modifier = Modifier.weight(1f), contentAlignment = Alignment.Center) {
                Text(
                    text = currentTabTitle,
                    style = MaterialTheme.typography.titleMedium,
                    textAlign = androidx.compose.ui.text.style.TextAlign.Center,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }
        }
    }

    val snackbarHostState = remember { SnackbarHostState() }

    // Track tabs button window rect so we can convert local pointer positions to window coords
    var tabsButtonWindowRect by remember { mutableStateOf<Rect?>(null) }

    val tabsButtonModifier = Modifier.pointerInput(Unit) {
        detectDragGestures(
            onDragStart = { _ ->
                isSheetManuallyOpened = true
                isDragging = true
                revealSheetState.show(tween(0))
            },
                onDrag = drag@{ change, _ ->
                // Convert local pointer position to window coords using captured button rect
                val localPos = change.position
                val buttonRect = tabsButtonWindowRect
                if (buttonRect == null) return@drag;
                
                val windowPos =
                    Offset(buttonRect.left + localPos.x, buttonRect.top + localPos.y)
                lastDragWindowPos = windowPos
                val tabHit =
                    tabItemLayouts.entries.find { (_, rect) -> rect.contains(windowPos) }
                highlightedTab = tabHit?.key

                // Hover-switch tables: if pointer over a table item, switch immediately
                // Prefer the most recently captured rects: order entries by rect area
                val tableHit = tableItemLayouts.entries
                    .sortedByDescending { (_, rect) -> rect.width * rect.height }
                    .find { (_, rect) -> rect.contains(windowPos) }

                if (tableHit != null) {
                    val tableId = tableHit.key
                    if (tableId != highlightedTable) {
                        highlightedTable = tableId
                        vm.selectTable(tableId)
                        // clear tab layouts on switch
                        tabItemLayouts = mapOf()
                    }
                }

                // Update hover controllers only with valid rects to avoid jitter from empty captures
                val addRect = addButtonWindowRect
                val addTableRect = addTableButtonWindowRect
                // debug: drag update windowPos=$windowPos highlightedTab=$highlightedTab highlightedTable=$highlightedTable tabRects=${tabItemLayouts.size} tableRects=${tableItemLayouts.size} addRect=$addRect addTableRect=$addTableRect
                if (addRect != null && addRect.width > 0f && addRect.height > 0f) {
                    addTabController.targetRect = addRect
                }
                addTabController.update(windowPos)
                
                if (addTableRect != null && addTableRect.width > 0f && addTableRect.height > 0f) {
                    addTableController.targetRect = addTableRect
                }
                addTableController.update(windowPos)
            },
            onDragEnd = {
                scope.launch {
                    // debug: drag end
                    // If we released over add button and it was ready, create a new tab
                if (addTabReadyState.value && lastDragWindowPos != null) {
                        val sel = vm.getSelectedTable()
                        if (sel != null) {
                            val res = vm.createNewTab(sel.id)
                            if (res.isSuccess) {
                                val newTab = res.getOrNull()
                                if (newTab != null) {
                                    vm.selectTab(newTab.id)
                                }
                            }
                        }
                    } else if (addTableReadyState.value && lastDragWindowPos != null) {
                        // Create new table on drag release
                        vm.viewModelScope.launch {
                            vm.createNewTable()
                        }
                    } else {
                        pendingTabSelection = highlightedTab
                    }

                    isSheetManuallyOpened = false
                    isDragging = false
                    // reset
                    addTabController.cancel()
                    addTableController.cancel()
                    lastDragWindowPos = null
                }
            },
            onDragCancel = {
                scope.launch {
                    isSheetManuallyOpened = false
                    isDragging = false
                }
            }
        )
    }

    val featuresButtonModifier = Modifier

    Scaffold(
        modifier = modifier,
        bottomBar = {
            val tablesStateForNav = vm.tablesState.collectAsState().value
            val selectedTableIdForNav = vm.selectedTableId.collectAsState().value
            val tabCountForNav = if (tablesStateForNav is TablesState.Data && selectedTableIdForNav != null) {
                tablesStateForNav.tables[selectedTableIdForNav]?.tabs?.size ?: 0
            } else 0

            DaybookBottomNavigationBar(
                onTabPressed = {
                    isSheetManuallyOpened = !isSheetManuallyOpened
                },
                onFeaturesPressed = { showFeaturesMenu = !showFeaturesMenu },
                centerContent = {
                    Row {
                        // original center content
                        centerNavBarContent()
                    }
                },
                tabsButtonModifier = tabsButtonModifier,
                onTabsButtonLayout = { rect -> tabsButtonWindowRect = rect },
                featuresButtonModifier = featuresButtonModifier,
                tabCount = tabCountForNav
            )
        },
        snackbarHost = { SnackbarHost(snackbarHostState) }
    ) { scaffoldPadding ->
        RevealBottomSheetScaffold(
            sheetState = revealSheetState,
            // For the TABS sheet we only want hidden and expanded anchors (no 0.5 partial)
            sheetAnchors = if (sheetContent == SheetContent.TABS) listOf(0f, 1f) else null,
            sheetDragHandle = null,
            sheetHeader = { headerModifier: Modifier ->
                // Place table title / header when in TABS sheet
                if (sheetContent == SheetContent.TABS) {
                    val tablesState = vm.tablesState.collectAsState().value
                    val selectedTableId = vm.selectedTableId.collectAsState().value
                    val table = if (tablesState is TablesState.Data && selectedTableId != null) {
                        tablesState.tables[selectedTableId]
                    } else null

                    if (table != null) {
                        Surface(modifier = headerModifier.fillMaxWidth(), color = Color.Transparent) {
                            Column {
                                // handle drawn in header
                                Box(modifier = Modifier
                                    .fillMaxWidth()
                                    .padding(top = 8.dp, bottom = 4.dp), contentAlignment = Alignment.Center) {
                                    Box(modifier = Modifier
                                        .height(4.dp)
                                        .width(36.dp)
                                        .background(MaterialTheme.colorScheme.onSurface.copy(alpha = 0.12f), shape = RoundedCornerShape(2.dp))
                                    )
                                }
                                Row(modifier = Modifier.padding(16.dp), verticalAlignment = Alignment.CenterVertically) {
                                    Text(text = table.title ?: "", style = MaterialTheme.typography.titleMedium)
                                }
                            }
                        }
                    }
                }
            },
            topBar = { TopAppBar(title = { Text("Daybook") }) },
            sheetContent = {
                SheetContentHost(
                    sheetContent = sheetContent,
                    onTabSelected = {
                        // When the user selects a tab from the sheet, route it via the vm
                        vm.selectTab(it.id)
                        isSheetManuallyOpened = false
                    },
                    onTableSelected = { table ->
                        vm.selectTable(table.id)
                        sheetContent = SheetContent.TABS // Switch back to tabs
                    },
                    onDismiss = {
                        isSheetManuallyOpened = false
                    },
                onTabLayout = { tabId, rect ->
                        tabItemLayouts = tabItemLayouts + (tabId to rect)
                    },
                    onTableLayout = { tableId, rect ->
                        tableItemLayouts = tableItemLayouts + (tableId to rect)
                    },
                    onAddTableLayout = { rect ->
                        if (rect.width > 0f && rect.height > 0f) {
                            addTableButtonWindowRect = rect
                            // debug: addTableButtonWindowRect set to $rect
                        } else {
                            // debug: addTableButtonWindowRect ignored empty rect $rect
                        }
                    },
                    addTableController = addTableController,
                    highlightedTab = highlightedTab,
                    highlightedTable = highlightedTable
                )
            },
            sheetPeekHeight = 0.dp,
            modifier = Modifier.padding(scaffoldPadding)
        ) { contentPadding ->
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(contentPadding)
            ) {
                Routes(
                    modifier = Modifier.fillMaxSize(),
                    extraAction = extraAction,
                    navController = navController
                )

                // Instant visual sheet overlay during drag (no scaffold animation)
                // overlay removed; rely on scaffold expand/hide behavior

                if (showFeaturesMenu) {
                    FeaturesFAB(
                        onDismiss = { showFeaturesMenu = false }
                    )
                }
            }
        }

// Handle pending tab selection applied after drag end
        LaunchedEffect(pendingTabSelection) {
            val pending = pendingTabSelection
            if (pending != null) {
                vm.selectTab(pending)
                pendingTabSelection = null
                highlightedTab = null
            }
        }
    }
}

@Composable
fun DaybookBottomNavigationBar(
    onTabPressed: () -> Unit,
    onFeaturesPressed: () -> Unit,
    modifier: Modifier = Modifier,
    centerContent: @Composable RowScope.() -> Unit,
    tabsButtonModifier: Modifier = Modifier,
    featuresButtonModifier: Modifier = Modifier,
    extraLeftContent: (@Composable () -> Unit)? = null,
    onTabsButtonLayout: ((Rect) -> Unit)? = null,
    tabCount: Int = 0,
) {
    /*
     * New simplified, extensible bottom bar implementation:
     * - left area: toggle / drag-enabled tab switcher
     * - center area: dynamic content provided by caller (or default)
     * - right area: toggle features / FABs
     */

    BottomAppBar(
        modifier = modifier,
        contentPadding = PaddingValues(4.dp)
    ) {
        // Left (tab) button area (tab button + optional extra)
        Box(modifier = Modifier.weight(0.15f)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                // Small tab count badge instead of icon
                Box(
                    modifier = tabsButtonModifier.then(
                        if (onTabsButtonLayout != null) Modifier.onGloballyPositioned {
                            onTabsButtonLayout(it.boundsInWindow())
                        } else Modifier
                    ).clickable(onClick = onTabPressed),
                    contentAlignment = Alignment.Center
                ) {
                Box(modifier = Modifier
                    .sizeIn(minWidth = 32.dp, minHeight = 24.dp)
                    .padding(4.dp)
                    .border(BorderStroke(2.dp, MaterialTheme.colorScheme.onSurface.copy(alpha = 0.12f)), shape = RoundedCornerShape(4.dp)),
                    contentAlignment = Alignment.Center) {
                    Text(text = tabCount.toString(), style = MaterialTheme.typography.bodySmall, modifier = Modifier.padding(horizontal = 6.dp, vertical = 4.dp))
                }
                }

                // optional extra content supplied by caller (e.g., a drag-detecting helper)
                if (extraLeftContent != null) {
                    extraLeftContent()
                }
            }
        }

        // Center dynamic area (expandable)
        Box(modifier = Modifier.weight(0.7f), contentAlignment = Alignment.Center) {
            // centerContent is a RowScope receiver; call it inside a Row so callers
            // can use RowScope.weight if they need to. We'll add a default slot for
            // the add-new-tab action when the sheet is closed.
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.Center) {
                centerContent()
            }
        }

        // Right (features) button area
        Box(modifier = Modifier.weight(0.15f), contentAlignment = Alignment.CenterEnd) {
                IconButton(
                    onClick = onFeaturesPressed,
                    modifier = featuresButtonModifier
            ) {
                Text("⚙️", fontSize = 16.sp)
            }
        }
    }
}


enum class SheetContent { TABS }

@Composable
fun SheetContentHost(
    sheetContent: SheetContent,
    onTabSelected: (Tab) -> Unit,
    onTableSelected: (Table) -> Unit,
    onDismiss: () -> Unit,
    modifier: Modifier = Modifier,
    onTabLayout: (tabId: Uuid, rect: Rect) -> Unit,
    onTableLayout: (tableId: Uuid, rect: Rect) -> Unit,
    onAddTableLayout: (rect: Rect) -> Unit,
    addTableController: HoverHoldControllerViewModel,
    highlightedTab: Uuid?,
    highlightedTable: Uuid?
) {
    // Action buttons: allow quick creation of tabs/tables from the sheet
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val addTableReadyState = addTableController.ready.collectAsState()
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value
    Column(
        modifier = modifier
            .fillMaxSize()
            .padding(top = 16.dp)
    ) {
        // debug: SheetContentHost mounted selectedTableId=$selectedTableId tableCount=${tablesState.let { if (it is TablesState.Data) it.tables.size else 0 }} tabCount=${tablesState.let { if (it is TablesState.Data) it.tabs.size else 0 }}
        // Header
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp)
                .padding(bottom = 16.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Show current table title as sheet title
            Text(
                text = if (selectedTableId != null && tablesState is TablesState.Data) {
                    tablesState.tables[selectedTableId]?.title ?: "Select Tab"
                } else "Select Tab",
                style = MaterialTheme.typography.headlineSmall
            )

            // header: no action buttons here; add-tab moved to navbar center

            Button(onClick = onDismiss) {
                Text("Close")
            }
        }

        Spacer(Modifier.weight(1f))

        // Content
        when (sheetContent) {
            SheetContent.TABS -> {
                // Show tabs list plus a NavigationRail for table switching
                Row(modifier = Modifier.fillMaxWidth()) {
                    // NavigationRail-based table switcher on the LEFT of the sheet
                    NavigationRail(modifier = Modifier.width(80.dp)) {
                        FloatingActionButton(
                            onClick = {
                                vm.viewModelScope.launch {
                                    vm.createNewTable()
                                }
                            },
                            modifier = Modifier
                                .size(48.dp)
                                .onGloballyPositioned { onAddTableLayout(it.boundsInWindow()) },

                            containerColor = if (addTableReadyState.value) MaterialTheme.colorScheme.secondary else MaterialTheme.colorScheme.primary
                        ) {
                            if (addTableReadyState.value) Text("✓") else Text("+")
                        }
                        // push items to the bottom
                        Spacer(Modifier.weight(1f))
                        if (tablesState is TablesState.Data) {
                            // Render reversed so items start from bottom
                            tablesState.tablesList.reversed().forEach { table ->
                                NavigationRailItem(
                                    modifier = Modifier.onGloballyPositioned {
                                        onTableLayout(table.id, it.boundsInWindow())
                                    },
                                    selected = (selectedTableId == table.id) || (highlightedTable == table.id),
                                    onClick = { onTableSelected(table) },
                                    icon = {
                                        Text("📁")
                                    }
                                )
                            }
                        } else {
                            CircularProgressIndicator(modifier = Modifier.size(24.dp))
                        }
                    }

                    TabSelectionList(
                        onTabSelected = onTabSelected,
                        modifier = Modifier.weight(1f).padding(start = 16.dp, end = 8.dp),
                        onItemLayout = onTabLayout,
                        highlightedTab = highlightedTab
                    )
                }
            }
        }
    }
}

@Composable
fun TabSelectionList(
    onTabSelected: (Tab) -> Unit,
    modifier: Modifier = Modifier,
    onItemLayout: (tabId: Uuid, rect: Rect) -> Unit,
    highlightedTab: Uuid?
) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    // Ensure a table is selected when data becomes available
    LaunchedEffect(tablesState) {
        if (tablesState is TablesState.Data) {
            val sel = vm.getSelectedTable()
            if (sel != null) vm.selectTable(sel.id)
        }
    }

    val tabsForSelectedTable = if (selectedTableId != null && tablesState is TablesState.Data) {
        val selectedTable = tablesState.tables[selectedTableId]
        selectedTable?.tabs?.mapNotNull { tabId -> tablesState.tabs[tabId] } ?: emptyList()
    } else emptyList()

    // Fill available height and render tabs starting from the bottom
    Column(
        modifier = modifier.fillMaxHeight().verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(4.dp, Alignment.Bottom)
    ) {
        if (tabsForSelectedTable.isEmpty()) {
            Text("No tabs in this table.", modifier = Modifier.padding(16.dp))
        } else {
            // Render tabs reversed so the last tab appears at the bottom
            tabsForSelectedTable.reversed().forEach { tab ->
                val isHighlighted = tab.id == highlightedTab
                NavigationDrawerItem(
                    selected = isHighlighted,
                    onClick = { onTabSelected(tab) },
                    icon = { Text("📄") },
                    label = { Text(tab.title) },
                    modifier = Modifier
                        .fillMaxWidth()
                        .onGloballyPositioned {
                            onItemLayout(tab.id, it.boundsInWindow())
                        }
                )
            }
        }
    }
}

@Composable
fun FeaturesFAB(
    onDismiss: () -> Unit,
    modifier: Modifier = Modifier
) {
    val animationProgress by animateFloatAsState(
        targetValue = 1f,
        animationSpec = tween(durationMillis = 300),
        label = "features_menu_animation"
    )

    Box(
        modifier = modifier.fillMaxSize(),
        contentAlignment = Alignment.BottomEnd
    ) {
        // Background overlay with click handling
                Box(
                    modifier = Modifier
                        .fillMaxSize()
                        .background(Color.Black.copy(alpha = 0.3f * animationProgress))
                        .clickable { onDismiss() }
                )

        // Floating action buttons
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            // Add Table FAB
            FloatingActionButton(
                onClick = {
                    // TODO: Handle add table
                    onDismiss()
                },
                modifier = Modifier.size(56.dp)
            ) {
                Text("📁", fontSize = 20.sp)
            }

            // Add Tab FAB
            FloatingActionButton(
                onClick = {
                    // TODO: Handle add tab
                    onDismiss()
                },
                modifier = Modifier.size(56.dp)
            ) {
                Text("📄", fontSize = 20.sp)
            }

            // Settings FAB
            FloatingActionButton(
                onClick = {
                    // TODO: Handle settings
                    onDismiss()
                },
                modifier = Modifier.size(56.dp)
            ) {
                Text("⚙️", fontSize = 20.sp)
            }
        }
    }
}
