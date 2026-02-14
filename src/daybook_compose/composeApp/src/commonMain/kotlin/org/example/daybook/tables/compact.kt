@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import org.example.daybook.DaybookContentType
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.sizeIn
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.Folder
import androidx.compose.material.icons.filled.Menu
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.BottomAppBar
import androidx.compose.material3.Button
import androidx.compose.material3.DismissibleDrawerSheet
import androidx.compose.material3.DismissibleNavigationDrawer
import androidx.compose.material3.DrawerValue
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.ScrollableTabRow
import androidx.compose.material3.rememberDrawerState
import androidx.compose.material3.SecondaryScrollableTabRow
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.Tab
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.drawscope.rotate
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.ChromeState
import org.example.daybook.ChromeStateTopAppBar
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalChromeStateManager
import org.example.daybook.LocalContainer
import org.example.daybook.Routes
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.uniffi.core.Tab
import org.example.daybook.uniffi.core.Table
// TODO: Update compact.kt to use new LayoutWindowConfig structure
// import org.example.daybook.uniffi.core.TableViewMode
import org.example.daybook.uniffi.core.Uuid

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
    private val delayMs = 250L
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
                job =
                    viewModelScope.launch {
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
                leaveJob =
                    viewModelScope.launch {
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

// Descriptor for a toolbar feature button
data class FeatureItem(
    val key: String,
    val icon: String,
    val label: String,
    val onActivate: suspend () -> Unit
)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun CompactLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    contentType: DaybookContentType
) {
    var showFeaturesMenu by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    val revealSheetState = rememberRevealBottomSheetState(initiallyVisible = false)
    var sheetContent by remember { mutableStateOf(SheetContent.MENU) }
    val leftDrawerState = rememberDrawerState(initialValue = DrawerValue.Closed)

    // Config ViewModel
    val configRepo = LocalContainer.current.configRepo
    val configVm = viewModel { ConfigViewModel(configRepo) }

    // TODO: Update to use new LayoutWindowConfig structure
    // val tableViewModeState = configVm.tableViewModeCompact.collectAsState()
    // val tableViewMode = tableViewModeState.value ?: org.example.daybook.uniffi.core.TableViewMode.HIDDEN
    val tableViewMode = "HIDDEN" // Placeholder

    // Error handling
    val snackbarHostState = remember { SnackbarHostState() }
    val configError = configVm.error.collectAsState()
    LaunchedEffect(configError.value) {
        configError.value?.let { error ->
            snackbarHostState.showSnackbar(error.message)
            configVm.clearError()
        }
    }

    var tabItemLayouts by remember { mutableStateOf(mapOf<Uuid, Rect>()) }
    var tableItemLayouts by remember { mutableStateOf(mapOf<Uuid, Rect>()) }
    var menuItemLayouts by remember { mutableStateOf(mapOf<String, Rect>()) }
    var highlightedTable by remember { mutableStateOf<Uuid?>(null) }
    var highlightedTab by remember { mutableStateOf<Uuid?>(null) }
    var highlightedMenuItem by remember { mutableStateOf<String?>(null) }
    var isDragging by remember { mutableStateOf(false) }
    var isLeftDrawerDragging by remember { mutableStateOf(false) }
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
    // feature button layout rects (populated when toolbar renders)
    var featureButtonLayouts by remember { mutableStateOf(mapOf<String, Rect>()) }

    // Use separate feature lists: navBar features for center rollout, menu features for menu sheet
    val navBarFeatures = rememberNavBarFeatures(navController)
    val baseMenuFeatures = rememberMenuFeatures(navController)

    // Get chrome state to check for prominent buttons
    val chromeStateManager = LocalChromeStateManager.current
    val chromeState by chromeStateManager.currentState.collectAsState()
    val prominentButtons = chromeState.additionalFeatureButtons.filter { it.prominent }

    // If prominent buttons are displacing nav bar features, add displaced features to menu
    val menuFeatures =
        remember(baseMenuFeatures, navBarFeatures, prominentButtons) {
            if (prominentButtons.isNotEmpty()) {
                // Prominent buttons displace nav bar features, so add them to menu
                baseMenuFeatures + navBarFeatures
            } else {
                baseMenuFeatures
            }
        }

    // Create controllers and ready-state trackers for each navBar feature (used in center rollout)
    val navBarFeatureKeys = navBarFeatures.map { it.key }
    val navBarFeatureControllers =
        navBarFeatureKeys.map { k ->
            viewModel<HoverHoldControllerViewModel>(key = k).also {
                it.label = k
            }
        }

    // Create controllers for prominent buttons too
    val prominentButtonKeys = prominentButtons.map { it.key }
    val prominentButtonControllers =
        prominentButtonKeys.map { k ->
            viewModel<HoverHoldControllerViewModel>(key = "prominent_$k").also {
                it.label = "prominent_$k"
            }
        }

    // Combine all controllers and ready states
    val featureControllers = navBarFeatureControllers + prominentButtonControllers
    val featureReadyStates = featureControllers.map { it.ready.collectAsState() }
    var menuGestureSurfaceWindowRect by remember { mutableStateOf<Rect?>(null) }

    val menuGestureModifier =
        Modifier
            .onGloballyPositioned { menuGestureSurfaceWindowRect = it.boundsInWindow() }
            .pointerInput(Unit) {
                var menuSheetOpenedByDrag = false
                var horizontalDragDistance = 0f
                detectDragGestures(
                    onDragStart = { _ ->
                        isDragging = true
                        isLeftDrawerDragging = true
                        menuSheetOpenedByDrag = false
                        horizontalDragDistance = 0f
                    },
                    onDrag = { change, dragAmount ->
                        horizontalDragDistance += dragAmount.x
                        if (!menuSheetOpenedByDrag && dragAmount.y < 0f && kotlin.math.abs(dragAmount.y) > kotlin.math.abs(dragAmount.x)) {
                            sheetContent = SheetContent.MENU
                            revealSheetState.openToContent(SheetContent.MENU, scope)
                            menuSheetOpenedByDrag = true
                        }
                        if (!menuSheetOpenedByDrag) {
                            return@detectDragGestures
                        }
                        val surfaceRect = menuGestureSurfaceWindowRect ?: return@detectDragGestures
                        val localPos = change.position
                        val windowPos = Offset(surfaceRect.left + localPos.x, surfaceRect.top + localPos.y)
                        lastDragWindowPos = windowPos

                        // Check if pointer is over a menu item
                        val menuHit = menuItemLayouts.entries.find { (_, rect) ->
                            rect.contains(windowPos)
                        }
                        highlightedMenuItem = menuHit?.key

                        // Also update controllers with their target rects for toolbar rollout
                        featureButtonLayouts.forEach { (k, r) ->
                            // Check if it's a nav bar feature
                            val navIdx = navBarFeatureKeys.indexOf(k)
                            if (navIdx >= 0) {
                                navBarFeatureControllers[navIdx].targetRect = r
                            } else {
                                // Check if it's a prominent button
                                val prominentIdx = prominentButtonKeys.indexOf(k)
                                if (prominentIdx >= 0) {
                                    prominentButtonControllers[prominentIdx].targetRect = r
                                }
                            }
                        }
                        navBarFeatureControllers.forEach { it.update(windowPos) }
                        prominentButtonControllers.forEach { it.update(windowPos) }
                    },
                    onDragEnd = {
                        scope.launch {
                            isLeftDrawerDragging = false
                            if (!menuSheetOpenedByDrag) {
                                isDragging = false
                                val triggerThresholdPx = 56f
                                when {
                                    horizontalDragDistance >= triggerThresholdPx &&
                                        leftDrawerState.currentValue == DrawerValue.Closed -> {
                                        leftDrawerState.open()
                                    }
                                    horizontalDragDistance <= -triggerThresholdPx &&
                                        leftDrawerState.currentValue == DrawerValue.Open -> {
                                        leftDrawerState.close()
                                    }
                                }
                                return@launch
                            }
                            var shouldClose = false

                            // If released over a menu item, activate it and close
                            if (highlightedMenuItem != null && lastDragWindowPos != null) {
                                val menuItemKey = highlightedMenuItem
                                val feature = menuFeatures.find { it.key == menuItemKey }
                                if (feature != null) {
                                    feature.onActivate()
                                    shouldClose = true
                                }
                            } else {
                                // Otherwise, activate any ready feature from toolbar rollout
                                // Check nav bar features first
                                navBarFeatureControllers.forEachIndexed { idx, ctrl ->
                                    if (ctrl.ready.value) {
                                        val feature = navBarFeatures.getOrNull(idx)
                                        if (feature != null) {
                                            scope.launch { feature.onActivate() }
                                            shouldClose = true
                                        }
                                    }
                                    ctrl.cancel()
                                }
                                // Check prominent buttons
                                prominentButtonControllers.forEachIndexed { idx, ctrl ->
                                    if (ctrl.ready.value) {
                                        val button = prominentButtons.getOrNull(idx)
                                        if (button != null && button.enabled) {
                                            scope.launch { button.onClick() }
                                            shouldClose = true
                                        }
                                    }
                                    ctrl.cancel()
                                }
                            }

                            // Clear highlights
                            highlightedMenuItem = null
                            lastDragWindowPos = null
                            isDragging = false
                            menuSheetOpenedByDrag = false

                            // Cancel all controllers
                            navBarFeatureControllers.forEach { it.cancel() }
                            prominentButtonControllers.forEach { it.cancel() }
                            showFeaturesMenu = false

                            // Close sheet if item was activated, otherwise settle to nearest anchor
                            if (shouldClose) {
                                revealSheetState.hide()
                            } else {
                                revealSheetState.settle(0f)
                            }
                        }
                    },
                    onDragCancel = {
                        scope.launch {
                            isLeftDrawerDragging = false
                            navBarFeatureControllers.forEach { it.cancel() }
                            prominentButtonControllers.forEach { it.cancel() }
                            highlightedMenuItem = null
                            lastDragWindowPos = null
                            isDragging = false
                            menuSheetOpenedByDrag = false
                            revealSheetState.hide()
                            showFeaturesMenu = false
                        }
                    }
                )
            }

    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }

    // Clear cached tab layout rects whenever the selected table or sheet content changes
    // FIXME:
    LaunchedEffect(vm.tablesState.collectAsState(), sheetContent) {
        tabItemLayouts = mapOf()
        tableItemLayouts = mapOf()
        menuItemLayouts = mapOf()
        highlightedTab = null
        highlightedTable = null
        highlightedMenuItem = null
    }

    // Ensure sheet snaps to correct anchor when content changes while sheet is open
    LaunchedEffect(sheetContent) {
        if (revealSheetState.isVisible) {
            revealSheetState.ensureValidAnchor(sheetContent, scope)
        }
    }

    val centerNavBarContent: @Composable RowScope.() -> Unit = {
        CenterNavBarContent(
            navController = navController,
            isMenuOpen = revealSheetState.isVisible && sheetContent == SheetContent.MENU,
            showFeaturesMenu = showFeaturesMenu,
            featureReadyStates = featureReadyStates,
            features = navBarFeatures,
            featureButtonLayouts = featureButtonLayouts,
            lastDragWindowPos = lastDragWindowPos,
            onFeatureButtonLayout = { key, rect ->
                featureButtonLayouts = featureButtonLayouts + (key to rect)
            },
            onFeatureActivate = { feature ->
                showFeaturesMenu = false
                scope.launch {
                    feature.onActivate()
                    // Close the sheet if it's open and showing the menu
                    if (revealSheetState.isVisible && sheetContent == SheetContent.MENU) {
                        revealSheetState.hide()
                    }
                }
            }
        )
    }

    // removed duplicate snackbarHostState (declared above)

    // (Duplicate controllers/handler removed; single definitions exist above near features list.)

    DismissibleNavigationDrawer(
        gesturesEnabled = false,
        drawerState = leftDrawerState,
        drawerContent = {
            LeftDrawer(
                onDismiss = {
                    scope.launch {
                        leftDrawerState.close()
                    }
                },
                onAddTab = {
                    val selectedTable = vm.getSelectedTable()
                    if (selectedTable != null) {
                        val createTabResult = vm.createNewTab(selectedTable.id)
                        if (createTabResult.isSuccess) {
                            createTabResult.getOrNull()?.let { newTabId ->
                                vm.selectTab(newTabId)
                            }
                        }
                    }
                },
                onTabSelected = { selectedTab ->
                    vm.selectTab(selectedTab.id)
                    scope.launch {
                        leftDrawerState.close()
                    }
                }
            )
        }
    ) {
        Scaffold(
            modifier = modifier,
            bottomBar = {
                DaybookBottomNavigationBar(
                    centerContent = {
                        centerNavBarContent()
                    },
                    // showLeftDrawerHint = leftDrawerState.currentValue == DrawerValue.Closed && !isLeftDrawerDragging,
                    showLeftDrawerHint = true,
                    bottomBarModifier = menuGestureModifier,
                )
            },
            snackbarHost = { SnackbarHost(snackbarHostState) }
        ) { scaffoldPadding ->
            Box(
                modifier =
                    Modifier
                        .fillMaxSize()
                        .padding(scaffoldPadding)
            ) {
                RevealBottomSheetScaffold(
                    sheetState = revealSheetState,
                    // For TABS sheet: hidden and expanded anchors. For MENU sheet: hidden, 2/3, and expanded anchors
                    sheetAnchors = SheetConfig.getAnchors(sheetContent),
                    sheetDragHandle = null,
                    sheetHeader = { headerModifier: Modifier ->
                        when (sheetContent) {
                    SheetContent.TABS -> {
                        // Place table title / header when in TABS sheet
                        val tablesState = vm.tablesState.collectAsState().value
                        val selectedTableId = vm.selectedTableId.collectAsState().value
                        val table =
                            if (tablesState is TablesState.Data && selectedTableId != null) {
                                tablesState.tables[selectedTableId]
                            } else {
                                null
                            }

                        if (table != null) {
                            Surface(
                                modifier = headerModifier.fillMaxWidth(),
                                color = Color.Transparent
                            ) {
                                Column {
                                    // handle drawn in header
                                    Box(
                                        modifier =
                                            Modifier
                                                .fillMaxWidth()
                                                .padding(top = 8.dp, bottom = 4.dp),
                                        contentAlignment = Alignment.Center
                                    ) {
                                        Box(
                                            modifier =
                                                Modifier
                                                    .height(4.dp)
                                                    .width(36.dp)
                                                    .background(
                                                        MaterialTheme.colorScheme.onSurface.copy(
                                                            alpha = 0.12f
                                                        ),
                                                        shape = RoundedCornerShape(2.dp)
                                                    )
                                        )
                                    }
                                    Row(
                                        modifier =
                                            Modifier
                                                .fillMaxWidth()
                                                .padding(16.dp),
                                        horizontalArrangement = Arrangement.SpaceBetween,
                                        verticalAlignment = Alignment.CenterVertically
                                    ) {
                                        // Toggle button for view mode
                                        IconButton(
                                            onClick = {
                                                // TODO: Update to use new LayoutWindowConfig structure
                                                // configVm.setTableViewModeCompact(
                                                //     when (tableViewMode) {
                                                //         TableViewMode.HIDDEN -> TableViewMode.RAIL
                                                //         TableViewMode.RAIL -> TableViewMode.TAB_ROW
                                                //         TableViewMode.TAB_ROW -> TableViewMode.HIDDEN
                                                //     }
                                                // )
                                            }
                                        ) {
                                            Icon(
                                                imageVector = Icons.Default.Menu,
                                                contentDescription = "Toggle table view mode"
                                            )
                                        }

                                        Text(
                                            text = table.title,
                                            style = MaterialTheme.typography.titleMedium
                                        )

                                        Spacer(Modifier.width(48.dp)) // Balance the toggle button on the left
                                    }
                                }
                            }
                        }
                    }

                            SheetContent.MENU -> {
                                // Header for menu sheet
                                Surface(
                                    modifier = headerModifier.fillMaxWidth(),
                                    color = Color.Transparent
                                ) {
                                    Column(
                                        modifier =
                                            Modifier
                                                .fillMaxWidth()
                                                .background(MaterialTheme.colorScheme.surfaceContainerLow)
                                    ) {
                                        // handle drawn in header
                                        Box(
                                            modifier =
                                                Modifier
                                                    .fillMaxWidth()
                                                    .padding(top = 8.dp, bottom = 4.dp),
                                            contentAlignment = Alignment.Center
                                        ) {
                                            Box(
                                                modifier =
                                                    Modifier
                                                        .height(4.dp)
                                                        .width(36.dp)
                                                        .background(
                                                            MaterialTheme.colorScheme.onSurface.copy(
                                                                alpha = 0.12f
                                                            ),
                                                            shape = RoundedCornerShape(2.dp)
                                                        )
                                            )
                                        }
                                        Row(
                                            modifier = Modifier.padding(16.dp),
                                            verticalAlignment = Alignment.CenterVertically
                                        ) {
                                            Text(
                                                text = "Menu",
                                                style = MaterialTheme.typography.titleMedium
                                            )
                                        }
                                    }
                                }
                            }
                        }
                    },
                    topBar = {
                // Get chrome state manager and observe the current state (from the current screen)
                val chromeStateManager = LocalChromeStateManager.current
                val screenChromeState by chromeStateManager.currentState.collectAsState()

                // Merge layout-specific chrome with screen chrome
                // Check if screen chrome is empty (no title, no navigation icon, no actions, and showTopBar is false)
                val isScreenChromeEmpty =
                    screenChromeState.title == null &&
                        screenChromeState.navigationIcon == null &&
                        screenChromeState.actions == null &&
                        !screenChromeState.showTopBar
                // Compact view doesn't show features menu in TopAppBar (it's in the bottom app bar)
                val mergedChromeState =
                    ChromeState(
                        title = screenChromeState.title ?: "Daybook",
                        navigationIcon = screenChromeState.navigationIcon,
                        onBack = screenChromeState.onBack,
                        actions = {
                            // Only screen actions in compact view (no layout actions since menu is in bottom bar)
                            screenChromeState.actions?.invoke()
                        },
                        showTopBar = if (isScreenChromeEmpty) true else screenChromeState.showTopBar
                    )

                ChromeStateTopAppBar(mergedChromeState)
                    },
                    sheetContent = {
                SheetContentHost(
                    sheetContent = sheetContent,
                    onTabSelected = {
                        // When the user selects a tab from the sheet, route it via the vm
                        vm.selectTab(it.id)
                        revealSheetState.hide()
                    },
                    onTableSelected = { table ->
                        vm.selectTable(table.id)
                    },
                    onDismiss = {
                        revealSheetState.hide()
                    },
                    onFeatureActivate = {
                        showFeaturesMenu = false
                        scope.launch {
                            revealSheetState.hide()
                        }
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
                    highlightedTable = highlightedTable,
                    features = menuFeatures,
                    onMenuItemLayout = { key, rect ->
                        menuItemLayouts = menuItemLayouts + (key to rect)
                    },
                    highlightedMenuItem = highlightedMenuItem,
                    tableViewMode = tableViewMode
                )
                    },
                    sheetPeekHeight = 0.dp,
                    modifier = Modifier.matchParentSize()
                ) { contentPadding ->
                    Box(
                        modifier =
                            Modifier
                                .fillMaxSize()
                                .padding(contentPadding)
                    ) {
                        Row(modifier = Modifier.fillMaxSize()) {
                            Box(modifier = Modifier.weight(1f, fill = true)) {
                                Routes(
                                    modifier = Modifier.fillMaxSize(),
                                    navController = navController,
                                    extraAction = extraAction,
                                    contentType = contentType
                                )
                            }

                            // Sidebar not shown in compact view
                        }
                    }
                }
            }
        }

    }
}
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun DaybookBottomNavigationBar(
    centerContent: @Composable RowScope.() -> Unit,
    showLeftDrawerHint: Boolean = false,
    bottomBarModifier: Modifier = Modifier,
) {
    /*
     * New simplified, extensible bottom bar implementation:
     * - left area: reserved for drag gesture driven LeftDrawer
     * - center area: dynamic content provided by caller (or default)
     * - right area: toggle features / FABs with floating action menu
     */

    // Box(modifier = bottomBarModifier.fillMaxWidth().height(70.dp)) {
    // }

    BottomAppBar(
        modifier = bottomBarModifier.fillMaxWidth().height(70.dp),
        actions = {
            Box(modifier = Modifier.fillMaxWidth()) {
                if (showLeftDrawerHint) {
                    Box(
                        modifier =
                            Modifier
                                .align(Alignment.BottomEnd)
                                .offset(x = 16.dp, y = -5.dp),
                    ) {
                        LeftDrawerEdgeHint(
                            modifier =
                                Modifier
                                    .size(width = 90.dp, height = 90.dp)
                                    .graphicsLayer(
                                        rotationZ = -90f,
                                        alpha = 0.85f
                                    )
                        )
                    }
                    Box(
                        modifier =
                            Modifier
                                .align(Alignment.CenterStart)
                                .graphicsLayer(alpha = 0.85f),
                    ) {
                        LeftDrawerEdgeHint(
                            modifier = Modifier.size(width = 90.dp, height = 90.dp)
                        )
                    }
                }
                Row(Modifier.matchParentSize()) {
                    // Center dynamic area (expandable). When featuresExpanded, render feature buttons
                    centerContent()

                }
            }
        }
    )
}

@Composable
private fun LeftDrawerEdgeHint(modifier: Modifier = Modifier) {
    val xValues = listOf(-83.1573f, -133.157f, -183.157f, -233.157f)
    val yValue = -83f
    val rectSizeValue = 400f
    val colorScheme = MaterialTheme.colorScheme
    val layerColors =
        listOf(
            colorScheme.surfaceContainerHighest,
            colorScheme.onSurfaceVariant,
            colorScheme.onSurface,
            colorScheme.surfaceVariant,
        )

    Canvas(modifier = modifier) {
        // Keep the original SVG proportions and scale by height.
        val scale = size.height / 400f
        val y = yValue * scale
        val rectSize = rectSizeValue * scale

        xValues.zip(layerColors).forEach { (xValue, layerColor) ->
            val x = xValue * scale
            rotate(degrees = 45f, pivot = Offset(x, y)) {
                drawRect(
                    color = layerColor,
                    topLeft = Offset(x, y),
                    size = Size(rectSize, rectSize)
                )
            }
        }
    }
}

@Composable
fun LeftDrawer(
    onDismiss: () -> Unit,
    onAddTab: suspend () -> Unit,
    onTabSelected: (Tab) -> Unit,
    modifier: Modifier = Modifier
) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val viewModel = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = viewModel.tablesState.collectAsState().value
    val selectedTableId = viewModel.selectedTableId.collectAsState().value
    val selectedTable =
        if (tablesState is TablesState.Data && selectedTableId != null) {
            tablesState.tables[selectedTableId]
        } else {
            null
        }

    DismissibleDrawerSheet(
        modifier = modifier.width(320.dp),
        drawerContainerColor = MaterialTheme.colorScheme.surfaceContainer
    ) {
        Text(
            text = "LeftDrawer",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(16.dp)
        )
        HorizontalDivider()
        selectedTable?.let { table ->
            Text(
                text = table.title,
                style = MaterialTheme.typography.bodyMedium,
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
            )
            HorizontalDivider()
        }
        TabSelectionList(
            onTabSelected = onTabSelected,
            modifier = Modifier.weight(1f),
            growUpward = false
        )
        NavDrawerBottomBar(onAddTab = onAddTab, onClose = onDismiss)
    }
}

@Composable
fun NavDrawerBottomBar(
    onAddTab: suspend () -> Unit,
    onClose: () -> Unit,
    modifier: Modifier = Modifier
) {
    val scope = rememberCoroutineScope()
    BottomAppBar(
        modifier = modifier.fillMaxWidth().height(70.dp),
        containerColor = MaterialTheme.colorScheme.surfaceContainerLow
    ) {
        Button(
            onClick = {
                scope.launch {
                    onAddTab()
                }
            },
            modifier = Modifier.weight(1f).padding(start = 8.dp)
        ) {
            Text("Add Tab")
        }
        OutlinedButton(
            onClick = onClose,
            modifier = Modifier.padding(horizontal = 8.dp)
        ) {
            Text("Close")
        }
    }
}

enum class SheetContent { TABS, MENU }

// Sheet configuration constants
private object SheetConfig {
    const val TABS_MAX_ANCHOR = 0.95f
    const val MENU_MAX_ANCHOR = 0.75f

    fun getAnchors(content: SheetContent): List<Float> = when (content) {
        SheetContent.TABS -> listOf(0f, TABS_MAX_ANCHOR)
        SheetContent.MENU -> listOf(0f, MENU_MAX_ANCHOR)
    }

    fun getMaxAnchor(content: SheetContent): Float = when (content) {
        SheetContent.TABS -> TABS_MAX_ANCHOR
        SheetContent.MENU -> MENU_MAX_ANCHOR
    }
}

// Helper functions for sheet state management
private fun RevealBottomSheetState.openToContent(
    content: SheetContent,
    scope: kotlinx.coroutines.CoroutineScope
) {
    scope.launch {
        showToProgress(SheetConfig.getMaxAnchor(content))
    }
}

private fun RevealBottomSheetState.switchContent(
    from: SheetContent,
    to: SheetContent,
    scope: kotlinx.coroutines.CoroutineScope
) {
    scope.launch {
        snapToProgress(SheetConfig.getMaxAnchor(to))
    }
}

private fun RevealBottomSheetState.ensureValidAnchor(
    content: SheetContent,
    scope: kotlinx.coroutines.CoroutineScope
) {
    scope.launch {
        val current = progress
        val maxAnchor = SheetConfig.getMaxAnchor(content)
        if (current != 0f && current != maxAnchor) {
            snapToProgress(maxAnchor)
        }
    }
}

private fun handleSheetToggle(
    targetContent: SheetContent,
    currentContent: SheetContent,
    sheetState: RevealBottomSheetState,
    scope: kotlinx.coroutines.CoroutineScope,
    onContentChange: (SheetContent) -> Unit
) {
    if (sheetState.isVisible && currentContent == targetContent) {
        // Same content and already open - toggle close
        scope.launch {
            sheetState.hide()
        }
    } else {
        val wasDifferentContent = currentContent != targetContent
        onContentChange(targetContent)

        if (sheetState.isVisible) {
            // Sheet is already open, switch content
            if (wasDifferentContent) {
                sheetState.switchContent(currentContent, targetContent, scope)
            }
        } else {
            // Sheet is closed, open to target content
            sheetState.openToContent(targetContent, scope)
        }
    }
}

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
    highlightedTable: Uuid?,
    features: List<FeatureItem>,
    onMenuItemLayout: (key: String, rect: Rect) -> Unit,
    highlightedMenuItem: String?,
    tableViewMode: String, // TODO: Update to use new LayoutWindowConfig structure (was TableViewMode)
    onFeatureActivate: (() -> Unit)? = null // Callback when a feature is activated from the menu
) {
    // Action buttons: allow quick creation of tabs/tables from the sheet
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val scope = rememberCoroutineScope()
    val addTableReadyState = addTableController.ready.collectAsState()
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    // Get chrome state to include non-prominent buttons in menu
    val chromeStateManager = LocalChromeStateManager.current
    val chromeState by chromeStateManager.currentState.collectAsState()
    val nonProminentButtons = chromeState.additionalFeatureButtons.filter { !it.prominent }

    // Combine menu features with non-prominent chrome buttons
    val allMenuItems =
        remember(features, nonProminentButtons) {
            features +
                nonProminentButtons.map { button ->
                    FeatureItem(
                        key = button.key,
                        icon = "", // Will use button.icon() composable instead
                        label = "", // Will use button.label() composable instead
                        onActivate = { button.onClick() }
                    )
                }
        }
    Column(
        modifier =
            modifier
                .fillMaxSize()
                .padding(top = 16.dp)
    ) {
        // debug: SheetContentHost mounted selectedTableId=$selectedTableId tableCount=${tablesState.let { if (it is TablesState.Data) it.tables.size else 0 }} tabCount=${tablesState.let { if (it is TablesState.Data) it.tabs.size else 0 }}
        // Fixed spacer to prevent content from being hidden by sheetHeader
        Spacer(Modifier.height(110.dp))

        Spacer(Modifier.weight(1f))

        // Content
        when (sheetContent) {
            SheetContent.TABS -> {
                Column(modifier = Modifier.fillMaxSize()) {
                    // Main content area: tabs list, optionally with NavigationRail for table switching
                    Row(modifier = Modifier.weight(1f).fillMaxWidth()) {
                        // NavigationRail-based table switcher on the LEFT of the sheet (only when view mode is RAIL)
                        if (tableViewMode == "RAIL") {
                            TablesRail(
                                showTitles = false,
                                growUpward = true,
                                onTableSelected = onTableSelected,
                                onTableLayout = onTableLayout,
                                highlightedTable = highlightedTable,
                                onAddTableLayout = onAddTableLayout,
                                addTableReadyState = addTableReadyState
                            )
                        }

                        TabSelectionList(
                            onTabSelected = onTabSelected,
                            modifier =
                                Modifier
                                    .weight(1f)
                                    .padding(
                                        start = if (tableViewMode == "RAIL") 16.dp else 16.dp,
                                        end = 8.dp
                                    ),
                            growUpward = true,
                            onItemLayout = onTabLayout,
                            highlightedTab = highlightedTab
                        )
                    }

                    // TabRow at the bottom for table selection with fixed Add button (only when view mode is TAB_ROW)
                    if (tableViewMode == "TAB_ROW" && tablesState is TablesState.Data) {
                        val tablesListSnapshot = tablesState.tablesList.toList()
                        val listSize = tablesListSnapshot.size

                        // Use remembered state for selected tab index to avoid race conditions
                        var selectedTabIndexState by remember {
                            mutableIntStateOf(0)
                        }

                        // Update selected tab index when selected table or list size changes
                        // Use tablesState.tables.size as a stable key instead of the snapshot
                        LaunchedEffect(tablesState.tables.size, selectedTableId) {
                            val currentList = tablesState.tablesList
                            val currentSize = currentList.size
                            if (currentSize > 0) {
                                val foundIndex = currentList.indexOfFirst {
                                    it.id == selectedTableId
                                }
                                selectedTabIndexState = foundIndex
                                    .takeIf { it in 0..<currentSize }
                                    ?.coerceIn(0, (currentSize - 1).coerceAtLeast(0))
                                    ?: 0
                            } else {
                                selectedTabIndexState = 0
                            }
                        }

                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            // Add table button on the left (fixed, doesn't scroll)
                            IconButton(
                                onClick = {
                                    vm.viewModelScope.launch {
                                        vm.createNewTable()
                                    }
                                },
                                modifier = Modifier.padding(horizontal = 8.dp)
                            ) {
                                Icon(
                                    imageVector = Icons.Default.Add,
                                    contentDescription = "Add table"
                                )
                            }

                            // ScrollableTabRow for table tabs
                            if (tablesListSnapshot.isNotEmpty()) {
                                // Ensure index is within bounds of the current snapshot
                                val maxIndex = (tablesListSnapshot.size - 1).coerceAtLeast(0)
                                SecondaryScrollableTabRow(
                                    selectedTabIndex = selectedTabIndexState.coerceIn(0, maxIndex),
                                    modifier = Modifier.weight(1f),
                                    edgePadding = 0.dp
                                ) {
                                    // Tabs for each table - use snapshot to ensure consistency
                                    tablesListSnapshot.forEachIndexed { index, table ->
                                        val tabCount = table.tabs.size
                                        Tab(
                                            selected =
                                                (selectedTableId == table.id) ||
                                                    (highlightedTable == table.id),
                                            onClick = { onTableSelected(table) },
                                            modifier =
                                                Modifier.onGloballyPositioned {
                                                    onTableLayout(table.id, it.boundsInWindow())
                                                },
                                            text = {
                                                Row(
                                                    verticalAlignment = Alignment.CenterVertically,
                                                    horizontalArrangement = Arrangement.spacedBy(
                                                        4.dp
                                                    )
                                                ) {
                                                    Text(
                                                        table.title,
                                                        maxLines = 1,
                                                        overflow = TextOverflow.Ellipsis
                                                    )
                                                    if (tabCount > 0) {
                                                        Text(
                                                            "($tabCount)",
                                                            style = MaterialTheme.typography.labelSmall
                                                        )
                                                    }
                                                }
                                            }
                                        )
                                    }
                                }
                            } else {
                                // Empty state - just show spacer to take up space
                                Spacer(Modifier.weight(1f))
                            }
                        }
                    }
                }
            }

            SheetContent.MENU -> {
                // Show list of navigation buttons from features and non-prominent chrome buttons
                Column(
                    modifier =
                        Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 16.dp)
                            .verticalScroll(rememberScrollState()),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    allMenuItems.forEach { item ->
                        val isHighlighted = item.key == highlightedMenuItem
                        // Check if this is a chrome button (has empty icon/label strings)
                        val isChromeButton = item.icon.isEmpty() && item.label.isEmpty()
                        val chromeButton =
                            if (isChromeButton) {
                                nonProminentButtons.find { it.key == item.key }
                            } else {
                                null
                            }

                        NavigationDrawerItem(
                            selected = isHighlighted,
                            onClick = {
                                scope.launch {
                                    item.onActivate()
                                    onFeatureActivate?.invoke()
                                    onDismiss()
                                }
                            },
                            icon = {
                                if (chromeButton != null) {
                                    chromeButton.icon()
                                } else {
                                    FeatureIcon(item)
                                }
                            },
                            label = {
                                if (chromeButton != null) {
                                    chromeButton.label()
                                } else {
                                    Text(item.label)
                                }
                            },
                            modifier =
                                Modifier
                                    .fillMaxWidth()
                                    .onGloballyPositioned {
                                        onMenuItemLayout(item.key, it.boundsInWindow())
                                    }
                        )
                    }
                }
            }
        }
    }
}

@Composable
fun FeaturesFAB(onDismiss: () -> Unit, modifier: Modifier = Modifier) {
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
            modifier =
                Modifier
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
                Icon(Icons.Default.Folder, contentDescription = "Add table")
            }

            // Add Tab FAB
            FloatingActionButton(
                onClick = {
                    // TODO: Handle add tab
                    onDismiss()
                },
                modifier = Modifier.size(56.dp)
            ) {
                Icon(Icons.Default.Description, contentDescription = "Add tab")
            }

            // Settings FAB
            FloatingActionButton(
                onClick = {
                    // TODO: Handle settings
                    onDismiss()
                },
                modifier = Modifier.size(56.dp)
            ) {
                Icon(Icons.Default.Settings, contentDescription = "Settings")
            }
        }
    }
}
