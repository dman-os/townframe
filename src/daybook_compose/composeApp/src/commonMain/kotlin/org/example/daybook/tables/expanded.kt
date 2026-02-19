@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.KeyboardArrowRight
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material.icons.filled.Menu
import androidx.compose.material.icons.filled.MenuOpen
import androidx.compose.material3.*
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.IconButton
import androidx.compose.material3.LargeFloatingActionButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.PermanentDrawerSheet
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.Stable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.PointerInputChange
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import androidx.navigation.compose.currentBackStackEntryAsState
import kotlinx.coroutines.launch
import org.example.daybook.AppScreens
import org.example.daybook.ChromeState
import org.example.daybook.ChromeStateTopAppBar
import org.example.daybook.ConfigViewModel
import org.example.daybook.DaybookContentType
import org.example.daybook.LocalChromeStateManager
import org.example.daybook.LocalContainer
import org.example.daybook.Routes
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.progress.ProgressList
import org.example.daybook.uniffi.core.WindowLayout
import org.example.daybook.uniffi.core.WindowLayoutOrientation as ConfigOrientation
import org.example.daybook.uniffi.core.WindowLayoutPane
import org.example.daybook.uniffi.core.WindowLayoutPaneVariant
import org.example.daybook.uniffi.core.WindowLayoutRegion
import org.example.daybook.uniffi.core.WindowLayoutRegionSize

/**
 * Constants for sidebar layout weights and sizes
 */
private object SidebarLayoutConstants {
    /** Default expanded sidebar weight (40% of available space) */
    const val DEFAULT_SIDEBAR_WEIGHT = 0.4f

    /** Collapsed/rail sidebar weight (10% of available space) */
    const val COLLAPSED_SIDEBAR_WEIGHT = 0.10f

    /** Threshold weight to determine if sidebar is expanded or collapsed */
    const val SIDEBAR_EXPANDED_THRESHOLD = 0.15f

    /** Minimum weight for any pane to prevent it from disappearing */
    const val MIN_PANE_WEIGHT = 0.10f

    /** Rail mode size in dp (icon-only navigation rail) */
    const val RAIL_SIZE_DP = 40f

    /** Maximum dp for discrete regime (transition point from rail to drawer) */
    const val DISCRETE_REGIME_MAX_DP = 235f
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ExpandedLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    contentType: DaybookContentType
) {
    var showFeaturesMenu by remember { mutableStateOf(false) }
    val navBarFeatures = rememberNavBarFeatures(navController)
    val sidebarFeatures = rememberSidebarFeatures(navController)
    val menuFeatures = rememberMenuFeatures(navController)
    val scope = rememberCoroutineScope()

    // Tables ViewModel
    val tablesRepo = LocalContainer.current.tablesRepo
    val tablesVm = viewModel { TablesViewModel(tablesRepo) }

    // Config ViewModel (for error handling)
    val configRepo = LocalContainer.current.configRepo
    val configVm = viewModel { ConfigViewModel(configRepo) }

    // Observe layout config from the selected window
    val tablesState by tablesVm.tablesState.collectAsState()
    val selectedTableId by tablesVm.selectedTableId.collectAsState()

    val layoutConfig: WindowLayout? =
        remember(tablesState, selectedTableId) {
            if (tablesState is TablesState.Data && selectedTableId != null) {
                val state = tablesState as TablesState.Data
                // Find the window that contains this table
                val windowId =
                    state.tables[selectedTableId]?.window?.let { windowPolicy ->
                        when (windowPolicy) {
                            is org.example.daybook.uniffi.core.TableWindow.Specific -> windowPolicy.id
                            is org.example.daybook.uniffi.core.TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                        }
                    }
                windowId?.let { state.windows[it]?.layout }
            } else {
                null
            }
        }

    // Error handling
    val snackbarHostState = remember { SnackbarHostState() }
    val configError = configVm.error.collectAsState()
    LaunchedEffect(configError.value) {
        configError.value?.let { error ->
            snackbarHostState.showSnackbar(error.message)
            configVm.clearError()
        }
    }

    // Get chrome state manager and observe the current state (from the current screen)
    val chromeStateManager = LocalChromeStateManager.current
    val screenChromeState by chromeStateManager.currentState.collectAsState()

    // Get non-prominent buttons for dropdown menu
    val nonProminentButtons = screenChromeState.additionalFeatureButtons.filter { !it.prominent }
    val allMenuFeatures =
        remember(menuFeatures, nonProminentButtons) {
            menuFeatures.withAdditionalFeatureButtons(nonProminentButtons)
        }

    // Merge layout-specific chrome with screen chrome
    // Check if screen chrome is empty (no title, no navigation icon, no actions, and showTopBar is false)
    val isScreenChromeEmpty =
        screenChromeState.title == null &&
            screenChromeState.navigationIcon == null &&
            screenChromeState.actions == null &&
            !screenChromeState.showTopBar
    val mergedChromeState =
        ChromeState(
            title = screenChromeState.title ?: "Daybook",
            navigationIcon =
                screenChromeState.navigationIcon ?: {
                    IconButton(onClick = {
                        // Toggle left pane: Hidden -> Visible (expanded) -> Visible (collapsed/rail) -> Hidden
                        if (layoutConfig != null && tablesState is TablesState.Data &&
                            selectedTableId != null
                        ) {
                            val state = tablesState as TablesState.Data
                            val windowId =
                                state.tables[selectedTableId]?.window?.let { windowPolicy ->
                                    when (windowPolicy) {
                                        is org.example.daybook.uniffi.core.TableWindow.Specific -> windowPolicy.id
                                        is org.example.daybook.uniffi.core.TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                                    }
                                }

                            windowId?.let { id ->
                                val window = state.windows[id]
                                if (window != null) {
                                    val currentVisible = window.layout.leftVisible
                                    val currentWeight =
                                        when (val s = window.layout.leftRegion.size) {
                                            is org.example.daybook.uniffi.core.WindowLayoutRegionSize.Weight -> s.v1
                                        }

                                    val (nextVisible, nextWeight) =
                                        when {
                                            !currentVisible -> {
                                                true to
                                                    SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
                                            }

                                            currentWeight >
                                                SidebarLayoutConstants.SIDEBAR_EXPANDED_THRESHOLD -> {
                                                true to
                                                    SidebarLayoutConstants.COLLAPSED_SIDEBAR_WEIGHT
                                            }

                                            else -> {
                                                false to
                                                    SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
                                            }
                                        }

                                    scope.launch {
                                        tablesRepo.setWindow(
                                            id,
                                            window.copy(
                                                layout =
                                                    window.layout.copy(
                                                        leftVisible = nextVisible,
                                                        leftRegion =
                                                            window.layout.leftRegion.copy(
                                                                size =
                                                                    org.example.daybook.uniffi.core.WindowLayoutRegionSize
                                                                        .Weight(nextWeight)
                                                            )
                                                    )
                                            )
                                        )
                                    }
                                }
                            }
                        }
                    }) {
                        Icon(
                            imageVector = if (layoutConfig?.leftVisible ==
                                true
                            ) {
                                Icons.Default.MenuOpen
                            } else {
                                Icons.Default.Menu
                            },
                            contentDescription = "Toggle Sidebar"
                        )
                    }
                },
            onBack = screenChromeState.onBack,
            actions = {
                // Screen actions first, then layout actions
                screenChromeState.actions?.invoke()
                Box {
                    IconButton(onClick = { showFeaturesMenu = true }) {
                        Icon(
                            imageVector = Icons.Default.MoreVert,
                            contentDescription = "Open features menu"
                        )
                    }
                    DropdownMenu(
                        expanded = showFeaturesMenu,
                        onDismissRequest = { showFeaturesMenu = false }
                    ) {
                        allMenuFeatures.forEach { item ->
                            DropdownMenuItem(
                                text = {
                                    item.labelContent?.invoke() ?: Text(item.label)
                                },
                                onClick = {
                                    showFeaturesMenu = false
                                    scope.launch {
                                        if (item.enabled) {
                                            item.onActivate()
                                        }
                                    }
                                },
                                leadingIcon = {
                                    item.icon()
                                },
                            )
                        }
                    }
                }
            },
            showTopBar = if (isScreenChromeEmpty) true else screenChromeState.showTopBar
        )

    val allFeatures = rememberAllFeatures(navController)
    val captureFeature = allFeatures.find { it.key == FeatureKeys.Capture }

    Scaffold(
        modifier = modifier,
        snackbarHost = { SnackbarHost(snackbarHostState) },
        floatingActionButton = {
            val button = screenChromeState.mainFeatureActionButton
            val onClick: (suspend () -> Unit)? = when (button) {
                is org.example.daybook.MainFeatureActionButton.Button -> {
                    if (button.enabled) button.onClick else null
                }

                null -> captureFeature?.onActivate
            }
            if (onClick != null) {
                LargeFloatingActionButton(
                    onClick = { scope.launch { onClick() } },
                    shape = CircleShape,
                ) {
                    Icon(Icons.Filled.Add, "Large floating action button")
                }
            }
        },
        topBar = {
            ChromeStateTopAppBar(mergedChromeState)
        }
    ) { innerPadding ->
        if (layoutConfig != null) {
            LayoutFromConfig(
                layoutConfig = layoutConfig,
                tablesVm = tablesVm,
                navController = navController,
                extraAction = extraAction,
                modifier = Modifier.padding(innerPadding).fillMaxSize(),
                contentType = contentType
            )
        } else {
            // Loading state - show empty or default layout
            Box(modifier = Modifier.fillMaxSize()) {
                Text("Loading layout...")
            }
        }
    }
}

@Composable
fun LayoutFromConfig(
    layoutConfig: WindowLayout,
    tablesVm: TablesViewModel,
    navController: NavHostController,
    extraAction: (() -> Unit)?,
    modifier: Modifier = Modifier,
    contentType: DaybookContentType
) {
    val scope = rememberCoroutineScope()
    val tablesState by tablesVm.tablesState.collectAsState()
    val selectedTableId by tablesVm.selectedTableId.collectAsState()

    fun updateWeights(newWeights: Map<Any, Float>) {
        if (tablesState is TablesState.Data && selectedTableId != null) {
            val state = tablesState as TablesState.Data
            val windowId =
                state.tables[selectedTableId]?.window?.let { windowPolicy ->
                    when (windowPolicy) {
                        is org.example.daybook.uniffi.core.TableWindow.Specific -> windowPolicy.id
                        is org.example.daybook.uniffi.core.TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                    }
                }

            windowId?.let { id ->
                val window = state.windows[id]
                if (window != null) {
                    val currentLayout = window.layout
                    val newLayout =
                        currentLayout.copy(
                            leftRegion =
                                currentLayout.leftRegion.copy(
                                    size =
                                        WindowLayoutRegionSize.Weight(
                                            newWeights[currentLayout.leftRegion.deets.key]
                                                ?: (currentLayout.leftRegion.size as? WindowLayoutRegionSize.Weight)?.v1
                                                ?: SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
                                        )
                                ),
                            centerRegion =
                                currentLayout.centerRegion.copy(
                                    size =
                                        WindowLayoutRegionSize.Weight(
                                            newWeights[currentLayout.centerRegion.deets.key]
                                                ?: (currentLayout.centerRegion.size as? WindowLayoutRegionSize.Weight)?.v1
                                                ?: 1.0f
                                        )
                                ),
                            rightRegion =
                                currentLayout.rightRegion.copy(
                                    size =
                                        WindowLayoutRegionSize.Weight(
                                            newWeights[currentLayout.rightRegion.deets.key]
                                                ?: (currentLayout.rightRegion.size as? WindowLayoutRegionSize.Weight)?.v1
                                                ?: SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
                                        )
                                )
                        )
                    scope.launch {
                        tablesVm.tablesRepo.setWindow(id, window.copy(layout = newLayout))
                    }
                }
            }
        }
    }

    val initialWeights =
        remember(layoutConfig) {
            val weights = mutableMapOf<Any, Float>()
            weights[layoutConfig.leftRegion.deets.key] =
                (layoutConfig.leftRegion.size as? WindowLayoutRegionSize.Weight)?.v1
                    ?: SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
            weights[layoutConfig.centerRegion.deets.key] =
                (layoutConfig.centerRegion.size as? WindowLayoutRegionSize.Weight)?.v1 ?: 1.0f
            weights[layoutConfig.rightRegion.deets.key] =
                (layoutConfig.rightRegion.size as? WindowLayoutRegionSize.Weight)?.v1
                    ?: SidebarLayoutConstants.DEFAULT_SIDEBAR_WEIGHT
            weights
        }

    DockableRegion(
        orientation = Orientation.Horizontal,
        initialWeights = initialWeights,
        onWeightsChanged = { updateWeights(it) },
        modifier = modifier
    ) {
        // Left region (if visible)
        if (layoutConfig.leftVisible) {
            val leftPane = layoutConfig.leftRegion.deets
            val leftRegimes =
                if (leftPane.variant is WindowLayoutPaneVariant.Sidebar) {
                    // Sidebar: discrete 0-RAIL_SIZE_DP for rail mode, continuous above
                    listOf(
                        PaneSizeRegime.Discrete(
                            minDp = 0f,
                            maxDp = SidebarLayoutConstants.DISCRETE_REGIME_MAX_DP,
                            sizeDp = SidebarLayoutConstants.RAIL_SIZE_DP
                        ),
                        PaneSizeRegime.Continuous(minDp = SidebarLayoutConstants.RAIL_SIZE_DP)
                    )
                } else {
                    // Default: continuous
                    listOf(PaneSizeRegime.Continuous())
                }
            pane(key = leftPane.key, regimes = leftRegimes) {
                RenderLayoutPane(
                    pane = leftPane,
                    navController = navController,
                    extraAction = extraAction,
                    modifier = Modifier.fillMaxSize(),
                    contentType = contentType
                )
            }
        }

        // Center region (always visible)
        pane(key = layoutConfig.centerRegion.deets.key) {
            RenderLayoutPane(
                pane = layoutConfig.centerRegion.deets,
                navController = navController,
                extraAction = extraAction,
                modifier = Modifier.fillMaxSize(),
                contentType = contentType
            )
        }

        // Right region (if visible)
        if (layoutConfig.rightVisible) {
            pane(key = layoutConfig.rightRegion.deets.key) {
                RenderLayoutPane(
                    pane = layoutConfig.rightRegion.deets,
                    navController = navController,
                    extraAction = extraAction,
                    modifier = Modifier.fillMaxSize(),
                    contentType = contentType
                )
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SidebarContent(navController: NavHostController, modifier: Modifier = Modifier) {
    val density = LocalDensity.current
    var widthPx by remember { mutableIntStateOf(0) }
    val widthDp = with(density) { widthPx.toDp() }
    val isWide = widthDp >= 200.dp

    val sidebarFeatures = rememberSidebarFeatures(navController)
    val scope = rememberCoroutineScope()

    // Observe route changes to update selection highlight
    // Use currentBackStackEntryAsState to reactively observe route changes
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentRoute = navBackStackEntry?.destination?.route

    // Get chrome state to check for main feature action button and prominent buttons
    val chromeStateManager = LocalChromeStateManager.current
    val chromeState by chromeStateManager.currentState.collectAsState()
    val prominentButtons = chromeState.additionalFeatureButtons.filter { it.prominent }

    // Combine sidebar features with prominent chrome buttons
    val allSidebarFeatures =
        remember(sidebarFeatures, prominentButtons) {
            sidebarFeatures.withAdditionalFeatureButtons(prominentButtons)
        }

    // Map feature keys to routes for selection
    fun getRouteForFeature(feature: FeatureItem): String? = when (feature.key) {
        FeatureKeys.Home -> AppScreens.Home.name
        FeatureKeys.Tables -> AppScreens.Tables.name
        FeatureKeys.Capture -> AppScreens.Capture.name
        FeatureKeys.Drawer -> AppScreens.Drawer.name
        FeatureKeys.Settings -> AppScreens.Settings.name
        else -> null
    }

    Box(
        modifier =
            modifier.onSizeChanged {
                widthPx = it.width
            }
    ) {
        if (isWide) {
            var selectedSidebarPane by remember { mutableIntStateOf(0) }

            // Wide mode: navigation row + tabbed pane (tabs/progress)
            PermanentDrawerSheet(
                modifier =
                    Modifier
                        .widthIn(min = 240.dp)
                        .fillMaxSize()
            ) {
                Column(
                    modifier = Modifier.fillMaxSize()
                ) {
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(horizontal = 8.dp, vertical = 8.dp),
                        horizontalArrangement = Arrangement.SpaceEvenly
                    ) {
                        allSidebarFeatures.forEach { item ->
                            val featureRoute = getRouteForFeature(item)
                            val isSelected = featureRoute != null && featureRoute == currentRoute
                            NavigationRailItem(
                                selected = isSelected,
                                onClick = {
                                    scope.launch {
                                        if (item.enabled) {
                                            item.onActivate()
                                        }
                                    }
                                },
                                enabled = item.enabled,
                                icon = { item.icon() },
                                label = { item.labelContent?.invoke() ?: Text(item.label) },
                                alwaysShowLabel = false
                            )
                        }
                    }
                    HorizontalDivider()
                    TabRow(selectedTabIndex = selectedSidebarPane) {
                        Tab(
                            selected = selectedSidebarPane == 0,
                            onClick = { selectedSidebarPane = 0 },
                            text = { Text("Tabs") }
                        )
                        Tab(
                            selected = selectedSidebarPane == 1,
                            onClick = { selectedSidebarPane = 1 },
                            text = { Text("Progress") }
                        )
                    }
                    when (selectedSidebarPane) {
                        0 -> {
                            TabSelectionList(
                                onTabSelected = { /* TODO: Handle tab selection */ },
                                modifier = Modifier.weight(1f)
                            )
                        }

                        else -> {
                            ProgressList(modifier = Modifier.weight(1f).fillMaxWidth())
                        }
                    }
                }
            }
        } else {
            // Narrow mode: NavigationRail with features only
            NavigationRail(modifier = Modifier.fillMaxHeight()) {
                allSidebarFeatures.forEach { item ->
                    val featureRoute = getRouteForFeature(item)
                    val isSelected = featureRoute != null && featureRoute == currentRoute

                    NavigationRailItem(
                        selected = isSelected,
                        onClick = {
                            scope.launch {
                                item.onActivate()
                            }
                        },
                        enabled = item.enabled,
                        icon = {
                            item.icon()
                        },
                        label = null // Rail mode: icon-only, no labels
                    )
                }
            }
        }
    }
}

@Composable
fun RenderLayoutPane(
    pane: WindowLayoutPane,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    modifier: Modifier = Modifier,
    contentType: DaybookContentType
) {
    when (val variant = pane.variant) {
        is WindowLayoutPaneVariant.Sidebar -> {
            // Render sidebar UI
            SidebarContent(
                navController = navController,
                modifier = modifier
            )
        }

        is WindowLayoutPaneVariant.Routes -> {
            // Render routes
            Routes(
                extraAction = extraAction,
                navController = navController,
                modifier = modifier,
                contentType = contentType
            )
        }

        is WindowLayoutPaneVariant.Region -> {
            // Render nested region recursively
            RenderLayoutRegion(
                region = variant.v1,
                navController = navController,
                extraAction = extraAction,
                modifier = modifier,
                contentType = contentType
            )
        }
    }
}

@Composable
fun RenderLayoutRegion(
    region: WindowLayoutRegion,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null,
    modifier: Modifier = Modifier,
    contentType: DaybookContentType
) {
    val orientation =
        when (region.orientation) {
            ConfigOrientation.HORIZONTAL -> Orientation.Horizontal
            ConfigOrientation.VERTICAL -> Orientation.Vertical
        }

    val initialWeights =
        remember(region) {
            region.children.associate { child ->
                child.deets.key as Any to
                    ((child.size as? WindowLayoutRegionSize.Weight)?.v1 ?: 1.0f)
            }
        }

    DockableRegion(
        orientation = orientation,
        initialWeights = initialWeights,
        onWeightsChanged = { /* TODO: Implement persistence for nested regions */ },
        modifier = modifier
    ) {
        region.children.forEach { child ->
            val childPane = child.deets
            val childRegimes =
                if (childPane.variant is WindowLayoutPaneVariant.Sidebar) {
                    // Sidebar: discrete 0-RAIL_SIZE_DP for rail mode, continuous above
                    listOf(
                        PaneSizeRegime.Discrete(
                            minDp = 0f,
                            maxDp = SidebarLayoutConstants.DISCRETE_REGIME_MAX_DP,
                            sizeDp = SidebarLayoutConstants.RAIL_SIZE_DP
                        ),
                        PaneSizeRegime.Continuous(minDp = SidebarLayoutConstants.RAIL_SIZE_DP)
                    )
                } else {
                    // Default: continuous
                    listOf(PaneSizeRegime.Continuous())
                }
            pane(key = childPane.key, regimes = childRegimes) {
                RenderLayoutPane(
                    pane = child.deets,
                    navController = navController,
                    extraAction = extraAction,
                    modifier = Modifier.fillMaxSize(),
                    contentType = contentType
                )
            }
        }
    }
}

/**
 * Defines how a pane's size behaves during resizing.
 */
sealed class PaneSizeRegime {
    /**
     * Continuous sizing - pane can be resized to any size within the regime range.
     */
    data class Continuous(val minDp: Float = 0f, val maxDp: Float = Float.MAX_VALUE) :
        PaneSizeRegime()

    /**
     * Discrete sizing - pane has a fixed explicit size that must fall within the range.
     * Size only changes when crossing to another regime.
     */
    data class Discrete(val minDp: Float, val maxDp: Float, val sizeDp: Float) : PaneSizeRegime() {
        init {
            require(sizeDp >= minDp && sizeDp <= maxDp) {
                "Discrete sizeDp ($sizeDp) must be between minDp ($minDp) and maxDp ($maxDp)"
            }
        }
    }
}

interface DockedRegionScope {
    fun pane(
        key: Any,
        modifier: Modifier = Modifier,
        regimes: List<PaneSizeRegime> = listOf(PaneSizeRegime.Continuous()),
        content: @Composable (() -> Unit)
    )
}

interface GenericLayoutScope {
    fun Modifier.weight(weight: Float, fill: Boolean = true): Modifier
}

@Composable
fun DockableRegion(
    modifier: Modifier,
    orientation: Orientation,
    initialWeights: Map<Any, Float> = emptyMap(),
    onWeightsChanged: ((Map<Any, Float>) -> Unit)? = null,
    block: DockedRegionScope.() -> Unit
) {
    data class RegionPaneData(
        val key: Any,
        val modifier: Modifier,
        val regimes: List<PaneSizeRegime>,
        val content: @Composable (() -> Unit)
    )

    class DockedRegionScopeImpl : DockedRegionScope {
        val items = mutableListOf<RegionPaneData>()

        override fun pane(
            key: Any,
            modifier: Modifier,
            regimes: List<PaneSizeRegime>,
            content: @Composable (() -> Unit)
        ) {
            items.add(RegionPaneData(key, modifier, regimes, content))
        }
    }

    @Stable
    class State(
        private val density: androidx.compose.ui.unit.Density,
        private val paneRegimes: Map<Any, List<PaneSizeRegime>>,
        initialWeights: Map<Any, Float>,
        private val onWeightsChanged: ((Map<Any, Float>) -> Unit)? = null
    ) {
        // We map Keys to Weights.
        // This ensures if you reorder items, their size travels with them.
        private val weightMap =
            mutableStateMapOf<Any, Float>().apply {
                putAll(initialWeights)
            }

        // Track size in dp for regime detection
        private val sizeDpMap = mutableStateMapOf<Any, Float>()

        // Track virtual drag offset (cumulative delta during drag, as if continuous)
        private var dragOffsetPx: Float = 0f
        private var dragStartSizeDpA: Float? = null
        private var dragStartSizeDpB: Float? = null
        private var dragStartKeyA: Any? = null
        private var dragStartKeyB: Any? = null

        // Accessor to get weight for a specific key
        fun getWeight(key: Any): Float = weightMap[key] ?: 1f

        // Get current size in dp for a key
        private fun getSizeDp(key: Any, totalSizePx: Int, totalWeight: Float): Float {
            if (totalSizePx == 0 || totalWeight == 0f) return sizeDpMap[key] ?: 0f
            val weight = getWeight(key)
            val sizePx = (weight / totalWeight) * totalSizePx
            return with(density) { sizePx.toDp().value }
        }

        // Determine which regime a pane is currently in based on its size
        private fun getCurrentRegime(key: Any, sizeDp: Float): PaneSizeRegime? {
            val regimes = paneRegimes[key] ?: return null
            return regimes.find { regime ->
                when (regime) {
                    is PaneSizeRegime.Discrete -> sizeDp >= regime.minDp && sizeDp <= regime.maxDp
                    is PaneSizeRegime.Continuous -> sizeDp >= regime.minDp && sizeDp <= regime.maxDp
                }
            } ?: regimes.lastOrNull()
        }

        // The Logic: Syncs the incoming N items with our storage
        fun reconcile(keys: List<Any>, totalSizePx: Int) {
            val totalWeight = keys.sumOf { getWeight(it).toDouble() }.toFloat()

            keys.forEach { key ->
                if (!weightMap.containsKey(key)) {
                    weightMap[key] = 1.0f // Default new items to 1.0
                }
                // Update size in dp
                sizeDpMap[key] = getSizeDp(key, totalSizePx, totalWeight)
            }

            val currentKeySet = keys.toSet()
            val sizeIterator = sizeDpMap.iterator()
            while (sizeIterator.hasNext()) {
                if (!currentKeySet.contains(sizeIterator.next().key)) {
                    sizeIterator.remove()
                }
            }
        }

        // Start drag - track initial state of both panes
        fun startDrag(keyA: Any, keyB: Any, totalSizePx: Int) {
            val totalWeight = getTotalWeight()
            dragStartKeyA = keyA
            dragStartKeyB = keyB
            dragStartSizeDpA = getSizeDp(keyA, totalSizePx, totalWeight)
            dragStartSizeDpB = getSizeDp(keyB, totalSizePx, totalWeight)
            dragOffsetPx = 0f
        }

        // End drag - resolve final handle position based on regimes
        // Uses the virtual handle position to determine final sizes
        fun endDrag(keys: List<Any>, indexA: Int, totalSizePx: Int) {
            val keyA = keys[indexA]
            val keyB = keys[indexA + 1]

            // Calculate final virtual sizes
            val dragStartDpA = dragStartSizeDpA ?: getSizeDp(keyA, totalSizePx, getTotalWeight())
            val dragStartDpB = dragStartSizeDpB ?: getSizeDp(keyB, totalSizePx, getTotalWeight())
            val deltaDp = with(density) { dragOffsetPx.toDp().value }
            val virtualSizeDpA = dragStartDpA + deltaDp
            val virtualSizeDpB = dragStartDpB - deltaDp

            // Determine which regimes these virtual sizes fall into
            val regimeA = getCurrentRegime(keyA, virtualSizeDpA)
            val regimeB = getCurrentRegime(keyB, virtualSizeDpB)

            val totalWeight = getTotalWeight()
            val totalSizeDp = with(density) { totalSizePx.toDp().value }
            val weightA = getWeight(keyA)
            val weightB = getWeight(keyB)
            val totalCurrentWeight = weightA + weightB

            // Resolve final sizes based on regimes
            when {
                // Both discrete: larger one wins
                regimeA is PaneSizeRegime.Discrete && regimeB is PaneSizeRegime.Discrete -> {
                    val sizeA = regimeA.sizeDp
                    val sizeB = regimeB.sizeDp
                    val inRangeA =
                        virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp
                    val inRangeB =
                        virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp

                    if (inRangeA && inRangeB) {
                        // Both in discrete ranges - larger one wins
                        if (sizeA >= sizeB) {
                            val targetWeightA = (sizeA / totalSizeDp) * totalWeight
                            val newWeightA = targetWeightA.coerceAtLeast(
                                SidebarLayoutConstants.MIN_PANE_WEIGHT
                            )
                            val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                                SidebarLayoutConstants.MIN_PANE_WEIGHT
                            )
                            weightMap[keyA] = newWeightA
                            weightMap[keyB] = newWeightB
                        } else {
                            val targetWeightB = (sizeB / totalSizeDp) * totalWeight
                            val newWeightB = targetWeightB.coerceAtLeast(
                                SidebarLayoutConstants.MIN_PANE_WEIGHT
                            )
                            val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(
                                SidebarLayoutConstants.MIN_PANE_WEIGHT
                            )
                            weightMap[keyA] = newWeightA
                            weightMap[keyB] = newWeightB
                        }
                    } else {
                        // One or both outside discrete range - use virtual sizes
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA =
                            targetWeightA
                                .coerceAtLeast(
                                    SidebarLayoutConstants.MIN_PANE_WEIGHT
                                ).coerceAtMost(
                                    totalCurrentWeight - SidebarLayoutConstants.MIN_PANE_WEIGHT
                                )
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }

                // A is discrete: snap to A's size if in range
                regimeA is PaneSizeRegime.Discrete -> {
                    val inRangeA =
                        virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp
                    if (inRangeA) {
                        val targetWeightA = (regimeA.sizeDp / totalSizeDp) * totalWeight
                        val newWeightA = targetWeightA.coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    } else {
                        // Outside discrete range - use virtual size
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA =
                            targetWeightA
                                .coerceAtLeast(
                                    SidebarLayoutConstants.MIN_PANE_WEIGHT
                                ).coerceAtMost(
                                    totalCurrentWeight - SidebarLayoutConstants.MIN_PANE_WEIGHT
                                )
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }

                // B is discrete: snap to B's size if in range
                regimeB is PaneSizeRegime.Discrete -> {
                    val inRangeB =
                        virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp
                    if (inRangeB) {
                        val targetWeightB = (regimeB.sizeDp / totalSizeDp) * totalWeight
                        val newWeightB = targetWeightB.coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    } else {
                        // Outside discrete range - use virtual size
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA =
                            targetWeightA
                                .coerceAtLeast(
                                    SidebarLayoutConstants.MIN_PANE_WEIGHT
                                ).coerceAtMost(
                                    totalCurrentWeight - SidebarLayoutConstants.MIN_PANE_WEIGHT
                                )
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }

                // Both continuous: use virtual sizes
                else -> {
                    val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                    val newWeightA =
                        targetWeightA
                            .coerceAtLeast(
                                SidebarLayoutConstants.MIN_PANE_WEIGHT
                            ).coerceAtMost(
                                totalCurrentWeight - SidebarLayoutConstants.MIN_PANE_WEIGHT
                            )
                    val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                        SidebarLayoutConstants.MIN_PANE_WEIGHT
                    )
                    weightMap[keyA] = newWeightA
                    weightMap[keyB] = newWeightB
                }
            }

            onWeightsChanged?.invoke(weightMap.toMap())

            // Reset drag state
            dragStartSizeDpA = null
            dragStartSizeDpB = null
            dragStartKeyA = null
            dragStartKeyB = null
            dragOffsetPx = 0f
        }

        fun syncWeights(newWeights: Map<Any, Float>) {
            if (dragStartKeyA == null) {
                newWeights.forEach { (k, v) ->
                    if (weightMap[k] != v) {
                        weightMap[k] = v
                    }
                }
            }
        }

        private fun getTotalWeight(): Float = weightMap.values.sum()

        // Handle dragging: Track virtual handle position and apply regime constraints
        // During drag, we track a virtual position as if both are continuous
        // Then apply discrete regime constraints if the virtual position falls within discrete ranges
        fun resize(keys: List<Any>, indexA: Int, delta: Float, totalSizePx: Int): Boolean {
            if (totalSizePx == 0) return false

            val keyA = keys[indexA]
            val keyB = keys[indexA + 1]

            // Accumulate drag offset (virtual handle position)
            dragOffsetPx += delta

            // Calculate virtual sizes as if both are continuous
            val dragStartDpA = dragStartSizeDpA ?: getSizeDp(keyA, totalSizePx, getTotalWeight())
            val dragStartDpB = dragStartSizeDpB ?: getSizeDp(keyB, totalSizePx, getTotalWeight())
            val deltaDp = with(density) { dragOffsetPx.toDp().value }
            val virtualSizeDpA = dragStartDpA + deltaDp
            val virtualSizeDpB = dragStartDpB - deltaDp

            // Determine which regimes these virtual sizes would fall into
            val regimeA = getCurrentRegime(keyA, virtualSizeDpA)
            val regimeB = getCurrentRegime(keyB, virtualSizeDpB)

            // Calculate target sizes based on regimes
            val targetSizeDpA =
                when (regimeA) {
                    is PaneSizeRegime.Discrete -> {
                        // If virtual position is within discrete range, use discrete size
                        if (virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp) {
                            regimeA.sizeDp
                        } else {
                            // Outside discrete range, use virtual size (will cross to another regime)
                            virtualSizeDpA
                        }
                    }

                    is PaneSizeRegime.Continuous -> {
                        virtualSizeDpA
                    }

                    null -> {
                        virtualSizeDpA
                    }
                }

            val targetSizeDpB =
                when (regimeB) {
                    is PaneSizeRegime.Discrete -> {
                        // If virtual position is within discrete range, use discrete size
                        if (virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp) {
                            regimeB.sizeDp
                        } else {
                            // Outside discrete range, use virtual size (will cross to another regime)
                            virtualSizeDpB
                        }
                    }

                    is PaneSizeRegime.Continuous -> {
                        virtualSizeDpB
                    }

                    null -> {
                        virtualSizeDpB
                    }
                }

            // Handle both discrete case: larger one wins
            if (regimeA is PaneSizeRegime.Discrete && regimeB is PaneSizeRegime.Discrete) {
                val sizeA = regimeA.sizeDp
                val sizeB = regimeB.sizeDp
                val inRangeA = virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp
                val inRangeB = virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp

                if (inRangeA && inRangeB) {
                    // Both in discrete ranges - larger one wins
                    if (sizeA >= sizeB) {
                        // A wins
                        val totalWeight = getTotalWeight()
                        val targetWeightA =
                            (sizeA / with(density) { totalSizePx.toDp().value }) * totalWeight
                        val weightA = getWeight(keyA)
                        val weightB = getWeight(keyB)
                        val totalCurrentWeight = weightA + weightB
                        val newWeightA = targetWeightA.coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                        return true
                    } else {
                        // B wins
                        val totalWeight = getTotalWeight()
                        val targetWeightB =
                            (sizeB / with(density) { totalSizePx.toDp().value }) * totalWeight
                        val weightA = getWeight(keyA)
                        val weightB = getWeight(keyB)
                        val totalCurrentWeight = weightA + weightB
                        val newWeightB = targetWeightB.coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(
                            SidebarLayoutConstants.MIN_PANE_WEIGHT
                        )
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                        return true
                    }
                }
            }

            // Convert target sizes to weights and apply
            val totalWeight = getTotalWeight()
            val totalSizeDp = with(density) { totalSizePx.toDp().value }
            val targetWeightA = (targetSizeDpA / totalSizeDp) * totalWeight
            val targetWeightB = (targetSizeDpB / totalSizeDp) * totalWeight

            // Ensure weights don't invert
            val weightA = getWeight(keyA)
            val weightB = getWeight(keyB)
            val totalCurrentWeight = weightA + weightB

            val newWeightA =
                targetWeightA
                    .coerceAtLeast(
                        SidebarLayoutConstants.MIN_PANE_WEIGHT
                    ).coerceAtMost(totalCurrentWeight - SidebarLayoutConstants.MIN_PANE_WEIGHT)
            val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(
                SidebarLayoutConstants.MIN_PANE_WEIGHT
            )

            // Update state
            weightMap[keyA] = newWeightA
            weightMap[keyB] = newWeightB

            // Update size in dp
            val newTotalWeight = getTotalWeight()
            sizeDpMap[keyA] = getSizeDp(keyA, totalSizePx, newTotalWeight)
            sizeDpMap[keyB] = getSizeDp(keyB, totalSizePx, newTotalWeight)

            return true
        }
    }

    val density = LocalDensity.current

    val scope = remember { DockedRegionScopeImpl() }
    scope.items.clear()
    block(scope)

    val paneRegimes =
        remember(scope.items) {
            scope.items.associate { it.key to it.regimes }
        }
    val state =
        remember(density, paneRegimes) {
            State(density, paneRegimes, initialWeights, onWeightsChanged)
        }

    val currentKeys = scope.items.map { it.key }
    var totalSizePx by remember { mutableIntStateOf(0) }

    // Reconcile when keys or total size changes
    androidx.compose.runtime.LaunchedEffect(currentKeys, totalSizePx) {
        state.reconcile(currentKeys, totalSizePx)
    }

    // Sync weights from DB when they change
    androidx.compose.runtime.LaunchedEffect(initialWeights) {
        state.syncWeights(initialWeights)
    }

    val draw: @Composable GenericLayoutScope.() -> Unit = @Composable {
        scope.items.forEachIndexed { index, item ->
            Box(
                modifier = item.modifier.weight(state.getWeight(item.key))
            ) {
                item.content()
            }
            // return@forEachIndexed
            if (index < scope.items.size - 1) {
                val interactionSource = remember { MutableInteractionSource() }
                val isHovered by interactionSource.collectIsHoveredAsState()
                var dragStarted by remember { mutableStateOf(false) }

                val modifier =
                    Modifier
                        .pointerHoverIcon(PointerIcon.Hand)
                        .hoverable(interactionSource)
                        .pointerInput(Unit) {
                            if (orientation == Orientation.Horizontal) {
                                detectHorizontalDragGestures(
                                    onDragStart = {
                                        dragStarted = true
                                        state.startDrag(
                                            currentKeys[index],
                                            currentKeys[index + 1],
                                            totalSizePx
                                        )
                                    },
                                    onDragEnd = {
                                        if (dragStarted) {
                                            state.endDrag(currentKeys, index, totalSizePx)
                                            dragStarted = false
                                        }
                                    },
                                    onDragCancel = {
                                        if (dragStarted) {
                                            state.endDrag(currentKeys, index, totalSizePx)
                                            dragStarted = false
                                        }
                                    },
                                    onHorizontalDrag = {
                                            change: PointerInputChange,
                                            dragAmount: Float
                                        ->
                                        change.consume()
                                        state.resize(currentKeys, index, dragAmount, totalSizePx)
                                    }
                                )
                            } else {
                                detectVerticalDragGestures(
                                    onDragStart = {
                                        dragStarted = true
                                        state.startDrag(
                                            currentKeys[index],
                                            currentKeys[index + 1],
                                            totalSizePx
                                        )
                                    },
                                    onDragEnd = {
                                        if (dragStarted) {
                                            state.endDrag(currentKeys, index, totalSizePx)
                                            dragStarted = false
                                        }
                                    },
                                    onDragCancel = {
                                        if (dragStarted) {
                                            state.endDrag(currentKeys, index, totalSizePx)
                                            dragStarted = false
                                        }
                                    },
                                    onVerticalDrag = {
                                            change: PointerInputChange,
                                            dragAmount: Float
                                        ->
                                        change.consume()
                                        state.resize(currentKeys, index, dragAmount, totalSizePx)
                                    }
                                )
                            }
                        }
                Box(
                    modifier =
                        when (orientation) {
                            Orientation.Horizontal -> {
                                modifier
                                    .width(8.dp)
                            }

                            Orientation.Vertical -> {
                                modifier
                                    .height(8.dp)
                            }
                        }
                ) {
                    val color =
                        if (isHovered) {
                            MaterialTheme.colorScheme.primary.copy(alpha = 0.3f)
                        } else {
                            MaterialTheme.colorScheme.outline.copy(alpha = 0.1f)
                        }
                    when (orientation) {
                        Orientation.Horizontal -> {
                            VerticalDivider(
                                modifier = Modifier.align(Alignment.Center),
                                color = color
                            )
                        }

                        Orientation.Vertical -> {
                            HorizontalDivider(
                                modifier = Modifier.align(Alignment.Center),
                                color = color
                            )
                        }
                    }
                }
            }
        }
    }

    when (orientation) {
        Orientation.Horizontal -> {
            class RowScopeAdapter(private val rowScope: RowScope) : GenericLayoutScope {
                override fun Modifier.weight(weight: Float, fill: Boolean): Modifier =
                    with(rowScope) { this@weight.weight(weight, fill) }
            }
            Row(
                modifier =
                    modifier.onSizeChanged {
                        totalSizePx = it.width
                    }
            ) {
                val adapter = remember(this) { RowScopeAdapter(this) }
                draw(adapter)
            }
        }

        Orientation.Vertical -> {
            class ColumnScopeAdapter(private val colScope: ColumnScope) : GenericLayoutScope {
                override fun Modifier.weight(weight: Float, fill: Boolean): Modifier =
                    with(colScope) { this@weight.weight(weight, fill) }
            }
            Column(
                modifier =
                    modifier.onSizeChanged {
                        totalSizePx = it.height
                    }
            ) {
                val adapter = remember(this) { ColumnScopeAdapter(this) }
                draw(adapter)
            }
        }
    }
}
