@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

// TablesTabsList lives in the same package (`org.example.daybook.tables`) so no import required
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
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
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.layout.width
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExtendedFloatingActionButton
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.PermanentDrawerSheet
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Switch
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
import org.example.daybook.ChromeState
import org.example.daybook.ChromeStateTopAppBar
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalChromeStateManager
import org.example.daybook.LocalContainer
import org.example.daybook.AppScreens
import org.example.daybook.Routes
import org.example.daybook.uniffi.core.LayoutPane
import org.example.daybook.uniffi.core.LayoutPaneVariant
import org.example.daybook.uniffi.core.LayoutRegion
import org.example.daybook.uniffi.core.LayoutWindowConfig
import org.example.daybook.uniffi.core.Orientation as ConfigOrientation
import org.example.daybook.uniffi.core.RegionSize
import org.example.daybook.uniffi.core.RootLayoutRegion

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ExpandedLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null
) {
    var showFeaturesMenu by remember { mutableStateOf(false) }
    val navBarFeatures = rememberNavBarFeatures(navController)
    val sidebarFeatures = rememberSidebarFeatures(navController)
    val menuFeatures = rememberMenuFeatures(navController)
    val scope = rememberCoroutineScope()

    // Config ViewModel
    val configRepo = LocalContainer.current.configRepo
    val configVm = viewModel { ConfigViewModel(configRepo) }

    // Observe layout config
    val layoutConfigState = configVm.layoutConfig.collectAsState()
    val layoutConfig = layoutConfigState.value

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
    val allMenuFeatures = remember(menuFeatures, nonProminentButtons) {
        menuFeatures + nonProminentButtons.map { button ->
            FeatureItem(
                key = button.key,
                icon = "", // Will use button.icon() composable instead
                label = "", // Will use button.label() composable instead
                onActivate = { button.onClick() }
            )
        }
    }

    // Merge layout-specific chrome with screen chrome
    // Check if screen chrome is empty (no title, no navigation icon, no actions, and showTopBar is false)
    val isScreenChromeEmpty = screenChromeState.title == null &&
            screenChromeState.navigationIcon == null &&
            screenChromeState.actions == null &&
            !screenChromeState.showTopBar
    val mergedChromeState = ChromeState(
        title = screenChromeState.title ?: "Daybook",
        navigationIcon = screenChromeState.navigationIcon ?: {
            IconButton(onClick = {
                // TODO: toggle left pane
            }) {
                Text("☰")
            }
        },
        actions = {
            // Screen actions first, then layout actions
            screenChromeState.actions?.invoke()
            Box {
                IconButton(onClick = { showFeaturesMenu = true }) {
                    Text("☰")
                }
                DropdownMenu(
                    expanded = showFeaturesMenu,
                    onDismissRequest = { showFeaturesMenu = false }
                ) {
                    allMenuFeatures.forEach { item ->
                        // Check if this is a chrome button (has empty icon/label strings)
                        val isChromeButton = item.icon.isEmpty() && item.label.isEmpty()
                        val chromeButton = if (isChromeButton) {
                            nonProminentButtons.find { it.key == item.key }
                        } else null
                        
                        DropdownMenuItem(
                            text = {
                                if (chromeButton != null) {
                                    chromeButton.label()
                                } else {
                                    Text(item.label)
                                }
                            },
                            onClick = {
                                showFeaturesMenu = false
                                scope.launch {
                                    item.onActivate()
                                }
                            },
                            leadingIcon = {
                                if (chromeButton != null) {
                                    chromeButton.icon()
                                } else {
                                    Text(item.icon)
                                }
                            }
                        )
                    }
                }
            }
        },
        showTopBar = if (isScreenChromeEmpty) true else screenChromeState.showTopBar
    )

    Scaffold(
        modifier = modifier,
        snackbarHost = { SnackbarHost(snackbarHostState) },
        topBar = {
            ChromeStateTopAppBar(mergedChromeState)
        }
    ) { innerPadding ->
        if (layoutConfig != null) {
            LayoutFromConfig(
                layoutConfig = layoutConfig,
                configVm = configVm,
                navController = navController,
                extraAction = extraAction,
                modifier = Modifier.padding(innerPadding).fillMaxSize()
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
    layoutConfig: LayoutWindowConfig,
    configVm: ConfigViewModel,
    navController: NavHostController,
    extraAction: (() -> Unit)?,
    modifier: Modifier = Modifier
) {
    DockableRegion(
        orientation = Orientation.Horizontal,
        modifier = modifier
    ) {
        // Left region (if visible)
        if (layoutConfig.leftVisible) {
            val leftPane = layoutConfig.leftRegion.deets
            val leftRegimes = if (leftPane.variant is LayoutPaneVariant.Sidebar) {
                // Sidebar: discrete 0-80dp for rail mode (80dp size), continuous above
                listOf(
                    PaneSizeRegime.Discrete(minDp = 0f, maxDp = 235f, sizeDp = 80f),
                    PaneSizeRegime.Continuous(minDp = 80f)
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
                    modifier = Modifier.fillMaxSize()
                )
            }
        }
        
        // Center region (always visible)
        pane(key = layoutConfig.centerRegion.deets.key) {
            RenderLayoutPane(
                pane = layoutConfig.centerRegion.deets,
                navController = navController,
                extraAction = extraAction,
                modifier = Modifier.fillMaxSize()
            )
        }
        
        // Right region (if visible)
        if (layoutConfig.rightVisible) {
            pane(key = layoutConfig.rightRegion.deets.key) {
                RenderLayoutPane(
                    pane = layoutConfig.rightRegion.deets,
                    navController = navController,
                    extraAction = extraAction,
                    modifier = Modifier.fillMaxSize()
                )
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SidebarContent(
    navController: NavHostController,
    modifier: Modifier = Modifier
) {
    val density = LocalDensity.current
    var widthPx by remember { mutableIntStateOf(0) }
    val widthDp = with(density) { widthPx.toDp() }
    val isWide = widthDp >= 200.dp
    
    val sidebarFeatures = rememberSidebarFeatures(navController)
    val allFeatures = rememberAllFeatures(navController)
    val scope = rememberCoroutineScope()
    
    // Observe route changes to update selection highlight
    // Use currentBackStackEntryAsState to reactively observe route changes
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentRoute = navBackStackEntry?.destination?.route
    
    // Get chrome state to check for main feature action button and prominent buttons
    val chromeStateManager = LocalChromeStateManager.current
    val chromeState by chromeStateManager.currentState.collectAsState()
    val mainFeatureActionButton = chromeState.mainFeatureActionButton
    val prominentButtons = chromeState.additionalFeatureButtons.filter { it.prominent }
    
    // Default to Capture feature if no chrome button is provided
    val captureFeature = allFeatures.find { it.key == "nav_capture" }
    
    // Combine sidebar features with prominent chrome buttons
    val allSidebarFeatures = remember(sidebarFeatures, prominentButtons) {
        sidebarFeatures + prominentButtons.map { button ->
            FeatureItem(
                key = button.key,
                icon = "", // Will use button.icon() composable instead
                label = "", // Will use button.label() composable instead
                onActivate = { button.onClick() }
            )
        }
    }
    
    // Map feature keys to routes for selection
    fun getRouteForFeature(feature: FeatureItem): String? {
        return when (feature.key) {
            "nav_home" -> AppScreens.Home.name
            "nav_tables" -> AppScreens.Tables.name
            "nav_capture" -> AppScreens.Capture.name
            "nav_search" -> AppScreens.Search.name
            "nav_settings" -> AppScreens.Settings.name
            else -> null
        }
    }
    
    Box(
        modifier = modifier.onSizeChanged {
            widthPx = it.width
        }
    ) {
        if (isWide) {
            // Wide mode: PermanentDrawerSheet with features and TabSelectionList
            PermanentDrawerSheet(
                modifier = Modifier
                    .widthIn(min = 240.dp)
                    .fillMaxSize()
            ) {
                Column(
                    modifier = Modifier.fillMaxSize()
                ) {
                    // Main feature action button at the top (from chrome or default to Capture)
                    when (val button = mainFeatureActionButton) {
                        is org.example.daybook.MainFeatureActionButton.Button -> {
                            ExtendedFloatingActionButton(
                                text = button.label,
                                icon = button.icon,
                                onClick = {
                                    if (button.enabled) {
                                        scope.launch {
                                            button.onClick()
                                        }
                                    }
                                },
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .padding(horizontal = 16.dp, vertical = 8.dp)
                            )
                            HorizontalDivider()
                        }
                        null -> {
                            // Default to Capture feature button when no chrome button is provided
                            captureFeature?.let { feature ->
                                ExtendedFloatingActionButton(
                                    text = { Text(feature.label) },
                                    icon = { Text(feature.icon) },
                                    onClick = {
                                        scope.launch {
                                            feature.onActivate()
                                        }
                                    },
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .padding(horizontal = 16.dp, vertical = 8.dp)
                                )
                                HorizontalDivider()
                            }
                        }
                    }
                    
                    // Features section - Home, Search, and prominent chrome buttons
                    allSidebarFeatures.forEach { item ->
                        val featureRoute = getRouteForFeature(item)
                        val isSelected = featureRoute != null && featureRoute == currentRoute
                        
                        // Check if this is a chrome button (has empty icon/label strings)
                        val isChromeButton = item.icon.isEmpty() && item.label.isEmpty()
                        val chromeButton = if (isChromeButton) {
                            prominentButtons.find { it.key == item.key }
                        } else null
                        
                        NavigationDrawerItem(
                            selected = isSelected,
                            onClick = {
                                scope.launch {
                                    item.onActivate()
                                }
                            },
                            icon = {
                                if (chromeButton != null) {
                                    chromeButton.icon()
                                } else {
                                    Text(item.icon)
                                }
                            },
                            label = {
                                if (chromeButton != null) {
                                    chromeButton.label()
                                } else {
                                    Text(item.label)
                                }
                            },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                    
                    // Divider between features and tabs
                    HorizontalDivider()
                    
                    // TabSelectionList section
                    TabSelectionList(
                        onTabSelected = { /* TODO: Handle tab selection */ },
                        modifier = Modifier.weight(1f)
                    )
                }
            }
        } else {
            // Narrow mode: NavigationRail with features only
            NavigationRail(modifier = Modifier.fillMaxHeight()) {
                // Main feature action button at the top (from chrome or default to Capture)
                when (val button = mainFeatureActionButton) {
                    is org.example.daybook.MainFeatureActionButton.Button -> {
                        Box(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(horizontal = 8.dp, vertical = 8.dp),
                            contentAlignment = Alignment.Center
                        ) {
                            FloatingActionButton(
                                onClick = {
                                    if (button.enabled) {
                                        scope.launch {
                                            button.onClick()
                                        }
                                    }
                                }
                            ) {
                                button.icon()
                            }
                        }
                    }
                    null -> {
                        // Default to Capture feature button when no chrome button is provided
                        captureFeature?.let { feature ->
                            Box(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .padding(horizontal = 8.dp, vertical = 8.dp),
                                contentAlignment = Alignment.Center
                            ) {
                                FloatingActionButton(
                                    onClick = {
                                        scope.launch {
                                            feature.onActivate()
                                        }
                                    }
                                ) {
                                    Text(feature.icon)
                                }
                            }
                        }
                    }
                }
                
                allSidebarFeatures.forEach { item ->
                    val featureRoute = getRouteForFeature(item)
                    val isSelected = featureRoute != null && featureRoute == currentRoute
                    
                    // Check if this is a chrome button (has empty icon/label strings)
                    val isChromeButton = item.icon.isEmpty() && item.label.isEmpty()
                    val chromeButton = if (isChromeButton) {
                        prominentButtons.find { it.key == item.key }
                    } else null
                    
                    NavigationRailItem(
                        selected = isSelected,
                        onClick = {
                            scope.launch {
                                item.onActivate()
                            }
                        },
                        icon = {
                            if (chromeButton != null) {
                                chromeButton.icon()
                            } else {
                                Text(item.icon)
                            }
                        },
                        label = {
                            if (chromeButton != null) {
                                chromeButton.label()
                            } else {
                                Text(item.label)
                            }
                        }
                    )
                }
            }
        }
    }
}

@Composable
fun RenderLayoutPane(
    pane: LayoutPane,
    navController: NavHostController,
    extraAction: (() -> Unit)?,
    modifier: Modifier = Modifier
) {
    when (val variant = pane.variant) {
        is LayoutPaneVariant.Sidebar -> {
            // Render sidebar UI
            SidebarContent(
                navController = navController,
                modifier = modifier
            )
        }
        is LayoutPaneVariant.Routes -> {
            // Render routes
            Routes(
                extraAction = extraAction,
                navController = navController,
                modifier = modifier
            )
        }
        is LayoutPaneVariant.Region -> {
            // Render nested region recursively
            RenderLayoutRegion(
                region = variant.v1,
                navController = navController,
                extraAction = extraAction,
                modifier = modifier
            )
        }
    }
}

@Composable
fun RenderLayoutRegion(
    region: LayoutRegion,
    navController: NavHostController,
    extraAction: (() -> Unit)?,
    modifier: Modifier = Modifier
) {
    val orientation = when (region.orientation) {
        ConfigOrientation.HORIZONTAL -> Orientation.Horizontal
        ConfigOrientation.VERTICAL -> Orientation.Vertical
    }
    
    DockableRegion(
        orientation = orientation,
        modifier = modifier
    ) {
        region.children.forEach { childPane ->
            val childRegimes = if (childPane.variant is LayoutPaneVariant.Sidebar) {
                // Sidebar: discrete 0-80dp for rail mode (80dp size), continuous above
                listOf(
                    PaneSizeRegime.Discrete(minDp = 0f, maxDp = 235f, sizeDp = 80f),
                    PaneSizeRegime.Continuous(minDp = 80f)
                )
            } else {
                // Default: continuous
                listOf(PaneSizeRegime.Continuous())
            }
            pane(key = childPane.key, regimes = childRegimes) {
                RenderLayoutPane(
                    pane = childPane,
                    navController = navController,
                    extraAction = extraAction,
                    modifier = Modifier.fillMaxSize()
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
    data class Continuous(
        val minDp: Float = 0f,
        val maxDp: Float = Float.MAX_VALUE
    ) : PaneSizeRegime()
    
    /**
     * Discrete sizing - pane has a fixed explicit size that must fall within the range.
     * Size only changes when crossing to another regime.
     */
    data class Discrete(
        val minDp: Float,
        val maxDp: Float,
        val sizeDp: Float
    ) : PaneSizeRegime() {
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
        private val paneRegimes: Map<Any, List<PaneSizeRegime>>
    ) {
        // We map Keys to Weights.
        // This ensures if you reorder items, their size travels with them.
        private val weightMap = mutableStateMapOf<Any, Float>()
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
            // Optional: Garbage collect keys that are no longer in the list
            val currentKeySet = keys.toSet()
            val iterator = weightMap.iterator()
            while (iterator.hasNext()) {
                if (!currentKeySet.contains(iterator.next().key)) {
                    iterator.remove()
                }
            }
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
                    val inRangeA = virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp
                    val inRangeB = virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp
                    
                    if (inRangeA && inRangeB) {
                        // Both in discrete ranges - larger one wins
                        if (sizeA >= sizeB) {
                            val targetWeightA = (sizeA / totalSizeDp) * totalWeight
                            val newWeightA = targetWeightA.coerceAtLeast(0.1f)
                            val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                            weightMap[keyA] = newWeightA
                            weightMap[keyB] = newWeightB
                        } else {
                            val targetWeightB = (sizeB / totalSizeDp) * totalWeight
                            val newWeightB = targetWeightB.coerceAtLeast(0.1f)
                            val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(0.1f)
                            weightMap[keyA] = newWeightA
                            weightMap[keyB] = newWeightB
                        }
                    } else {
                        // One or both outside discrete range - use virtual sizes
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA = targetWeightA.coerceAtLeast(0.1f).coerceAtMost(totalCurrentWeight - 0.1f)
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }
                
                // A is discrete: snap to A's size if in range
                regimeA is PaneSizeRegime.Discrete -> {
                    val inRangeA = virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp
                    if (inRangeA) {
                        val targetWeightA = (regimeA.sizeDp / totalSizeDp) * totalWeight
                        val newWeightA = targetWeightA.coerceAtLeast(0.1f)
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    } else {
                        // Outside discrete range - use virtual size
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA = targetWeightA.coerceAtLeast(0.1f).coerceAtMost(totalCurrentWeight - 0.1f)
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }
                
                // B is discrete: snap to B's size if in range
                regimeB is PaneSizeRegime.Discrete -> {
                    val inRangeB = virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp
                    if (inRangeB) {
                        val targetWeightB = (regimeB.sizeDp / totalSizeDp) * totalWeight
                        val newWeightB = targetWeightB.coerceAtLeast(0.1f)
                        val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    } else {
                        // Outside discrete range - use virtual size
                        val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                        val newWeightA = targetWeightA.coerceAtLeast(0.1f).coerceAtMost(totalCurrentWeight - 0.1f)
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                    }
                }
                
                // Both continuous: use virtual sizes
                else -> {
                    val targetWeightA = (virtualSizeDpA / totalSizeDp) * totalWeight
                    val newWeightA = targetWeightA.coerceAtLeast(0.1f).coerceAtMost(totalCurrentWeight - 0.1f)
                    val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                    weightMap[keyA] = newWeightA
                    weightMap[keyB] = newWeightB
                }
            }
            
            // Reset drag state
            dragStartSizeDpA = null
            dragStartSizeDpB = null
            dragStartKeyA = null
            dragStartKeyB = null
            dragOffsetPx = 0f
        }

        private fun getTotalWeight(): Float {
            return weightMap.values.sum()
        }

        // Handle dragging: Track virtual handle position and apply regime constraints
        // During drag, we track a virtual position as if both are continuous
        // Then apply discrete regime constraints if the virtual position falls within discrete ranges
        fun resize(
            keys: List<Any>,
            indexA: Int,
            delta: Float,
            totalSizePx: Int
        ): Boolean {
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
            val targetSizeDpA = when (regimeA) {
                is PaneSizeRegime.Discrete -> {
                    // If virtual position is within discrete range, use discrete size
                    if (virtualSizeDpA >= regimeA.minDp && virtualSizeDpA <= regimeA.maxDp) {
                        regimeA.sizeDp
                    } else {
                        // Outside discrete range, use virtual size (will cross to another regime)
                        virtualSizeDpA
                    }
                }
                is PaneSizeRegime.Continuous -> virtualSizeDpA
                null -> virtualSizeDpA
            }

            val targetSizeDpB = when (regimeB) {
                is PaneSizeRegime.Discrete -> {
                    // If virtual position is within discrete range, use discrete size
                    if (virtualSizeDpB >= regimeB.minDp && virtualSizeDpB <= regimeB.maxDp) {
                        regimeB.sizeDp
                    } else {
                        // Outside discrete range, use virtual size (will cross to another regime)
                        virtualSizeDpB
                    }
                }
                is PaneSizeRegime.Continuous -> virtualSizeDpB
                null -> virtualSizeDpB
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
                        val targetWeightA = (sizeA / with(density) { totalSizePx.toDp().value }) * totalWeight
                        val weightA = getWeight(keyA)
                        val weightB = getWeight(keyB)
                        val totalCurrentWeight = weightA + weightB
                        val newWeightA = targetWeightA.coerceAtLeast(0.1f)
                        val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)
                        weightMap[keyA] = newWeightA
                        weightMap[keyB] = newWeightB
                        return true
                    } else {
                        // B wins
                        val totalWeight = getTotalWeight()
                        val targetWeightB = (sizeB / with(density) { totalSizePx.toDp().value }) * totalWeight
                        val weightA = getWeight(keyA)
                        val weightB = getWeight(keyB)
                        val totalCurrentWeight = weightA + weightB
                        val newWeightB = targetWeightB.coerceAtLeast(0.1f)
                        val newWeightA = (totalCurrentWeight - newWeightB).coerceAtLeast(0.1f)
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
            
            val newWeightA = targetWeightA.coerceAtLeast(0.1f).coerceAtMost(totalCurrentWeight - 0.1f)
            val newWeightB = (totalCurrentWeight - newWeightA).coerceAtLeast(0.1f)

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
    
    val paneRegimes = remember(scope.items) {
        scope.items.associate { it.key to it.regimes }
    }
    val state = remember(density, paneRegimes) { 
        State(density, paneRegimes) 
    }

    val currentKeys = scope.items.map { it.key }
    var totalSizePx by remember { mutableIntStateOf(0) }
    
    // Reconcile when keys or total size changes
    androidx.compose.runtime.LaunchedEffect(currentKeys, totalSizePx) {
        state.reconcile(currentKeys, totalSizePx)
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
                
                val modifier = Modifier
                    .pointerHoverIcon(PointerIcon.Hand)
                    .hoverable(interactionSource)
                    .pointerInput(Unit) {
                        if (orientation == Orientation.Horizontal) {
                            detectHorizontalDragGestures(
                                onDragStart = {
                                    dragStarted = true
                                    state.startDrag(currentKeys[index], currentKeys[index + 1], totalSizePx)
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
                                onHorizontalDrag = { change: PointerInputChange, dragAmount: Float ->
                                    change.consume()
                                    state.resize(currentKeys, index, dragAmount, totalSizePx)
                                }
                            )
                        } else {
                            detectVerticalDragGestures(
                                onDragStart = {
                                    dragStarted = true
                                    state.startDrag(currentKeys[index], currentKeys[index + 1], totalSizePx)
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
                                onVerticalDrag = { change: PointerInputChange, dragAmount: Float ->
                                    change.consume()
                                    state.resize(currentKeys, index, dragAmount, totalSizePx)
                                }
                            )
                        }
                    };
                Box(
                    modifier = when (orientation) {
                        Orientation.Horizontal -> modifier
                            .width(8.dp)

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
    };


    when (orientation) {
        Orientation.Horizontal -> {
            class RowScopeAdapter(private val rowScope: RowScope) : GenericLayoutScope {
                override fun Modifier.weight(weight: Float, fill: Boolean): Modifier {
                    return with(rowScope) { this@weight.weight(weight, fill) }
                }
            }
            Row(
                modifier = modifier.onSizeChanged {
                    totalSizePx = it.width
                }
            ) {
                val adapter = remember(this) { RowScopeAdapter(this) }
                draw(adapter)
            }
        }

        Orientation.Vertical -> {
            class ColumnScopeAdapter(private val colScope: ColumnScope) : GenericLayoutScope {
                override fun Modifier.weight(weight: Float, fill: Boolean): Modifier {
                    return with(colScope) { this@weight.weight(weight, fill) }
                }
            }
            Column(
                modifier = modifier.onSizeChanged {
                    totalSizePx = it.height
                }
            ) {
                val adapter = remember(this) { ColumnScopeAdapter(this) }
                draw(adapter)
            }
        }
    }
}

@Composable
fun TablesTabsList(
    onToggleTableRail: () -> Unit,
    showToggleButton: Boolean = true
) {
    Column(modifier = Modifier.fillMaxHeight()) {
        TabSelectionList(
            onTabSelected = { /* TODO: Select tab */ },
            modifier = Modifier.weight(1f)
        )

        // Bottom row with toggle button (only when nav rail is hidden)
        if (showToggleButton) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(8.dp),
                horizontalArrangement = Arrangement.Start
            ) {
                IconButton(onClick = onToggleTableRail) {
                    Text("▶")
                }
            }
        }
    }
}
