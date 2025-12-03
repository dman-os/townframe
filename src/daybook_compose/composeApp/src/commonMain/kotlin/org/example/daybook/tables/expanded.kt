@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

// TablesTabsList lives in the same package (`org.example.daybook.tables`) so no import required
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.draggable
import androidx.compose.foundation.gestures.rememberDraggableState
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
import androidx.compose.foundation.layout.width
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
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
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.ChromeState
import org.example.daybook.ChromeStateTopAppBar
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalChromeStateStack
import org.example.daybook.LocalContainer
import org.example.daybook.Routes
import org.example.daybook.dockable.PaneState
import org.example.daybook.uniffi.core.SidebarMode
import org.example.daybook.uniffi.core.SidebarPosition
import org.example.daybook.uniffi.core.SidebarVisibility
import org.example.daybook.uniffi.core.TabListVisibility

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ExpandedLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null
) {
    var showFeaturesMenu by remember { mutableStateOf(false) }
    val features = rememberFeatures(navController)
    val scope = rememberCoroutineScope()

    // Config ViewModel
    val configRepo = LocalContainer.current.configRepo
    val configVm = viewModel { ConfigViewModel(configRepo) }

    // Observe config state
    val showTableRailState = configVm.tableRailVisExpanded.collectAsState()
    // Default to VISIBLE if not loaded yet (matches Rust default)
    val showTableRail = when (showTableRailState.value) {
        null -> true // Default to visible while loading
        TabListVisibility.VISIBLE -> true
        TabListVisibility.HIDDEN -> true
    }

    val showSidebarState = configVm.sidebarVisExpanded.collectAsState()
    val showSidebar = when (showSidebarState.value) {
        null -> true // Default to visible while loading
        SidebarVisibility.VISIBLE -> true
        SidebarVisibility.HIDDEN -> false
    }

    val sidebarPosState = configVm.sidebarPosExpanded.collectAsState()
    val sidebarPos = sidebarPosState.value ?: SidebarPosition.RIGHT

    // Sidebar mode and auto-hide
    val sidebarModeState = configVm.sidebarModeExpanded.collectAsState()
    val sidebarMode = sidebarModeState.value ?: SidebarMode.COMPACT
    val sidebarAutoHideState = configVm.sidebarAutoHideExpanded.collectAsState()
    val sidebarAutoHide = sidebarAutoHideState.value ?: false

    // Error handling
    val snackbarHostState = remember { SnackbarHostState() }
    val configError = configVm.error.collectAsState()
    LaunchedEffect(configError.value) {
        configError.value?.let { error ->
            snackbarHostState.showSnackbar(error.message)
            configVm.clearError()
        }
    }

    // Get chrome state stack and observe the top state (from the current screen)
    val chromeStateStack = LocalChromeStateStack.current
    val screenChromeState by chromeStateStack.topState.collectAsState()

    // Merge layout-specific chrome with screen chrome
    // Check if screen chrome is empty (no title, no navigation icon, no actions, and showTopBar is false)
    val isScreenChromeEmpty = screenChromeState.title == null &&
            screenChromeState.navigationIcon == null &&
            screenChromeState.actions == null &&
            !screenChromeState.showTopBar
    println("$screenChromeState XXX")
    val mergedChromeState = ChromeState(
        title = screenChromeState.title ?: "Daybook",
        navigationIcon = screenChromeState.navigationIcon ?: {
            IconButton(onClick = {
                // Toggle sidebar visibility
                configVm.setSidebarVisExpanded(
                    if (showSidebar)
                        SidebarVisibility.HIDDEN
                    else
                        SidebarVisibility.VISIBLE
                )
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
                    // When rail is visible, show only features not in the rail (like Settings)
                    // When rail is hidden, show all features (including ones that were in the rail)
                    val featuresToShow = if (showSidebar) {
                        // Filter out features that are shown in the sidebar (Home, Tables, Capture)
                        // Keep only features not in sidebar (Settings)
                        features.filter { it.key == "nav_settings" }
                    } else {
                        // When sidebar is hidden, show all features
                        features
                    }

                    featuresToShow.forEach { feature ->
                        DropdownMenuItem(
                            text = { Text(feature.label) },
                            onClick = {
                                showFeaturesMenu = false
                                scope.launch {
                                    feature.onActivate()
                                }
                            },
                            leadingIcon = {
                                Text(feature.icon)
                            }
                        )
                    }

                    // Separator and toggle
                    HorizontalDivider()
                    DropdownMenuItem(
                        text = {
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
                            ) {
                                Text(
                                    "Show Sidebar",
                                    modifier = Modifier.weight(1f)
                                )
                                Switch(
                                    checked = showSidebar,
                                    onCheckedChange = { checked ->
                                        showFeaturesMenu = false
                                        configVm.setSidebarVisExpanded(
                                            if (checked) SidebarVisibility.VISIBLE else SidebarVisibility.HIDDEN
                                        )
                                    }
                                )
                            }
                        },
                        onClick = {
                            // Toggle on click as well
                            showFeaturesMenu = false
                            configVm.setSidebarVisExpanded(
                                if (showSidebar) SidebarVisibility.HIDDEN else SidebarVisibility.VISIBLE
                            )
                        }
                    )
                }
            }
        },
        showTopBar = if (isScreenChromeEmpty) true else screenChromeState.showTopBar
    )

    // Resizable sidebar width state
    val density = androidx.compose.ui.platform.LocalDensity.current
    var sidebarWidth by rememberSaveable { mutableFloatStateOf(80f) }
    val sidebarWidthDp = sidebarWidth.dp

    // Determine effective mode based on width and config
    // If width is narrow (< 150dp), force compact mode
    // If width is wide (>= 150dp), use config mode (but respect hidden)
    val effectiveMode = when {
        !showSidebar -> SidebarMode.HIDDEN
        sidebarWidthDp < 150.dp -> SidebarMode.COMPACT
        sidebarMode == SidebarMode.HIDDEN -> SidebarMode.HIDDEN
        else -> sidebarMode
    }

    // Update config mode when width crosses threshold
    LaunchedEffect(sidebarWidthDp) {
        val widthBasedMode =
            if (sidebarWidthDp < 150.dp) SidebarMode.COMPACT else SidebarMode.EXPANDED
        if (widthBasedMode != sidebarMode && sidebarMode != SidebarMode.HIDDEN) {
            configVm.setSidebarModeExpanded(widthBasedMode)
        }
    }

    // Handler for pane resize - updates sidebar width
    val onLeftPaneResize: (Float) -> Unit = { dragAmount ->
        val newWidth = (sidebarWidth + dragAmount / density.density)
            .coerceIn(60f, 400f)
        sidebarWidth = newWidth
    }

    val onRightPaneResize: (Float) -> Unit = { dragAmount ->
        val newWidth = (sidebarWidth - dragAmount / density.density)
            .coerceIn(60f, 400f)
        sidebarWidth = newWidth
    }

    @Composable
    fun ResizableSidebar(
        modifier: Modifier = Modifier
    ) {
        Sidebar(
            navController = navController,
            onToggle = { configVm.setSidebarVisExpanded(SidebarVisibility.HIDDEN) },
            showToggleButton = true,
            scrollableContent = if (showTableRail) {
                {
                    TablesTabsList(
                        onToggleTableRail = {
                            configVm.setTableRailVisExpanded(
                                if (showTableRail)
                                    TabListVisibility.HIDDEN
                                else
                                    TabListVisibility.VISIBLE
                            )
                        },
                        showToggleButton = false
                    )
                }
            } else null,
            width = sidebarWidthDp,
            mode = effectiveMode,
            autoHide = sidebarAutoHide
        )
    }

    // Create pane states for dockable layout (mutable for resize updates)
    // Note: PaneState is a data class with var fields, so we can mutate it directly
    val leftPaneState = rememberSaveable {
        PaneState(
            "left",
            width = sidebarWidth,
            isVisible = showSidebar && sidebarPos == SidebarPosition.LEFT
        )
    }
    val rightPaneState = rememberSaveable {
        PaneState(
            "right",
            width = sidebarWidth,
            isVisible = showSidebar && sidebarPos == SidebarPosition.RIGHT
        )
    }

    // Update pane states when config changes
    LaunchedEffect(showSidebar, sidebarPos) {
        leftPaneState.isVisible = showSidebar && sidebarPos == SidebarPosition.LEFT
        rightPaneState.isVisible = showSidebar && sidebarPos == SidebarPosition.RIGHT
    }

    // Sync sidebar width with pane state
    LaunchedEffect(sidebarWidth) {
        leftPaneState.width = sidebarWidth
        rightPaneState.width = sidebarWidth
    }

    // Sync pane state width back to sidebar width when resized
    LaunchedEffect(leftPaneState.width) {
        if (sidebarPos == SidebarPosition.LEFT) {
            sidebarWidth = leftPaneState.width
        }
    }

    LaunchedEffect(rightPaneState.width) {
        if (sidebarPos == SidebarPosition.RIGHT) {
            sidebarWidth = rightPaneState.width
        }
    }

    // Create center region pane state
    val centerPaneState = rememberSaveable {
        PaneState("center", width = 0f, height = 0f, isVisible = true)
    }

    // Create bottom pane state (placeholder for now)
    val bottomPaneState = rememberSaveable {
        PaneState("bottom", width = 0f, height = 0f, isVisible = false)
    }

    // Handler for pane resize - needs to trigger recomposition
    val onPaneResize: (String, Float) -> Unit = { paneId, dragAmount ->
        val allPanes = listOf(
            leftPaneState,
            rightPaneState,
            centerPaneState,
            bottomPaneState
        )
        val pane = allPanes.find { it.id == paneId }
        if (pane != null) {
            val isHorizontal = pane.width > 0
            // Use the density from the composable scope
            if (isHorizontal) {
                val newWidth = (pane.width + dragAmount / density.density)
                    .coerceIn(pane.minWidth, pane.maxWidth)
                pane.width = newWidth
                // Also update sidebarWidth if this is a sidebar pane to trigger recomposition
                if (paneId == "left" || paneId == "right") {
                    sidebarWidth = newWidth
                }
            } else {
                val newHeight = (pane.height + dragAmount / density.density)
                    .coerceIn(pane.minHeight, pane.maxHeight)
                pane.height = newHeight
            }
        }
    }

    // Build the region structure using composable DSL:
    // Row Region (horizontal)
    //   - Left Pane (if sidebar on left)
    //   - Center Region (vertical)
    //     - Main Region (horizontal) - center pane
    //     - Bottom Region (horizontal) - bottom pane
    //   - Right Pane (if sidebar on right)

    Scaffold(
        modifier = modifier,
        snackbarHost = { SnackbarHost(snackbarHostState) },
        topBar = {
            ChromeStateTopAppBar(mergedChromeState)
        }
    ) { innerPadding ->
        DockableRegion(
            orientation = Orientation.Vertical,
            modifier = Modifier.padding(innerPadding).fillMaxSize(),
        ) {
            pane(key = "routes") {
                Routes(
                    extraAction = extraAction,
                    navController = navController,
                    modifier = Modifier.fillMaxSize()
                )
            }
            pane(key = "left") {
                Text("hi")
            }
        }
    }
}


interface DockedRegionScope {
    fun pane(key: Any, modifier: Modifier = Modifier, content: @Composable (() -> Unit))
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
        val content: @Composable (() -> Unit)
    )

    class DockedRegionScopeImpl : DockedRegionScope {
        val items = mutableListOf<RegionPaneData>()

        override fun pane(key: Any, modifier: Modifier, content: @Composable (() -> Unit)) {
            items.add(RegionPaneData(key, modifier, content))
        }
    }

    @Stable
    class State {
        // We map Keys to Weights.
        // This ensures if you reorder items, their size travels with them.
        private val weightMap = mutableStateMapOf<Any, Float>()

        // Accessor to get weight for a specific key
        fun getWeight(key: Any): Float = weightMap[key] ?: 1f

        // The Logic: Syncs the incoming N items with our storage
        fun reconcile(keys: List<Any>) {
            keys.forEach { key ->
                if (!weightMap.containsKey(key)) {
                    weightMap[key] = 1.0f // Default new items to 1.0
                }
            }
            // Optional: Garbage collect keys that are no longer in the list
            // (Not strictly necessary for small lists, but good for cleanup)
            val currentKeySet = keys.toSet()
            val iterator = weightMap.iterator()
            while (iterator.hasNext()) {
                if (!currentKeySet.contains(iterator.next().key)) {
                    iterator.remove()
                }
            }
        }

        // Handle dragging: Redistribute weight between index A and B
        fun resize(
            keys: List<Any>,
            indexA: Int,
            delta: Float,
            totalWidth: Int
        ) {
            if (totalWidth == 0) return

            val keyA = keys[indexA]
            val keyB = keys[indexA + 1]

            val weightA = getWeight(keyA)
            val weightB = getWeight(keyB)

            // Convert pixels to weight units
            // We approximate that sum of weights correlates to totalWidth
            // (Simplified logic for demonstration)
            val totalCurrentWeight = keys.sumOf { getWeight(it).toDouble() }.toFloat()
            val weightDelta = (delta / totalWidth) * totalCurrentWeight

            // Apply limits so items don't invert
            val newWeightA = (weightA + weightDelta).coerceAtLeast(0.1f)
            val newWeightB = (weightB - weightDelta).coerceAtLeast(0.1f)

            // Update state
            weightMap[keyA] = newWeightA
            weightMap[keyB] = newWeightB
        }
    }

    val state = remember { State() }

    val scope = remember { DockedRegionScopeImpl() }
    scope.items.clear()
    block(scope)

    val currentKeys = scope.items.map { it.key }
    state.reconcile(currentKeys)

    var totalSizePx by remember { mutableIntStateOf(0) }
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
                val modifier = Modifier
                    .pointerHoverIcon(PointerIcon.Hand)
                    .hoverable(interactionSource)
                    .draggable(
                        orientation = orientation,
                        state = rememberDraggableState { delta ->
                            state.resize(currentKeys, index, delta, totalSizePx)
                        }
                    );
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
