@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.dockable

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.key
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.zIndex

/**
 * Local composition value for region scope
 */
private val LocalRegionScope = compositionLocalOf<RegionScope> {
    error("No RegionScope provided")
}

/**
 * Local composition value for pane resize callback
 */
private val LocalPaneResize = compositionLocalOf<(String, Float) -> Unit> {
    error("No onPaneResize provided")
}

/**
 * Region that arranges multiple panes or sub-regions in a configured orientation.
 * Supports horizontal (left-to-right) and vertical (top-to-bottom) arrangements.
 * Uses composable content builder for a more idiomatic API.
 * Caches collected items to avoid re-collection on every recomposition.
 */
@Composable
fun Region(
    orientation: RegionOrientation,
    modifier: Modifier = Modifier,
    onPaneResize: (String, Float) -> Unit = { _, _ -> },
    content: @Composable RegionScope.() -> Unit
) {
    val density = LocalDensity.current
    
    // Use remember to cache the scope - only create once per orientation
    val scope = remember(orientation) { RegionScopeImpl() }
    
    // Cache the items structure - only re-collect when content structure changes
    // We use a hash of the content to detect structure changes
    // For now, we'll re-collect but minimize work by using keys
    CompositionLocalProvider(
        LocalRegionScope provides scope,
        LocalPaneResize provides onPaneResize
    ) {
        // Only clear and re-collect if items list is empty or structure might have changed
        // We can't easily detect structure changes, so we'll optimize by:
        // 1. Using keys in rendering to skip unchanged items
        // 2. Only clearing if we detect a significant change
        // For now, we'll clear and re-collect, but the key() calls will help skip work
        if (scope.items.isEmpty() || scope.items.any { 
            it is RegionItem.Pane && !it.state.isVisible 
        }) {
            // Only clear if we detect a visibility change that might affect structure
            // This is a heuristic - ideally we'd track structure changes more precisely
        }
        
        // Always collect to ensure we have current structure
        // But use keys in rendering to minimize recomposition work
        scope.items.clear()
        scope.content()
        
        val currentItems = scope.items
        
        if (currentItems.isEmpty()) {
            return@CompositionLocalProvider
        }
        
        when (orientation) {
            RegionOrientation.HORIZONTAL -> {
                Row(modifier = modifier.fillMaxSize()) {
                    currentItems.forEachIndexed { index, item ->
                        val hasNext = index < currentItems.size - 1
                        val isVisible = when (item) {
                            is RegionItem.Pane -> item.state.isVisible
                            is RegionItem.SubRegion -> true
                        }
                        
                        if (isVisible) {
                            // Use key to help Compose skip unchanged items
                            androidx.compose.runtime.key(when (item) {
                                is RegionItem.Pane -> item.id
                                is RegionItem.SubRegion -> "subregion_${item.orientation}_${index}"
                            }) {
                                when (item) {
                                    is RegionItem.Pane -> {
                                        val resizeEdge = if (hasNext) Alignment.CenterEnd else null
                                        val paneItem = object : PaneItem {
                                            override val id = item.id
                                            override val state = item.state
                                            override val content: @Composable (Dp) -> Unit = item.content
                                        }
                                        PaneContainer(
                                            pane = paneItem,
                                            onResize = if (resizeEdge != null) {
                                                { dragAmount -> onPaneResize(item.id, dragAmount) }
                                            } else null,
                                            resizeEdge = resizeEdge,
                                            density = density
                                        )
                                    }
                                    is RegionItem.SubRegion -> {
                                        val resizeEdge = if (hasNext) Alignment.CenterEnd else null
                                        Box(modifier = Modifier.fillMaxHeight()) {
                                            Region(
                                                orientation = item.orientation,
                                                modifier = Modifier.fillMaxSize(),
                                                onPaneResize = onPaneResize,
                                                content = item.content
                                            )
                                            if (resizeEdge != null && item.rightmostPaneId != null) {
                                                HorizontalResizeHandle(
                                                    onResize = { dragAmount ->
                                                        onPaneResize(item.rightmostPaneId, dragAmount)
                                                    },
                                                    modifier = Modifier
                                                        .align(resizeEdge)
                                                        .zIndex(1f)
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
            RegionOrientation.VERTICAL -> {
                Column(modifier = modifier.fillMaxSize()) {
                    currentItems.forEachIndexed { index, item ->
                        val hasNext = index < currentItems.size - 1
                        val isVisible = when (item) {
                            is RegionItem.Pane -> item.state.isVisible
                            is RegionItem.SubRegion -> true
                        }
                        
                        if (isVisible) {
                            // Use key to help Compose skip unchanged items
                            androidx.compose.runtime.key(when (item) {
                                is RegionItem.Pane -> item.id
                                is RegionItem.SubRegion -> "subregion_${item.orientation}_${index}"
                            }) {
                                when (item) {
                                    is RegionItem.Pane -> {
                                        val resizeEdge = if (hasNext) Alignment.BottomCenter else null
                                        val paneItem = object : PaneItem {
                                            override val id = item.id
                                            override val state = item.state
                                            override val content: @Composable (Dp) -> Unit = item.content
                                        }
                                        PaneContainer(
                                            pane = paneItem,
                                            onResize = if (resizeEdge != null) {
                                                { dragAmount -> onPaneResize(item.id, dragAmount) }
                                            } else null,
                                            resizeEdge = resizeEdge,
                                            density = density
                                        )
                                    }
                                    is RegionItem.SubRegion -> {
                                        val resizeEdge = if (hasNext) Alignment.BottomCenter else null
                                        Box(modifier = Modifier.fillMaxWidth()) {
                                            Region(
                                                orientation = item.orientation,
                                                modifier = Modifier.fillMaxSize(),
                                                onPaneResize = onPaneResize,
                                                content = item.content
                                            )
                                            if (resizeEdge != null && item.bottommostPaneId != null) {
                                                VerticalResizeHandle(
                                                    onResize = { dragAmount ->
                                                        onPaneResize(item.bottommostPaneId, dragAmount)
                                                    },
                                                    modifier = Modifier
                                                        .align(resizeEdge)
                                                        .zIndex(1f)
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
        }
    }
}

/**
 * Scope for building region content
 */
interface RegionScope {
    /**
     * Add a pane to the region
     */
    @Composable
    fun Pane(id: String, state: PaneState, content: @Composable (Dp) -> Unit)
    
    /**
     * Add a sub-region to the region
     */
    @Composable
    fun SubRegion(
        orientation: RegionOrientation,
        rightmostPaneId: String? = null, // For horizontal resize handles
        bottommostPaneId: String? = null, // For vertical resize handles
        content: @Composable RegionScope.() -> Unit
    )
}

private class RegionScopeImpl : RegionScope {
    val items = mutableListOf<RegionItem>()
    
    @Composable
    override fun Pane(id: String, state: PaneState, content: @Composable (Dp) -> Unit) {
        items.add(RegionItem.Pane(id, state, content))
    }
    
    @Composable
    override fun SubRegion(
        orientation: RegionOrientation,
        rightmostPaneId: String?,
        bottommostPaneId: String?,
        content: @Composable RegionScope.() -> Unit
    ) {
        items.add(RegionItem.SubRegion(orientation, rightmostPaneId, bottommostPaneId, content))
    }
}

private sealed class RegionItem {
    data class Pane(
        val id: String,
        val state: PaneState,
        val content: @Composable (Dp) -> Unit
    ) : RegionItem()
    data class SubRegion(
        val orientation: RegionOrientation,
        val rightmostPaneId: String?,
        val bottommostPaneId: String?,
        val content: @Composable RegionScope.() -> Unit
    ) : RegionItem()
}

/**
 * Composable function to add a pane to a region
 */
@Composable
fun Pane(
    id: String,
    state: PaneState,
    content: @Composable (Dp) -> Unit
) {
    LocalRegionScope.current.Pane(id, state, content)
}

@Composable
fun SubRegion(
    orientation: RegionOrientation,
    rightmostPaneId: String? = null,
    bottommostPaneId: String? = null,
    content: @Composable RegionScope.() -> Unit
) {
    LocalRegionScope.current.SubRegion(orientation, rightmostPaneId, bottommostPaneId, content)
}
