@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.dockable

import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

/**
 * Represents a dockable pane that can be positioned and resized.
 */
data class PaneState(
    val id: String,
    var width: Float = 0f, // in dp
    var height: Float = 0f, // in dp
    var isVisible: Boolean = true,
    var minWidth: Float = 60f,
    var maxWidth: Float = 400f,
    var minHeight: Float = 60f,
    var maxHeight: Float = 400f
) {
    fun widthDp(density: Density): Dp {
        return with(density) {
            width.dp
        }
    }
    fun heightDp(density: Density): Dp {
        return with(density) {
            height.dp
        }
    }
}

/**
 * Orientation for arranging panes in a region
 */
enum class RegionOrientation {
    HORIZONTAL, // Left to right
    VERTICAL    // Top to bottom
}

/**
 * Interface for a pane item - used internally by PaneContainer
 */
interface PaneItem {
    val id: String
    val state: PaneState
    val content: @Composable (Dp) -> Unit
}

/**
 * Generic container for a single pane with resize handle
 */
@Composable
fun PaneContainer(
    pane: PaneItem,
    modifier: Modifier = Modifier,
    onResize: ((Float) -> Unit)? = null, // Called when resized, receives drag amount in pixels
    resizeEdge: Alignment? = null, // Which edge has the resize handle (null = no resize)
    density: Density = LocalDensity.current
) {
    if (!pane.state.isVisible) {
        return
    }
    
    // Use mutableStateOf to track changes and trigger recomposition
    // We need to observe the pane state, but since it's not a state object,
    // we'll update our local state when the resize callback is called
    val widthState = remember(pane.id) { mutableStateOf(pane.state.width) }
    val heightState = remember(pane.id) { mutableStateOf(pane.state.height) }
    
    // Create a wrapper for onResize that also updates our local state
    val wrappedOnResize = onResize?.let { originalOnResize ->
        { dragAmount: Float ->
            originalOnResize(dragAmount)
            // Update local state to trigger recomposition
            if (pane.state.width > 0) {
                widthState.value = pane.state.width
            } else {
                heightState.value = pane.state.height
            }
        }
    }
    
    val size = if (widthState.value > 0) {
        with(density) { widthState.value.dp }
    } else {
        with(density) { heightState.value.dp }
    }
    
    Box(modifier = modifier) {
        // Pane content
        Box(
            modifier = Modifier
                .then(
                    if (widthState.value > 0) {
                        Modifier.width(size).fillMaxHeight()
                    } else {
                        Modifier.height(size).fillMaxWidth()
                    }
                )
        ) {
            pane.content(size)
        }
        
        // Resize handle if configured
        if (wrappedOnResize != null && resizeEdge != null) {
            when {
                resizeEdge == Alignment.CenterEnd || resizeEdge == Alignment.CenterStart -> {
                    // Horizontal resize handle
                    HorizontalResizeHandle(
                        onResize = wrappedOnResize,
                        modifier = Modifier.align(resizeEdge)
                    )
                }
                resizeEdge == Alignment.BottomCenter || resizeEdge == Alignment.TopCenter -> {
                    // Vertical resize handle
                    VerticalResizeHandle(
                        onResize = wrappedOnResize,
                        modifier = Modifier.align(resizeEdge)
                    )
                }
            }
        }
    }
}

/**
 * Resize handle for horizontal resizing (left/right edges)
 */
@Composable
fun HorizontalResizeHandle(
    onResize: (Float) -> Unit, // dragAmount in pixels
    modifier: Modifier = Modifier,
    edge: Alignment.Horizontal = Alignment.CenterHorizontally
) {
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    val density = LocalDensity.current
    
    Box(
        modifier = modifier
            .width(8.dp)
            .fillMaxHeight()
            .background(
                if (isHovered) {
                    MaterialTheme.colorScheme.primary.copy(alpha = 0.3f)
                } else {
                    MaterialTheme.colorScheme.outline.copy(alpha = 0.1f)
                }
            )
            .hoverable(interactionSource)
            .pointerInput(Unit) {
                detectHorizontalDragGestures { change, dragAmount ->
                    onResize(dragAmount)
                    change.consume()
                }
            }
    )
}

/**
 * Resize handle for vertical resizing (top/bottom edges)
 */
@Composable
fun VerticalResizeHandle(
    onResize: (Float) -> Unit, // dragAmount in pixels
    modifier: Modifier = Modifier,
    edge: Alignment.Vertical = Alignment.CenterVertically
) {
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    val density = LocalDensity.current
    
    Box(
        modifier = modifier
            .height(8.dp)
            .fillMaxWidth()
            .background(
                if (isHovered) {
                    MaterialTheme.colorScheme.primary.copy(alpha = 0.3f)
                } else {
                    MaterialTheme.colorScheme.outline.copy(alpha = 0.1f)
                }
            )
            .hoverable(interactionSource)
            .pointerInput(Unit) {
                detectVerticalDragGestures { change, dragAmount ->
                    onResize(dragAmount)
                    change.consume()
                }
            }
    )
}

/**
 * Edge drag zone for opening closed panes.
 * When a pane is hidden, this creates a thin drag zone at the screen edge.
 * Dragging from this zone will open the pane.
 */
@Composable
fun EdgeDragZone(
    onDrag: (Float) -> Unit, // dragAmount in pixels (positive = opening)
    onDragEnd: () -> Unit,
    modifier: Modifier = Modifier,
    isHorizontal: Boolean = true, // true for left/right edges, false for top/bottom
    edgeWidth: Dp = 8.dp
) {
    val density = LocalDensity.current
    var dragAmount by remember { mutableFloatStateOf(0f) }
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    
    Box(
        modifier = modifier
            .then(
                if (isHorizontal) {
                    Modifier.width(edgeWidth).fillMaxHeight()
                } else {
                    Modifier.height(edgeWidth).fillMaxWidth()
                }
            )
            .background(
                if (isHovered) {
                    MaterialTheme.colorScheme.primary.copy(alpha = 0.3f)
                } else {
                    MaterialTheme.colorScheme.outline.copy(alpha = 0.15f)
                }
            )
            .hoverable(interactionSource)
            .pointerInput(Unit) {
                if (isHorizontal) {
                    detectHorizontalDragGestures(
                        onHorizontalDrag = { change, dragDelta ->
                            dragAmount += dragDelta
                            onDrag(dragDelta)
                        },
                        onDragEnd = {
                            onDragEnd()
                            dragAmount = 0f
                        },
                        onDragCancel = {
                            onDragEnd()
                            dragAmount = 0f
                        }
                    )
                } else {
                    detectVerticalDragGestures(
                        onVerticalDrag = { change, dragDelta ->
                            dragAmount += dragDelta
                            onDrag(dragDelta)
                        },
                        onDragEnd = {
                            onDragEnd()
                            dragAmount = 0f
                        },
                        onDragCancel = {
                            onDragEnd()
                            dragAmount = 0f
                        }
                    )
                }
            }
    )
}
