@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.width
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.IconButton
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.core.SidebarMode

/**
 * Sidebar component displaying navigation features.
 * Supports three modes: Hidden, Compact, and Expanded.
 * - Hidden: Sidebar is not visible
 * - Compact: Only feature buttons visible, scrollable content hidden
 *   - Hovering in the scrollable content area expands it temporarily
 * - Expanded: Both feature buttons and scrollable content visible
 */
@Composable
fun Sidebar(
    navController: NavHostController,
    onToggle: (() -> Unit)? = null,
    showToggleButton: Boolean = false,
    scrollableContent: (@Composable () -> Unit)? = null,
    width: Dp = 80.dp,
    mode: SidebarMode = SidebarMode.COMPACT,
    autoHide: Boolean = false
) {
    val features = rememberFeatures(navController)
    val scope = rememberCoroutineScope()
    
    // Local hover state for auto-expand in compact mode
    var isHoveringScrollArea by remember { mutableStateOf(false) }
    val scrollAreaInteractionSource = remember { MutableInteractionSource() }
    val isHoveringScrollAreaState by scrollAreaInteractionSource.collectIsHoveredAsState()
    
    // Determine effective mode: if auto-hide is enabled and we're in compact mode, check hover
    val effectiveMode = when {
        mode == SidebarMode.HIDDEN -> SidebarMode.HIDDEN
        mode == SidebarMode.COMPACT && autoHide && isHoveringScrollAreaState -> SidebarMode.EXPANDED
        else -> mode
    }
    
    // Update hover state
    isHoveringScrollArea = isHoveringScrollAreaState
    
    if (effectiveMode == SidebarMode.HIDDEN) {
        return
    }
    
    val isWideEnough = width >= 200.dp // Threshold for using NavigationDrawerItem vs NavigationRailItem
    val showScrollableContent = effectiveMode == SidebarMode.EXPANDED && scrollableContent != null
    
    Column(
        modifier = Modifier.width(width).fillMaxHeight()
    ) {
        // Feature buttons - use NavigationDrawerItem when wide enough, otherwise NavigationRailItem
        if (isWideEnough) {
            // Use NavigationDrawerItem for wider sidebars
            features.forEach { feature ->
                NavigationDrawerItem(
                    selected = false,
                    onClick = {
                        scope.launch {
                            feature.onActivate()
                        }
                    },
                    icon = {
                        androidx.compose.material3.Text(feature.icon)
                    },
                    label = { androidx.compose.material3.Text(feature.label) }
                )
            }
        } else {
            // Use NavigationRail for narrow sidebars
            NavigationRail(
                modifier = Modifier.width(width)
            ) {
                features.forEach { feature ->
                    NavigationRailItem(
                        selected = false,
                        onClick = {
                            scope.launch {
                                feature.onActivate()
                            }
                        },
                        icon = {
                            androidx.compose.material3.Text(feature.icon)
                        },
                        label = { androidx.compose.material3.Text(feature.label) }
                    )
                }
            }
        }
        
        // Scrollable content area with hover detection
        if (scrollableContent != null) {
            Box(
                modifier = Modifier
                    .weight(1f)
                    .fillMaxHeight()
                    .hoverable(scrollAreaInteractionSource)
            ) {
                // Show content only in expanded mode
                if (showScrollableContent) {
                    Column(modifier = Modifier.fillMaxHeight()) {
                        HorizontalDivider()
                        scrollableContent()
                    }
                } else if (mode == SidebarMode.COMPACT && autoHide) {
                    // In compact mode with auto-hide, show empty space that expands on hover
                    // Empty space that triggers hover
                    Spacer(modifier = Modifier.fillMaxHeight())
                }
            }
        } else {
            // Toggle button at the bottom (only if no scrollable content)
            if (showToggleButton && onToggle != null) {
                Spacer(modifier = Modifier.weight(1f))
                IconButton(onClick = onToggle) {
                    androidx.compose.material3.Text("◀")
                }
            }
        }
    }
}
