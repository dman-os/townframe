@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.width
import androidx.compose.ui.Alignment
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.PermanentDrawerSheet
import androidx.compose.material3.PermanentNavigationDrawer
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ResizablePermanentNavigationDrawer(
    drawerContent: @Composable () -> Unit,
    modifier: Modifier = Modifier,
    initialWidth: Dp = 280.dp,
    minWidth: Dp = 200.dp,
    maxWidth: Dp = 600.dp,
    content: @Composable () -> Unit
) {
    val density = LocalDensity.current
    var drawerWidth by rememberSaveable { mutableFloatStateOf(initialWidth.value) }
    val drawerWidthDp = with(density) { drawerWidth.dp }
    
    PermanentNavigationDrawer(
        drawerContent = {
            Box(modifier = Modifier.fillMaxHeight()) {
                PermanentDrawerSheet(
                    modifier = Modifier.width(drawerWidthDp)
                ) {
                    drawerContent()
                }
                
                // Resize handle on the right edge
                val interactionSource = remember { MutableInteractionSource() }
                val isHovered by interactionSource.collectIsHoveredAsState()
                
                Box(
                    modifier = Modifier
                        .width(8.dp)
                        .fillMaxHeight()
                        .align(Alignment.CenterEnd)
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
                                val newWidth = (drawerWidth + dragAmount / density.density)
                                    .coerceIn(minWidth.value, maxWidth.value)
                                drawerWidth = newWidth
                                change.consume()
                            }
                        }
                )
            }
        },
        modifier = modifier
    ) {
        Box(modifier = Modifier.fillMaxSize()) {
            content()
        }
    }
}
