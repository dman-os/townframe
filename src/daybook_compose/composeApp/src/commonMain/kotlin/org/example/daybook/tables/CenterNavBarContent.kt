@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.AppScreens
import org.example.daybook.LocalContainer
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.capture.LocalCameraCaptureContext

/**
 * Abstraction for center navigation bar content that adapts based on navigation state
 * and sheet visibility.
 */
@Composable
fun RowScope.CenterNavBarContent(
    navController: NavHostController,
    revealSheetState: RevealBottomSheetState,
    showFeaturesMenu: Boolean,
    addTabReadyState: androidx.compose.runtime.State<Boolean>,
    addTableReadyState: androidx.compose.runtime.State<Boolean>,
    featureReadyStates: List<androidx.compose.runtime.State<Boolean>>,
    features: List<FeatureItem>,
    featureButtonLayouts: Map<String, Rect>,
    lastDragWindowPos: androidx.compose.ui.geometry.Offset?,
    onAddButtonLayout: (Rect) -> Unit,
    onFeatureButtonLayout: (String, Rect) -> Unit,
    onAddTab: suspend () -> Unit,
    onFeatureActivate: suspend (FeatureItem) -> Unit,
    modifier: Modifier = Modifier,
) {
    val currentRoute = navController.currentBackStackEntry?.destination?.route
    val isOnCaptureScreen = currentRoute == AppScreens.Capture.name
    val captureContext = LocalCameraCaptureContext.current
    val scope = rememberCoroutineScope()
    
    // When sheet is open, show controls (add button). When closed, show current tab title or camera controls.
    if (revealSheetState.isVisible) {
        // Add-tab button expands to fill the center area
        Button(
            onClick = {
                scope.launch {
                    onAddTab()
                }
            },
            modifier = modifier
                .fillMaxWidth()
                .weight(1f)
                .onGloballyPositioned { layoutCoordinates ->
                    val r = layoutCoordinates.boundsInWindow()
                    if (r.width > 0f && r.height > 0f) {
                        onAddButtonLayout(r)
                    }
                },
            colors = if (addTabReadyState.value) ButtonDefaults.filledTonalButtonColors() else ButtonDefaults.buttonColors()
        ) {
            if (addTabReadyState.value) Text("Release to Add") else Text("Add Tab")
        }
    } else if (showFeaturesMenu) {
        // rollout toolbar: fill the center area with nav-style buttons
        AnimatedVisibility(
            visible = showFeaturesMenu,
            enter = fadeIn(animationSpec = tween(220)) + slideInHorizontally(
                initialOffsetX = { it / 4 },
                animationSpec = tween(220)
            ),
            exit = fadeOut(animationSpec = tween(160)) + slideOutHorizontally(
                targetOffsetX = { it / 4 },
                animationSpec = tween(160)
            )
        ) {
            Row(
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                val btnModifier = Modifier.weight(1f).height(48.dp)

                // Render features from the top-level `features` list and minimize the rollout on press
                features.forEachIndexed { idx, feature ->
                    val key = feature.key
                    val iconText = feature.icon
                    val labelText = feature.label

                    // Highlight when pointer (during drag) is over the button rect, or controller reports ready
                    val hoverOver =
                        lastDragWindowPos?.let { pw -> featureButtonLayouts[key]?.contains(pw) }
                            ?: false
                    val ready = featureReadyStates.getOrNull(idx)?.value ?: false

                    NavigationBarItem(
                        onClick = {
                            scope.launch {
                                onFeatureActivate(feature)
                            }
                        },
                        modifier = btnModifier.onGloballyPositioned { layoutCoordinates ->
                            onFeatureButtonLayout(key, layoutCoordinates.boundsInWindow())
                        },
                        icon = {
                            Text(iconText, style = MaterialTheme.typography.bodyLarge)
                        },
                        label = {
                            Text(labelText, style = MaterialTheme.typography.labelSmall)
                        },
                        selected = hoverOver || ready,
                    )
                }
            }
        }
    } else if (isOnCaptureScreen && captureContext != null) {
        // Show camera capture button when on capture screen
        val canCapture by captureContext.canCapture.collectAsState()
        val isCapturing by captureContext.isCapturing.collectAsState()
        
        Button(
            onClick = {
                captureContext.requestCapture()
            },
            modifier = modifier.weight(1f),
            enabled = canCapture && !isCapturing
        ) {
            Text(if (isCapturing) "Capturing..." else "Save Photo")
        }
    } else {
        // Default: show current tab title
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

        Box(modifier = modifier.weight(1f), contentAlignment = Alignment.Center) {
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
