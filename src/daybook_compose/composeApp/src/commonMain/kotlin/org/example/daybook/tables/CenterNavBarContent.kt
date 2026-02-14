@file:OptIn(
    kotlin.uuid.ExperimentalUuidApi::class,
    androidx.compose.material3.ExperimentalMaterial3Api::class
)

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
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
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
import org.example.daybook.ChromeState
import org.example.daybook.LocalChromeStateManager
import org.example.daybook.LocalContainer
import org.example.daybook.MainFeatureActionButton
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel

/**
 * Abstraction for center navigation bar content that adapts based on navigation state
 * and sheet visibility.
 */
@Composable
fun RowScope.CenterNavBarContent(
    navController: NavHostController,
    isMenuOpen: Boolean,
    showFeaturesMenu: Boolean,
    featureReadyStates: List<androidx.compose.runtime.State<Boolean>>,
    features: List<FeatureItem>,
    featureButtonLayouts: Map<String, Rect>,
    lastDragWindowPos: androidx.compose.ui.geometry.Offset?,
    onFeatureButtonLayout: (String, Rect) -> Unit,
    onFeatureActivate: suspend (FeatureItem) -> Unit,
    modifier: Modifier = Modifier
) {
    val scope = rememberCoroutineScope()

    // Get chrome state from manager
    val chromeStateManager = LocalChromeStateManager.current
    val chromeState by chromeStateManager.currentState.collectAsState()
    val mainFeatureActionButton = chromeState.mainFeatureActionButton
    val prominentButtons = chromeState.additionalFeatureButtons.filter { it.prominent }
    val isMenuOpenResolved = showFeaturesMenu || isMenuOpen

    if (prominentButtons.isNotEmpty() && (isMenuOpenResolved || mainFeatureActionButton == null)) {
        // Show prominent buttons when:
        // 1. Menu is open (supplanting main feature action button if it exists), OR
        // 2. No main feature action button exists (show prominent buttons always)
        Row(
            modifier =
                modifier
                    .weight(1f)
                    .padding(horizontal = 8.dp),
            horizontalArrangement = Arrangement.SpaceBetween
        ) {
            prominentButtons.forEachIndexed { idx, button ->
                // Get ready state for this prominent button
                val prominentButtonKey = button.key
                // Prominent buttons come after nav bar features in the ready states
                val readyState =
                    featureReadyStates
                        .getOrNull(
                            features.size + idx
                        )?.value ?: false
                val hoverOver =
                    lastDragWindowPos?.let { pw ->
                        featureButtonLayouts[prominentButtonKey]?.contains(pw)
                    } ?: false

                NavigationBarItem(
                    onClick = {
                        if (button.enabled) {
                            scope.launch {
                                button.onClick()
                            }
                        }
                    },
                    modifier =
                        Modifier
                            .weight(1f)
                            .onGloballyPositioned { layoutCoordinates ->
                                onFeatureButtonLayout(
                                    button.key,
                                    layoutCoordinates.boundsInWindow()
                                )
                            },
                    icon = { button.icon() },
                    label = { button.label() },
                    selected = hoverOver || readyState,
                    enabled = button.enabled
                )
            }
        }
    } else if (mainFeatureActionButton != null && !isMenuOpenResolved) {
        // Show button from ChromeState (only when menu is not open and no prominent buttons to show)
        val button = mainFeatureActionButton as MainFeatureActionButton.Button
        Button(
            onClick = {
                scope.launch {
                    button.onClick()
                }
            },
            modifier = modifier.weight(1f),
            enabled = button.enabled
        ) {
            Row(
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                button.icon()
                button.label()
            }
        }
    } else {
        // Show default nav bar features (Home, Capture, Documents) in the center
        Row(
            modifier =
                Modifier
                    .weight(1f)
                    .padding(horizontal = 8.dp),
            horizontalArrangement = Arrangement.SpaceBetween
        ) {
            features.forEachIndexed { idx, feature ->
                val hoverOver =
                    lastDragWindowPos?.let { pw -> featureButtonLayouts[feature.key]?.contains(pw) }
                        ?: false
                val ready = featureReadyStates.getOrNull(idx)?.value ?: false

                NavigationBarItem(
                    onClick = {
                        scope.launch {
                            onFeatureActivate(feature)
                        }
                    },
                    modifier =
                        Modifier
                            .weight(1f)
                            .onGloballyPositioned { layoutCoordinates ->
                                onFeatureButtonLayout(
                                    feature.key,
                                    layoutCoordinates.boundsInWindow()
                                )
                            },
                    icon = {
                        feature.icon()
                    },
                    label = {
                        Text(feature.label, style = MaterialTheme.typography.labelSmall)
                    },
                    selected = hoverOver || ready
                )
            }
        }
    }
}
