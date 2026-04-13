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
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.unit.dp
import androidx.navigation.NavHostController
import androidx.navigation.compose.currentBackStackEntryAsState
import kotlinx.coroutines.launch
import org.example.daybook.AppScreens
import org.example.daybook.LocalChromeStateManager
import org.example.daybook.MainFeatureActionButton

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
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentRoute = navBackStackEntry?.destination?.route
    val armedIndicatorColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.95f)
    val hoverFill = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f)
    val selectedFill = MaterialTheme.colorScheme.primary.copy(alpha = 0.24f)

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

                val selected = isFeatureRouteSelected(button.key, currentRoute)
                CustomBottomBarItem(
                    modifier =
                        Modifier
                            .padding(vertical = 3.dp)
                            .onGloballyPositioned { layoutCoordinates ->
                                onFeatureButtonLayout(
                                    button.key,
                                    layoutCoordinates.boundsInWindow()
                                )
                            },
                    selected = selected,
                    hover = hoverOver,
                    armed = hoverOver && readyState,
                    enabled = button.enabled,
                    hoverFill = hoverFill,
                    selectedFill = selectedFill,
                    armedIndicatorColor = armedIndicatorColor,
                    icon = button.icon,
                    label = button.label,
                    onClick = {
                        if (button.enabled) {
                            scope.launch { button.onClick() }
                        }
                    }
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

                val selected = isFeatureRouteSelected(feature.key, currentRoute)
                CustomBottomBarItem(
                    modifier =
                        Modifier
                            .padding(vertical = 3.dp)
                            .onGloballyPositioned { layoutCoordinates ->
                                onFeatureButtonLayout(
                                    feature.key,
                                    layoutCoordinates.boundsInWindow()
                                )
                            },
                    selected = selected,
                    hover = hoverOver,
                    armed = hoverOver && ready,
                    enabled = feature.enabled,
                    hoverFill = hoverFill,
                    selectedFill = selectedFill,
                    armedIndicatorColor = armedIndicatorColor,
                    icon = if (selected) (feature.selectedIcon ?: feature.icon) else feature.icon,
                    label = { Text(feature.label, style = MaterialTheme.typography.labelSmall) },
                    onClick = {
                        scope.launch {
                            if (selected) {
                                (feature.onReselect ?: { onFeatureActivate(feature) }).invoke()
                            } else {
                                onFeatureActivate(feature)
                            }
                        }
                    }
                )
            }
        }
    }
}

@Composable
private fun CustomBottomBarItem(
    selected: Boolean,
    hover: Boolean,
    armed: Boolean,
    enabled: Boolean,
    hoverFill: androidx.compose.ui.graphics.Color,
    selectedFill: androidx.compose.ui.graphics.Color,
    armedIndicatorColor: androidx.compose.ui.graphics.Color,
    icon: @Composable () -> Unit,
    label: @Composable () -> Unit,
    onClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    val itemShape = RoundedCornerShape(20.dp)
    val outerInteraction = remember { MutableInteractionSource() }
    val background =
        when {
            selected -> selectedFill
            hover -> hoverFill
            else -> androidx.compose.ui.graphics.Color.Transparent
        }

    Box(
        modifier =
            modifier
                .clickable(
                    enabled = enabled,
                    interactionSource = outerInteraction,
                    indication = null,
                    onClick = onClick
                )
                .padding(vertical = 2.dp),
        contentAlignment = Alignment.Center
    ) {
        Column(
            modifier = Modifier.padding(vertical = 2.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Box(
                modifier =
                    Modifier
                        .width(56.dp)
                        .clip(itemShape)
                        .background(background)
                        .then(
                            if (armed) {
                                Modifier.border(width = 1.5.dp, color = armedIndicatorColor, shape = itemShape)
                            } else {
                                Modifier
                            }
                        )
                        .padding(horizontal = 10.dp, vertical = 6.dp),
                contentAlignment = Alignment.Center
            ) {
                Box { icon() }
            }
            Box { label() }
        }
    }
}

private fun isFeatureRouteSelected(featureKey: String, currentRoute: String?): Boolean {
    val targetRoute =
        when (featureKey) {
            FeatureKeys.Home -> AppScreens.Home.name
            FeatureKeys.Capture -> AppScreens.Capture.name
            FeatureKeys.Drawer -> AppScreens.Drawer.name
            FeatureKeys.Tables -> AppScreens.Tables.name
            FeatureKeys.Progress -> AppScreens.Progress.name
            FeatureKeys.Settings -> AppScreens.Settings.name
            else -> null
        }
    return targetRoute != null && targetRoute == currentRoute
}
