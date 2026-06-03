@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.LibraryBooks
import androidx.compose.material.icons.filled.EditNote
import androidx.compose.material.icons.filled.Home
import androidx.compose.material.icons.filled.Notifications
import androidx.compose.material.icons.filled.QrCode2
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Icon
import androidx.compose.runtime.Composable
import org.example.daybook.capture.CaptureNavActions
import org.example.daybook.navigation.DaybookNavKey
import org.example.daybook.navigation.DaybookNavigationState

fun destinationForFeatureKey(featureKey: String): DaybookNavKey? = when (featureKey) {
    FeatureKeys.Home -> DaybookNavKey.Home
    FeatureKeys.Capture -> DaybookNavKey.Capture
    FeatureKeys.Drawer -> DaybookNavKey.Drawer
    FeatureKeys.Progress -> DaybookNavKey.Progress
    FeatureKeys.Settings -> DaybookNavKey.Settings
    else -> null
}

/**
 * All available features. This is the master list.
 */
@Composable
fun rememberAllFeatures(navState: DaybookNavigationState): List<FeatureItem> = listOf(
    FeatureItem(
        key = FeatureKeys.Home,
        icon = { Icon(Icons.Default.Home, contentDescription = "Home") },
        selectedIcon = { Icon(Icons.Default.Home, contentDescription = "Home") },
        label = "Home",
        onActivate = { navState.navigate(DaybookNavKey.Home) },
        onReselect = { navState.navigate(DaybookNavKey.Home) },
    ),
    FeatureItem(
        key = FeatureKeys.Capture,
        icon = { Icon(Icons.Default.EditNote, contentDescription = "Capture") },
        selectedIcon = { Icon(Icons.Default.EditNote, contentDescription = "Capture") },
        label = "Capture",
        onActivate = { navState.navigate(DaybookNavKey.Capture) },
        onReselect = { CaptureNavActions.requestModeCycle() },
    ),
    FeatureItem(
        key = FeatureKeys.Drawer,
        icon = { Icon(Icons.AutoMirrored.Filled.LibraryBooks, contentDescription = "Drawer") },
        selectedIcon = {
            Icon(Icons.AutoMirrored.Filled.LibraryBooks, contentDescription = "Drawer")
        },
        label = "Drawer",
        onActivate = { navState.navigate(DaybookNavKey.Drawer) },
        onReselect = { navState.navigate(DaybookNavKey.Drawer) },
    ),
    FeatureItem(
        key = FeatureKeys.Progress,
        icon = { Icon(Icons.Default.Notifications, contentDescription = "Progress") },
        selectedIcon = { Icon(Icons.Default.Notifications, contentDescription = "Progress") },
        label = "Progress",
        onActivate = { navState.navigate(DaybookNavKey.Progress) },
        onReselect = { navState.navigate(DaybookNavKey.Progress) },
    ),
)

/**
 * Features that appear in the bottom nav bar (compact).
 * For compact layout: show Home, Capture, Documents in that order in the bottom bar.
 */
@Composable
fun rememberNavBarFeatures(navState: DaybookNavigationState): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navState)

    // Return Home, Capture, Documents in that order
    return listOfNotNull(
        allFeatures.find { it.key == FeatureKeys.Home },
        allFeatures.find { it.key == FeatureKeys.Capture },
        allFeatures.find { it.key == FeatureKeys.Drawer },
    )
}

/**
 * Features that appear in the sidebar (expanded layout).
 * For sidebar: show Capture first, then Home and Documents.
 */
@Composable
fun rememberSidebarFeatures(navState: DaybookNavigationState): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navState)

    // Return Capture first so it becomes the primary sidebar action.
    return listOfNotNull(
        allFeatures.find { it.key == FeatureKeys.Capture },
        allFeatures.find { it.key == FeatureKeys.Home },
        allFeatures.find { it.key == FeatureKeys.Drawer },
    )
}

/**
 * Features that appear in the menu/dropdown (hidden by default).
 * These are secondary features that are accessed via menu.
 * For compact layout: everything except Home, Capture, Documents goes in the menu.
 */
@Composable
fun rememberMenuFeatures(navState: DaybookNavigationState, onShowCloneShare: () -> Unit = {}): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navState)

    // Get the bottom bar features (Home, Capture, Documents)
    val bottomBarKeys = setOf(FeatureKeys.Home, FeatureKeys.Capture, FeatureKeys.Drawer)

    // Return all features except those in the bottom bar, plus Settings
    val otherFeatures = allFeatures.filter { it.key !in bottomBarKeys }

    return otherFeatures +
        listOf(
            FeatureItem(
                key = FeatureKeys.CloneShare,
                icon = { Icon(Icons.Default.QrCode2, contentDescription = "Clone") },
                selectedIcon = { Icon(Icons.Default.QrCode2, contentDescription = "Clone") },
                label = "Clone",
                onActivate = { onShowCloneShare() },
                onReselect = { onShowCloneShare() },
            ),
            FeatureItem(
                key = FeatureKeys.Settings,
                icon = { Icon(Icons.Default.Settings, contentDescription = "Settings") },
                selectedIcon = { Icon(Icons.Default.Settings, contentDescription = "Settings") },
                label = "Settings",
                onActivate = { navState.navigate(DaybookNavKey.Settings) },
                onReselect = { navState.navigate(DaybookNavKey.Settings) },
            ),
        )
}
