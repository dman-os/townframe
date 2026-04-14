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
import androidx.navigation.NavHostController
import org.example.daybook.AppScreens
import org.example.daybook.capture.CaptureNavActions

fun routeForFeatureKey(featureKey: String): String? =
    when (featureKey) {
        FeatureKeys.Home -> AppScreens.Home.name
        FeatureKeys.Capture -> AppScreens.Capture.name
        FeatureKeys.Drawer -> AppScreens.Drawer.name
        FeatureKeys.Progress -> AppScreens.Progress.name
        FeatureKeys.Settings -> AppScreens.Settings.name
        else -> null
    }

/**
 * All available features. This is the master list.
 */
@Composable
fun rememberAllFeatures(navController: NavHostController): List<FeatureItem> {
    return listOf(
        FeatureItem(
            key = FeatureKeys.Home,
            icon = { Icon(Icons.Default.Home, contentDescription = "Home") },
            selectedIcon = { Icon(Icons.Default.Home, contentDescription = "Home") },
            label = "Home",
            onActivate = { navController.navigate(AppScreens.Home.name) },
            onReselect = { navController.navigate(AppScreens.Home.name) }
        ),
        FeatureItem(
            key = FeatureKeys.Capture,
            icon = { Icon(Icons.Default.EditNote, contentDescription = "Capture") },
            selectedIcon = { Icon(Icons.Default.EditNote, contentDescription = "Capture") },
            label = "Capture",
            onActivate = { navController.navigate(AppScreens.Capture.name) },
            onReselect = { CaptureNavActions.requestModeCycle() }
        ),
        FeatureItem(
            key = FeatureKeys.Drawer,
            icon = { Icon(Icons.AutoMirrored.Filled.LibraryBooks, contentDescription = "Drawer") },
            selectedIcon = { Icon(Icons.AutoMirrored.Filled.LibraryBooks, contentDescription = "Drawer") },
            label = "Drawer",
            onActivate = { navController.navigate(AppScreens.Drawer.name) },
            onReselect = { navController.navigate(AppScreens.Drawer.name) }
        ),
        FeatureItem(
            key = FeatureKeys.Progress,
            icon = { Icon(Icons.Default.Notifications, contentDescription = "Progress") },
            selectedIcon = { Icon(Icons.Default.Notifications, contentDescription = "Progress") },
            label = "Progress",
            onActivate = { navController.navigate(AppScreens.Progress.name) },
            onReselect = { navController.navigate(AppScreens.Progress.name) }
        )
    )
}

/**
 * Features that appear in the bottom nav bar (compact).
 * For compact layout: show Home, Capture, Documents in that order in the bottom bar.
 */
@Composable
fun rememberNavBarFeatures(navController: NavHostController): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navController)

    // Return Home, Capture, Documents in that order
    return listOfNotNull(
        allFeatures.find { it.key == FeatureKeys.Home },
        allFeatures.find { it.key == FeatureKeys.Capture },
        allFeatures.find { it.key == FeatureKeys.Drawer }
    )
}

/**
 * Features that appear in the sidebar (expanded layout).
 * For sidebar: show only Home and Documents.
 */
@Composable
fun rememberSidebarFeatures(navController: NavHostController): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navController)

    // Return only Home and Documents
    return listOfNotNull(
        allFeatures.find { it.key == FeatureKeys.Home },
        allFeatures.find { it.key == FeatureKeys.Drawer }
    )
}

/**
 * Features that appear in the menu/dropdown (hidden by default).
 * These are secondary features that are accessed via menu.
 * For compact layout: everything except Home, Capture, Documents goes in the menu.
 */
@Composable
fun rememberMenuFeatures(
    navController: NavHostController,
    onShowCloneShare: () -> Unit = {}
): List<FeatureItem> {
    val allFeatures = rememberAllFeatures(navController)

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
                onReselect = { onShowCloneShare() }
            ),
            FeatureItem(
                key = FeatureKeys.Settings,
                icon = { Icon(Icons.Default.Settings, contentDescription = "Settings") },
                selectedIcon = { Icon(Icons.Default.Settings, contentDescription = "Settings") },
                label = "Settings",
                onActivate = { navController.navigate(AppScreens.Settings.name) },
                onReselect = { navController.navigate(AppScreens.Settings.name) }
            )
        )
}

/**
 * @deprecated Use rememberNavBarFeatures, rememberMenuFeatures, or rememberSidebarFeatures instead.
 * This function is kept for backward compatibility but returns all features.
 */
@Composable
@Deprecated("Use rememberNavBarFeatures, rememberMenuFeatures, or rememberSidebarFeatures instead")
fun rememberFeatures(navController: NavHostController): List<FeatureItem> =
    rememberAllFeatures(navController)
