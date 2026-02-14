@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.LibraryBooks
import androidx.compose.material.icons.filled.CameraAlt
import androidx.compose.material.icons.filled.Home
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material.icons.filled.TableChart
import androidx.compose.material3.Icon
import androidx.compose.runtime.Composable
import androidx.compose.runtime.rememberCoroutineScope
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.AppScreens

/**
 * All available features. This is the master list.
 */
@Composable
fun rememberAllFeatures(navController: NavHostController): List<FeatureItem> {
    val scope = rememberCoroutineScope()

    return listOf(
        FeatureItem(FeatureKeys.Home, { Icon(Icons.Default.Home, contentDescription = "Home") }, "Home") {
            scope.launch {
                navController.navigate(AppScreens.Home.name)
            }
        },
        FeatureItem(FeatureKeys.Capture, { Icon(Icons.Default.CameraAlt, contentDescription = "Capture") }, "Capture") {
            scope.launch {
                navController.navigate(AppScreens.Capture.name)
            }
        },
        FeatureItem(FeatureKeys.Drawer, { Icon(Icons.AutoMirrored.Filled.LibraryBooks, contentDescription = "Drawer") }, "Drawer") {
            scope.launch {
                navController.navigate(AppScreens.Drawer.name)
            }
        },
        FeatureItem(FeatureKeys.Tables, { Icon(Icons.Default.TableChart, contentDescription = "Tables") }, "Tables") {
            scope.launch {
                navController.navigate(AppScreens.Tables.name)
            }
        }
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
fun rememberMenuFeatures(navController: NavHostController): List<FeatureItem> {
    val scope = rememberCoroutineScope()
    val allFeatures = rememberAllFeatures(navController)

    // Get the bottom bar features (Home, Capture, Documents)
    val bottomBarKeys = setOf(FeatureKeys.Home, FeatureKeys.Capture, FeatureKeys.Drawer)

    // Return all features except those in the bottom bar, plus Settings
    val otherFeatures = allFeatures.filter { it.key !in bottomBarKeys }

    return otherFeatures +
        listOf(
            FeatureItem(FeatureKeys.Settings, { Icon(Icons.Default.Settings, contentDescription = "Settings") }, "Settings") {
                scope.launch {
                    navController.navigate(AppScreens.Settings.name)
                }
            }
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
