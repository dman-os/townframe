@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

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
        FeatureItem("nav_home", "H", "Home") {
            scope.launch {
                navController.navigate(AppScreens.Home.name)
            }
        },
        FeatureItem("nav_capture", "＋", "Capture") {
            scope.launch {
                navController.navigate(AppScreens.Capture.name)
            }
        },
        FeatureItem("nav_documents", "📄", "Documents") {
            scope.launch {
                navController.navigate(AppScreens.Documents.name)
            }
        },
        FeatureItem("nav_tables", "T", "Tables") {
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
        allFeatures.find { it.key == "nav_home" },
        allFeatures.find { it.key == "nav_capture" },
        allFeatures.find { it.key == "nav_documents" }
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
        allFeatures.find { it.key == "nav_home" },
        allFeatures.find { it.key == "nav_documents" }
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
    val bottomBarKeys = setOf("nav_home", "nav_capture", "nav_documents")

    // Return all features except those in the bottom bar, plus Settings
    val otherFeatures = allFeatures.filter { it.key !in bottomBarKeys }

    return otherFeatures +
        listOf(
            FeatureItem("nav_settings", "⚙️", "Settings") {
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
