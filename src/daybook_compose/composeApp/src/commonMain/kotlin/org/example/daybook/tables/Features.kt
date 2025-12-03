@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.runtime.Composable
import androidx.compose.runtime.rememberCoroutineScope
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.AppScreens

/**
 * Common data source for all feature menus across different layouts.
 * Returns a list of FeatureItem that can be used in:
 * - Compact bottom sheet features menu
 * - Center nav bar features rollout
 * - Sidebar (expanded/medium)
 * - Top app bar dropdown menu (expanded/medium)
 */
@Composable
fun rememberFeatures(navController: NavHostController): List<FeatureItem> {
    val scope = rememberCoroutineScope()
    
    return listOf(
        FeatureItem("nav_home", "H", "Home") {
            scope.launch {
                navController.navigate(AppScreens.Home.name)
            }
        },
        FeatureItem("nav_tables", "T", "Tables") {
            scope.launch {
                navController.navigate(AppScreens.Tables.name)
            }
        },
        FeatureItem("nav_capture", "＋", "Capture") {
            scope.launch {
                navController.navigate(AppScreens.Capture.name)
            }
        },
        FeatureItem("nav_settings", "⚙️", "Settings") {
            scope.launch {
                navController.navigate(AppScreens.Settings.name)
            }
        },
    )
}
