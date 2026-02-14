@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.runtime.Composable
import org.example.daybook.AdditionalFeatureButton

object FeatureKeys {
    const val Home = "nav_home"
    const val Capture = "nav_capture"
    const val Drawer = "nav_drawer"
    const val Tables = "nav_tables"
    const val Settings = "nav_settings"
}

data class FeatureItem(
    val key: String,
    val icon: @Composable () -> Unit,
    val label: String,
    val labelContent: (@Composable () -> Unit)? = null,
    val enabled: Boolean = true,
    val onActivate: suspend () -> Unit
)

fun AdditionalFeatureButton.toFeatureItem(): FeatureItem =
    FeatureItem(
        key = key,
        icon = { icon() },
        label = "",
        labelContent = { label() },
        enabled = enabled,
        onActivate = { onClick() }
    )

fun List<FeatureItem>.withAdditionalFeatureButtons(buttons: List<AdditionalFeatureButton>): List<FeatureItem> =
    this + buttons.map { it.toFeatureItem() }

