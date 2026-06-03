@file:OptIn(androidx.compose.material3.ExperimentalMaterial3Api::class)

package org.example.daybook.home

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.LibraryBooks
import androidx.compose.material.icons.filled.CameraAlt
import androidx.compose.material.icons.filled.EditNote
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.QrCode2
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.unit.dp
import org.example.daybook.DaybookScreenScaffold
import org.example.daybook.LocalPermCtx
import org.example.daybook.PermissionRequest
import org.example.daybook.ScreenChromeSpec

data class HomeScreenConfig(val widgets: List<HomeWidgetConfig>)

sealed interface HomeWidgetConfig {
    val id: String
}

data class WipPermissionsWidgetConfig(
    override val id: String = "wip_permissions",
    val ctaLabel: String = "request missing permissions",
) : HomeWidgetConfig

data class HomeMenuWidgetConfig(override val id: String = "home_menu", val items: List<MenuNavItem>) : HomeWidgetConfig

data class MenuNavItem(val id: String, val label: String, val icon: HomeIcon, val onClick: () -> Unit)

enum class HomeIcon {
    Settings,
    Clone,
    NewDoc,
    Camera,
    Mic,
    Drawer,
}

@Composable
fun HomeScreen(config: HomeScreenConfig, chrome: ScreenChromeSpec, modifier: Modifier = Modifier) {
    DaybookScreenScaffold(
        chrome = chrome,
        modifier = modifier,
    ) { scaffoldPadding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(scaffoldPadding),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            config.widgets.forEach { widget ->
                when (widget) {
                    is WipPermissionsWidgetConfig -> WipPermissions(widget = widget)
                    is HomeMenuWidgetConfig -> HomeMenu(widget = widget)
                }
            }
        }
    }
}

@Composable
fun WipPermissions(widget: WipPermissionsWidgetConfig) {
    val permCtx = LocalPermCtx.current ?: return
    if (permCtx.hasAll) return

    TextButton(
        onClick = {
            permCtx.requestPermissions(
                PermissionRequest(
                    camera = true,
                    notifications = true,
                    microphone = true,
                    overlay = true,
                    storageRead = true,
                    storageWrite = true,
                ),
            )
        },
    ) {
        Text(widget.ctaLabel.lowercase())
    }
}

@Composable
fun HomeMenu(widget: HomeMenuWidgetConfig) {
    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
        widget.items.forEach { item ->
            TextButton(
                onClick = item.onClick,
            ) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    Icon(
                        imageVector = item.icon.vector,
                        contentDescription = item.label,
                        modifier = Modifier.size(20.dp),
                    )
                    Text(item.label.lowercase())
                }
            }
        }
    }
}

private val HomeIcon.vector: ImageVector
    get() =
        when (this) {
            HomeIcon.Settings -> Icons.Filled.Settings
            HomeIcon.Clone -> Icons.Filled.QrCode2
            HomeIcon.NewDoc -> Icons.Filled.EditNote
            HomeIcon.Camera -> Icons.Filled.CameraAlt
            HomeIcon.Mic -> Icons.Filled.Mic
            HomeIcon.Drawer -> Icons.AutoMirrored.Filled.LibraryBooks
        }
