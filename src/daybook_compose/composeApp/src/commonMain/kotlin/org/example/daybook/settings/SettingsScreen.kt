@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.settings

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalContainer
// TODO: Update SettingsScreen to use new WindowLayout structure
// import org.example.daybook.uniffi.core.WindowLayoutSidebarMode
// import org.example.daybook.uniffi.core.WindowLayoutSidebarPosition
// import org.example.daybook.uniffi.core.WindowLayoutSidebarVisibility
// import org.example.daybook.uniffi.core.WindowLayoutTabListVisibility
// import org.example.daybook.uniffi.core.WindowLayoutTableViewMode

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsScreen(
    modifier: Modifier = Modifier
) {
    val configRepo = LocalContainer.current.configRepo
    val configVm = viewModel { ConfigViewModel(configRepo) }
    
    // TODO: Update to use new LayoutWindowConfig structure
    // Observe sidebar settings
    // val sidebarPosExpandedState = configVm.sidebarPosExpanded.collectAsState()
    // val sidebarPos = sidebarPosExpandedState.value ?: SidebarPosition.RIGHT
    val sidebarPos = "RIGHT" // Placeholder
    
    // val sidebarAutoHideExpandedState = configVm.sidebarAutoHideExpanded.collectAsState()
    // val sidebarAutoHide = sidebarAutoHideExpandedState.value ?: false
    val sidebarAutoHide = false // Placeholder
    
    Column(
        modifier = modifier
            .verticalScroll(rememberScrollState())
            .padding(16.dp)
    ) {
        // Layout section
        Text(
            text = "Layout",
            style = androidx.compose.material3.MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 8.dp)
        )

        HorizontalDivider(modifier = Modifier.padding(bottom = 16.dp))

        // Combined sidebar position toggle for both medium and expanded
        Row(
            modifier = Modifier
                .padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Sidebar Position",
                modifier = Modifier.weight(1f),
                style = androidx.compose.material3.MaterialTheme.typography.bodyLarge
            )
            Switch(
                checked = sidebarPos == "RIGHT",
                onCheckedChange = { isRight ->
                    // TODO: Update to use new LayoutWindowConfig structure
                    // val newPos = if (isRight) SidebarPosition.RIGHT else SidebarPosition.LEFT
                    // configVm.setSidebarPosExpanded(newPos)
                }
            )
            Text(
                text = if (sidebarPos == "RIGHT") "Right" else "Left",
                modifier = Modifier.padding(start = 8.dp),
                style = androidx.compose.material3.MaterialTheme.typography.bodyMedium
            )
        }
        
        // Sidebar auto-hide toggle
        Row(
            modifier = Modifier
                .padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Sidebar Auto-Hide",
                modifier = Modifier.weight(1f),
                style = androidx.compose.material3.MaterialTheme.typography.bodyLarge
            )
            Switch(
                checked = sidebarAutoHide,
                onCheckedChange = { checked ->
                    // TODO: Update to use new LayoutWindowConfig structure
                    // configVm.setSidebarAutoHideExpanded(checked)
                }
            )
            Text(
                text = if (sidebarAutoHide) "On" else "Off",
                modifier = Modifier.padding(start = 8.dp),
                style = androidx.compose.material3.MaterialTheme.typography.bodyMedium
            )
        }
        
        // Reset layout settings button
        Row(
            modifier = Modifier
                .padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Reset Layout Settings",
                modifier = Modifier.weight(1f),
                style = androidx.compose.material3.MaterialTheme.typography.bodyLarge
            )
            ResetLayoutButton(
                onReset = {
                    // TODO: Update to use new LayoutWindowConfig structure
                    // Reset all layout settings to defaults
                    // configVm.setSidebarPosExpanded(SidebarPosition.RIGHT)
                    // configVm.setSidebarVisExpanded(SidebarVisibility.VISIBLE)
                    // configVm.setSidebarModeExpanded(SidebarMode.COMPACT)
                    // configVm.setSidebarAutoHideExpanded(false)
                    // configVm.setTabListVisExpanded(TabListVisibility.VISIBLE)
                    // configVm.setTableRailVisExpanded(TabListVisibility.VISIBLE)
                    // configVm.setTableRailVisCompact(TabListVisibility.VISIBLE)
                    // configVm.setTableViewModeCompact(TableViewMode.HIDDEN)
                }
            )
        }
    }
}
