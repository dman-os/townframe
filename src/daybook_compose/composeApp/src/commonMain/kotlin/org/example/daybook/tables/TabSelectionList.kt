@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.IconButton
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.input.nestedscroll.NestedScrollConnection
import androidx.compose.ui.input.nestedscroll.NestedScrollSource
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.launch
import org.example.daybook.LocalContainer
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.uniffi.core.Tab
import org.example.daybook.uniffi.core.Uuid

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TabSelectionList(
    onTabSelected: (Tab) -> Unit,
    modifier: Modifier = Modifier,
    growUpward: Boolean = false,
    onItemLayout: ((Uuid, Rect) -> Unit)? = null,
    highlightedTab: Uuid? = null
) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    // Ensure a table is selected when data becomes available
    LaunchedEffect(tablesState) {
        if (tablesState is TablesState.Data) {
            val sel = vm.getSelectedTable()
            if (sel != null) vm.selectTable(sel.id)
        }
    }

    val tabsForSelectedTable = if (selectedTableId != null && tablesState is TablesState.Data) {
        val selectedTable = tablesState.tables[selectedTableId]
        selectedTable?.tabs?.mapNotNull { tabId -> tablesState.tabs[tabId] } ?: emptyList()
    } else emptyList()

    val scrollState = rememberScrollState()
    // Start at bottom (max scroll) if growing upward
    LaunchedEffect(tabsForSelectedTable.size, growUpward) {
        if (growUpward) {
            scrollState.scrollTo(scrollState.maxValue)
        }
    }
    
    // Nested scroll connection to prevent scroll propagation to parent sheet
    // Don't consume in onPreScroll when we can scroll - let child handle it
    // Then consume in onPostScroll any remaining scroll to prevent parent from getting it
    val tabListNestedScroll = remember(scrollState) {
        object : NestedScrollConnection {
            override fun onPreScroll(available: Offset, source: NestedScrollSource): Offset {
                // Don't consume here - let the child's verticalScroll handle it first
                return Offset.Zero
            }
            
            override fun onPostScroll(
                consumed: Offset,
                available: Offset,
                source: NestedScrollSource
            ): Offset {
                val dy = available.y
                if (dy == 0f) return Offset.Zero
                
                // Check if we can still scroll in the requested direction
                val canScrollUp = scrollState.value > 0
                val canScrollDown = scrollState.value < scrollState.maxValue
                
                // If we can scroll, consume remaining scroll to prevent parent from getting it
                return when {
                    dy > 0 && canScrollUp -> Offset(0f, dy) // Scrolling up, can scroll - consume
                    dy < 0 && canScrollDown -> Offset(0f, dy) // Scrolling down, can scroll - consume
                    else -> Offset.Zero // Can't scroll, let parent handle it
                }
            }
        }
    }

    // Fill available height and render tabs
    Column(
        modifier = modifier
            .fillMaxHeight()
            .nestedScroll(tabListNestedScroll)
            .verticalScroll(scrollState),
        verticalArrangement = if (growUpward) {
            Arrangement.spacedBy(4.dp, Alignment.Bottom)
        } else {
            Arrangement.spacedBy(4.dp)
        }
    ) {
        if (tabsForSelectedTable.isEmpty()) {
            Text("No tabs in this table.", modifier = Modifier.padding(16.dp))
        } else {
            // Render in normal order - if growUpward, items appear from bottom due to Arrangement.Bottom
            // Otherwise, items appear from top (normal behavior)
            tabsForSelectedTable.forEach { tab ->
                val isHighlighted = tab.id == highlightedTab
                // per-row menu state
                val menuExpandedState = remember { mutableStateOf(false) }

                // Use NavigationDrawerItem so we can use selected highlighting and badge slot
                NavigationDrawerItem(
                    selected = isHighlighted,
                    onClick = { onTabSelected(tab) },
                    icon = { Text("📄") },
                    label = { Text(tab.title) },
                    modifier = Modifier
                        .fillMaxWidth()
                        .then(
                            if (onItemLayout != null) {
                                Modifier.onGloballyPositioned { 
                                    onItemLayout(tab.id, it.boundsInWindow()) 
                                }
                            } else {
                                Modifier
                            }
                        )
                        .combinedClickable(
                            onClick = { onTabSelected(tab) },
                            onLongClick = { menuExpandedState.value = true }
                        ),
                    badge = {
                        // place close action in the badge area
                        IconButton(onClick = { vm.viewModelScope.launch { vm.removeTab(tab.id) } }) {
                            Text("✕")
                        }
                    }
                )

                DropdownMenu(
                    expanded = menuExpandedState.value,
                    onDismissRequest = { menuExpandedState.value = false }
                ) {
                    DropdownMenuItem(text = { Text("Close") }, onClick = {
                        menuExpandedState.value = false
                        vm.viewModelScope.launch { vm.removeTab(tab.id) }
                    })
                }
            }
        }
    }
}
