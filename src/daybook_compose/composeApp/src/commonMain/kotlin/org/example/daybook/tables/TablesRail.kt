@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
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
import org.example.daybook.uniffi.core.Table
import org.example.daybook.uniffi.core.Uuid

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun TablesRail(
    modifier: Modifier = Modifier,
    showTitles: Boolean = false,
    growUpward: Boolean = false,
    onTableSelected: (Table) -> Unit,
    onTableLayout: ((Uuid, androidx.compose.ui.geometry.Rect) -> Unit)? = null,
    highlightedTable: Uuid? = null,
    onAddTableLayout: ((androidx.compose.ui.geometry.Rect) -> Unit)? = null,
    addTableReadyState: androidx.compose.runtime.State<Boolean>? = null,
    onToggleTableRail: (() -> Unit)? = null
) {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    NavigationRail(modifier = modifier.width(80.dp)) {
        FloatingActionButton(
            onClick = {
                vm.viewModelScope.launch {
                    vm.createNewTable()
                }
            },
            modifier =
                Modifier
                    .size(48.dp)
                    .then(
                        if (onAddTableLayout != null) {
                            Modifier.onGloballyPositioned { onAddTableLayout(it.boundsInWindow()) }
                        } else {
                            Modifier
                        }
                    ),
            containerColor =
                if (addTableReadyState?.value == true) {
                    MaterialTheme.colorScheme.secondary
                } else {
                    MaterialTheme.colorScheme.primary
                }
        ) {
            Text(if (addTableReadyState?.value == true) "✓" else "+")
        }

        // Scrollable list of table items
        if (tablesState is TablesState.Data) {
            val scrollState = rememberScrollState()
            // Start at bottom (max scroll) if growing upward
            LaunchedEffect(tablesState.tablesList.size, growUpward) {
                if (growUpward) {
                    scrollState.scrollTo(scrollState.maxValue)
                }
            }

            // Nested scroll connection to prevent scroll propagation to parent sheet
            // Don't consume in onPreScroll when we can scroll - let child handle it
            // Then consume in onPostScroll any remaining scroll to prevent parent from getting it
            val railNestedScroll =
                remember(scrollState) {
                    object : NestedScrollConnection {
                        override fun onPreScroll(
                            available: Offset,
                            source: NestedScrollSource
                        ): Offset {
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
                                dy > 0 && canScrollUp -> Offset(0f, dy)

                                // Scrolling up, can scroll - consume
                                dy < 0 && canScrollDown -> Offset(0f, dy)

                                // Scrolling down, can scroll - consume
                                else -> Offset.Zero // Can't scroll, let parent handle it
                            }
                        }
                    }
                }
            Column(
                modifier =
                    Modifier
                        .weight(1f)
                        .nestedScroll(railNestedScroll)
                        .verticalScroll(scrollState),
                verticalArrangement =
                    if (growUpward) {
                        androidx.compose.foundation.layout.Arrangement
                            .spacedBy(4.dp, Alignment.Bottom)
                    } else {
                        androidx.compose.foundation.layout.Arrangement
                            .spacedBy(4.dp)
                    }
            ) {
                // Render in normal order - if growUpward, items appear from bottom due to Arrangement.Bottom
                // Otherwise, items appear from top (normal behavior)
                tablesState.tablesList.forEach { table ->
                    val tabCount = table.tabs.size ?: 0
                    NavigationRailItem(
                        modifier =
                            Modifier.then(
                                if (onTableLayout != null) {
                                    Modifier.onGloballyPositioned {
                                        onTableLayout(table.id, it.boundsInWindow())
                                    }
                                } else {
                                    Modifier
                                }
                            ),
                        selected = (selectedTableId == table.id) || (highlightedTable == table.id),
                        onClick = { onTableSelected(table) },
                        icon = {
                            // Icon + small subscript count
                            Row {
                                Box(
                                    modifier = Modifier.size(36.dp),
                                    contentAlignment = Alignment.Center
                                ) {
                                    Text("📁")
                                }
                                Spacer(modifier = Modifier.height(4.dp))
                                Text(
                                    text = tabCount.toString(),
                                    style = MaterialTheme.typography.bodySmall
                                )
                            }
                        },
                        label =
                            if (showTitles) {
                                { Text(table.title) }
                            } else {
                                null
                            }
                    )
                }
            }
        } else {
            Spacer(Modifier.weight(1f))
            CircularProgressIndicator(modifier = Modifier.size(24.dp))
        }

        // Bottom row with toggle button
        if (onToggleTableRail != null) {
            Row(
                modifier =
                    Modifier
                        .fillMaxWidth()
                        .padding(8.dp),
                horizontalArrangement = Arrangement.Start
            ) {
                IconButton(onClick = onToggleTableRail) {
                    Text("◀")
                }
            }
        }
    }
}
