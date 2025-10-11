@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.tables

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.NavigationRail
import androidx.compose.material3.NavigationRailItem
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.PermanentDrawerSheet
import androidx.compose.material3.PermanentNavigationDrawer
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.NavHostController
import kotlinx.coroutines.launch
import org.example.daybook.LocalContainer
import org.example.daybook.Routes
import org.example.daybook.TablesState
// TablesTabsList lives in the same package (`org.example.daybook.tables`) so no import required
import org.example.daybook.TablesViewModel
import org.example.daybook.AppScreens

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ExpandedLayout(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    extraAction: (() -> Unit)? = null
) {
    Scaffold(
        modifier = modifier,
        topBar = {
            TopAppBar(
                title = { Text("Daybook") }
            )
        }
    ) { innerPadding ->
        Row(modifier = Modifier.fillMaxSize().padding(innerPadding)) {
            FeaturesRail(navController = navController)
            ExpandedTablesRail()
            PermanentNavigationDrawer(
                drawerContent = {
                    PermanentDrawerSheet(
                        modifier = Modifier.padding(10.dp),
                    ) {
                        Column {
                            TablesTabsList()
                        }
                    }
                }
            ) {
                Box(modifier = Modifier.weight(1f)) {
                    Routes(
                        extraAction = extraAction,
                        navController = navController
                    )
                }
            }
        }
    }
}

@Composable
fun ExpandedTablesRail() {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState().value
    val selectedTableId = vm.selectedTableId.collectAsState().value

    NavigationRail(
        modifier = Modifier.width(80.dp)
    ) {
        // Add Table Button
        FloatingActionButton(
            onClick = {
                vm.viewModelScope.launch {
                    vm.createNewTable()
                }
            },
            modifier = Modifier.size(48.dp)
        ) {
            Text("+")
        }

        Spacer(modifier = Modifier.height(16.dp))

        // Table List
        when (tablesState) {
            is TablesState.Data -> {
                tablesState.tablesList.forEach { table ->
                    NavigationRailItem(
                        selected = selectedTableId == table.id,
                        onClick = { vm.selectTable(table.id) },
                        icon = {
                            Text("📁")
                        },
                        label = { Text(table.title) }
                    )
                }
            }

            is TablesState.Loading -> {
                CircularProgressIndicator(modifier = Modifier.size(24.dp))
            }

            is TablesState.Error -> {
                Text("Error")
            }
        }
    }
}

@Composable
fun FeaturesRail(
    navController: NavHostController,
) {
    NavigationRail(
        modifier = Modifier.width(80.dp)
    ) {
        NavigationRailItem(
            selected = false,
            onClick = { navController.navigate(
                AppScreens.Home.name
            ) },
            icon = {
                Text("H")
            },
            label = { Text("Home") }
        )

        NavigationRailItem(
            selected = false,
            onClick = { navController.navigate(
                AppScreens.Home.name
            ) },
            icon = {
                Text("H")
            },
            label = { Text("Home") }
        )

        NavigationRailItem(
            selected = false,
            onClick = { navController.navigate(
                AppScreens.Capture.name
            ) },
            icon = {
                Text("+")
            },
            label = { Text("Capture") }
        )
    }
}


@Composable
fun TablesTabsList() {
    val tablesRepo = LocalContainer.current.tablesRepo
    val vm = viewModel { TablesViewModel(tablesRepo) }
    val tablesState = vm.tablesState.collectAsState()

    LaunchedEffect(tablesState.value) {
        if (tablesState.value is TablesState.Data) {
            val selectedTable = vm.getSelectedTable()
            if (selectedTable != null) {
                vm.selectTable(selectedTable.id)
            }
        }
    }

    val currentState = tablesState.value
    val selectedTableId = vm.selectedTableId.collectAsState()

    when (currentState) {
        is TablesState.Data -> {
            val selectedTable = selectedTableId.value?.let { currentState.tables[it] }
            if (selectedTable != null) {
                Column {
                    Row(
                        modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "Tabs in ${selectedTable.title}", modifier = Modifier.weight(1f)
                        )
                        // Add new tab button
                        FloatingActionButton(
                            onClick = {
                                vm.viewModelScope.launch {
                                    vm.createNewTab(selectedTable.id)
                                }
                            }, modifier = Modifier.size(32.dp)
                        ) {
                            Text("+", fontSize = 12.sp)
                        }
                    }

                    selectedTable.tabs.mapNotNull { tabId -> currentState.tabs[tabId] }
                        .forEach { tab ->
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                NavigationDrawerItem(
                                    selected = false, // TODO: Track selected tab
                                    onClick = { /* TODO: Select tab */ }, icon = {
                                        Text("📄")
                                    }, badge = {
                                        // Close tab button
                                        FloatingActionButton(
                                            onClick = {
                                                vm.viewModelScope.launch {
                                                    vm.removeTab(tab.id)
                                                }
                                            }, modifier = Modifier.size(24.dp).padding(end = 8.dp)
                                        ) {
                                            Text("×", fontSize = 10.sp)
                                        }
                                    }, label = { Text(tab.title) }, modifier = Modifier.weight(1f)
                                )
                            }
                        }
                }
            } else {
                Text(
                    text = "No table selected",
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                )
            }
        }

        is TablesState.Loading -> {
            Text(
                text = "Loading...",
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
            )
        }

        is TablesState.Error -> {
            Text(
                text = "Error loading tables",
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
            )
        }
    }
}
