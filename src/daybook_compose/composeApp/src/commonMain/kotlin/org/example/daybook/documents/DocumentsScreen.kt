@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class, androidx.compose.material3.ExperimentalMaterial3Api::class, kotlin.time.ExperimentalTime::class)

package org.example.daybook.documents

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.size
import org.example.daybook.tables.DockableRegion
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.ChromeState
import org.example.daybook.DaybookContentType
import org.example.daybook.LocalContainer
import org.example.daybook.ProvideChromeState
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.ui.DocEditor
import org.example.daybook.DrawerViewModel
import org.example.daybook.uniffi.types.*
import org.example.daybook.DocListState
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.*
import kotlin.time.Clock
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn

class DocumentsScreenViewModel(
    val drawerVm: DrawerViewModel,
    val tablesRepo: TablesRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val tablesVm: TablesViewModel
) : ViewModel() {

    val listSizeExpanded = tablesVm.tablesState.map { state ->
        if (state is TablesState.Data) {
            val selectedTableId = tablesVm.selectedTableId.value
            val windowId = selectedTableId?.let { id ->
                state.tables[id]?.window?.let { windowPolicy ->
                    when (windowPolicy) {
                        is TableWindow.Specific -> windowPolicy.id
                        is TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                    }
                }
            }
            windowId?.let { id ->
                state.windows[id]?.documentsScreenListSizeExpanded
            } ?: WindowLayoutRegionSize.Weight(0.4f)
        } else {
            WindowLayoutRegionSize.Weight(0.4f)
        }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), WindowLayoutRegionSize.Weight(0.4f))

    fun updateListSize(weight: Float) {
        viewModelScope.launch {
            val state = tablesVm.tablesState.value
            val selectedTableId = tablesVm.selectedTableId.value
            if (state is TablesState.Data && selectedTableId != null) {
                val windowId = state.tables[selectedTableId]?.window?.let { windowPolicy ->
                    when (windowPolicy) {
                        is TableWindow.Specific -> windowPolicy.id
                        is TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                    }
                }
                windowId?.let { id ->
                    state.windows[id]?.let { window ->
                        tablesRepo.setWindow(id, window.copy(documentsScreenListSizeExpanded = WindowLayoutRegionSize.Weight(weight)))
                    }
                }
            }
        }
    }

    private var debounceJob: kotlinx.coroutines.Job? = null
    
    fun updateDocContent(content: String) {
        val docId = drawerVm.selectedDocId.value ?: return
        val current = drawerVm.selectedDoc.value ?: return
        
        // Optimistic update
        val updatedDoc = current.copy(
            content = DocContent.Text(content),
            updatedAt = Clock.System.now()
        )
        drawerVm._selectedDocMutable.value = updatedDoc

        // Cancel previous debounce job
        debounceJob?.cancel()
        // Start new debounce job
        debounceJob = viewModelScope.launch {
            kotlinx.coroutines.delay(500) // 500ms debounce
            drawerVm.updateDoc(DocPatch(
                id = docId,
                createdAt = null,
                content = DocContent.Text(content),
                updatedAt = Clock.System.now(),
                props = null
            ))
        }
    }
}

@Composable
fun DocumentsScreen(
    contentType: DaybookContentType,
    modifier: Modifier = Modifier
) {
    val container = LocalContainer.current
    val tablesVm: TablesViewModel = viewModel { TablesViewModel(container.tablesRepo) }
    val drawerVm: DrawerViewModel = viewModel { DrawerViewModel(container.drawerRepo) }
    val vm = viewModel { DocumentsScreenViewModel(drawerVm, container.tablesRepo, container.blobsRepo, tablesVm) }
    
    val selectedDocId by drawerVm.selectedDocId.collectAsState()
    val selectedDoc by drawerVm.selectedDoc.collectAsState()

    if (contentType == DaybookContentType.LIST_AND_DETAIL) {
        val listSize by vm.listSizeExpanded.collectAsState()
        val weight = when (val s = listSize) {
            is WindowLayoutRegionSize.Weight -> s.v1
        }
        
        ProvideChromeState(ChromeState(title = "Documents")) {
            DockableRegion(
                modifier = modifier.fillMaxSize(),
                orientation = Orientation.Horizontal,
                initialWeights = mapOf("list" to weight, "editor" to 1f - weight),
                onWeightsChanged = { newWeights ->
                    newWeights["list"]?.let { vm.updateListSize(it) }
                }
            ) {
                pane("list") {
                    Box(modifier = Modifier.fillMaxSize()) {
                        DocList(
                            drawerViewModel = drawerVm,
                            selectedDocId = selectedDocId,
                            onDocClick = { drawerVm.selectDoc(it) }
                        )
                    }
                }
                
                pane("editor") {
                    Box(modifier = Modifier.fillMaxSize()) {
                        if (selectedDocId != null) {
                            DocEditor(
                                doc = selectedDoc,
                                onContentChange = { vm.updateDocContent(it) },
                                modifier = Modifier.padding(16.dp),
                                blobsRepo = vm.blobsRepo,
                                drawerViewModel = drawerVm
                            )
                        } else {
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Text("Select a document to view details")
                            }
                        }
                    }
                }
            }
        }
    } else {
        if (selectedDocId != null) {
            ProvideChromeState(
                ChromeState(
                    title = "Edit Document",
                    onBack = { drawerVm.selectDoc(null) }
                )
            ) {
                Box(modifier = modifier.fillMaxSize()) {
                        DocEditor(
                            doc = selectedDoc,
                            onContentChange = { vm.updateDocContent(it) },
                            modifier = Modifier.padding(16.dp),
                            blobsRepo = vm.blobsRepo,
                            drawerViewModel = drawerVm
                        )
                }
            }
        } else {
            ProvideChromeState(ChromeState(title = "Documents")) {
                // Observe drawerState reactively
                DocList(
                    drawerViewModel = drawerVm,
                    selectedDocId = null,
                    onDocClick = { drawerVm.selectDoc(it) },
                    modifier = modifier
                )
            }
        }
    }
}

@Composable
fun DocList(
    drawerViewModel: DrawerViewModel,
    selectedDocId: String?,
    onDocClick: (String) -> Unit,
    modifier: Modifier = Modifier
) {
    val docListState by drawerViewModel.docListState.collectAsState()
    val loadedDocs by drawerViewModel.loadedDocs.collectAsState()
    val loadingDocs by drawerViewModel.loadingDocs.collectAsState()
    
    val listState = rememberLazyListState()
    
    val currentState = docListState
    when (currentState) {
        is DocListState.Loading -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                CircularProgressIndicator()
            }
        }
        is DocListState.Error -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                Text("Error: ${currentState.error.message()}")
            }
        }
        is DocListState.Data -> {
            val docIds = currentState.docIds
            if (docIds.isEmpty()) {
                Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text("No documents in drawer", style = MaterialTheme.typography.bodyLarge)
                }
            } else {
                // Track visible items and preload next documents
                LaunchedEffect(listState.firstVisibleItemIndex, listState.firstVisibleItemScrollOffset) {
                    val layoutInfo = listState.layoutInfo
                    val firstVisible = listState.firstVisibleItemIndex
                    val lastVisible = layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: firstVisible
                    val preloadCount = 10 // Preload next 10 items
                    val endIndex = (lastVisible + preloadCount).coerceAtMost(docIds.size - 1)
                    
                    val idsToLoad = docIds.subList(firstVisible.coerceAtLeast(0), endIndex + 1)
                    drawerViewModel.loadDocs(idsToLoad)
                }
                
                LazyColumn(
                    state = listState,
                    modifier = modifier.fillMaxSize()
                ) {
                    items(docIds.size, key = { idx -> docIds[idx] }) { index ->
                        val docId = docIds[index]
                        val doc = loadedDocs[docId]
                        val isLoading = loadingDocs.contains(docId)
                        val isSelected = docId == selectedDocId
                        
                        if (doc != null) {
                            // Document is loaded, show it
                            val draw = @Composable {
                                ListItem(
                                    headlineContent = { 
                                        val titleTag = doc.props.firstOrNull { it is DocProp.TitleGeneric } as? DocProp.TitleGeneric
                                        Text(
                                            text = titleTag?.v1 ?: when (val content = doc.content) {
                                                is DocContent.Text -> content.v1.take(50).ifEmpty { "Empty document" }
                                                else -> "Unsupported content"
                                            },
                                            maxLines = 1
                                        )
                                    },
                                    supportingContent = {
                                        Text("ID: ${doc.id.take(8)}...")
                                    }
                                )
                            }
                            if (isSelected) {
                                OutlinedCard(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) },
                                ) {
                                    draw()
                                }
                            } else {
                                Card(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) },
                                ) {
                                    draw()
                                }
                            }
                        } else if (isLoading) {
                            // Document is loading, show loading indicator
                            Card(
                                modifier = Modifier.fillMaxWidth(),
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Row(
                                            modifier = Modifier.fillMaxWidth(),
                                            horizontalArrangement = Arrangement.SpaceBetween,
                                            verticalAlignment = Alignment.CenterVertically
                                        ) {
                                            Text("Loading...", style = MaterialTheme.typography.bodyMedium)
                                            CircularProgressIndicator(
                                                modifier = Modifier.size(16.dp)
                                            )
                                        }
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    }
                                )
                            }
                        } else {
                            // Document not loaded yet, trigger load and show placeholder
                            LaunchedEffect(docId) {
                                drawerViewModel.loadDoc(docId)
                            }
                            Card(
                                modifier = Modifier.fillMaxWidth(),
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Text("Loading...", style = MaterialTheme.typography.bodyMedium)
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    }
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

