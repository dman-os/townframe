@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class, androidx.compose.material3.ExperimentalMaterial3Api::class, kotlin.time.ExperimentalTime::class)

package org.example.daybook.documents

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
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
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.*
import kotlin.time.Clock
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn

sealed interface DocumentsState {
    data class Data(val docs: List<Doc>) : DocumentsState
    data class Error(val error: FfiException) : DocumentsState
    object Loading : DocumentsState
}

class DocumentsScreenViewModel(
    val drawerRepo: DrawerRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val tablesVm: TablesViewModel
) : ViewModel() {
    private val _documentsState = MutableStateFlow<DocumentsState>(DocumentsState.Loading)
    val documentsState = _documentsState.asStateFlow()

    private val _selectedDocId = MutableStateFlow<String?>(null)
    val selectedDocId = _selectedDocId.asStateFlow()

    private val _selectedDoc = MutableStateFlow<Doc?>(null)
    val selectedDoc = _selectedDoc.asStateFlow()

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

    private var listenerRegistration: ListenerRegistration? = null

    private val listener = object : DrawerEventListener {
        override fun onDrawerEvent(event: DrawerEvent) {
            viewModelScope.launch {
                when (event) {
                    DrawerEvent.ListChanged -> refreshDocs()
                    is DrawerEvent.DocUpdated -> {
                        if (event.id == _selectedDocId.value) {
                            loadSelectedDoc(event.id)
                        }
                        refreshDocs()
                    }
                    else -> {}
                }
            }
        }
    }

    init {
        refreshDocs()
        viewModelScope.launch {
            listenerRegistration = drawerRepo.ffiRegisterListener(listener)
        }
    }

    fun refreshDocs() {
        viewModelScope.launch {
            _documentsState.value = DocumentsState.Loading
            try {
                val ids = drawerRepo.list()
                val docs = ids.mapNotNull { id ->
                    try {
                        drawerRepo.get(id)
                    } catch (e: FfiException) {
                        null
                    }
                }
                _documentsState.value = DocumentsState.Data(docs)
            } catch (e: FfiException) {
                _documentsState.value = DocumentsState.Error(e)
            }
        }
    }

    fun selectDoc(id: String?) {
        _selectedDocId.value = id
        if (id != null) {
            loadSelectedDoc(id)
        } else {
            _selectedDoc.value = null
        }
    }

    private fun loadSelectedDoc(id: String) {
        viewModelScope.launch {
            _selectedDoc.value = drawerRepo.get(id)
        }
    }

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

    fun updateDocContent(content: String) {
        viewModelScope.launch {
            val docId = _selectedDocId.value ?: return@launch
            val current = _selectedDoc.value ?: return@launch
            
            // Optimistic update
            val updatedDoc = current.copy(
                content = DocContent.Text(content),
                updatedAt = Clock.System.now()
            )
            _selectedDoc.value = updatedDoc

            drawerRepo.updateBatch(listOf(DocPatch(
                id = docId,
                createdAt = null,
                content = DocContent.Text(content),
                updatedAt = Clock.System.now(),
                tags = null
            )))
        }
    }

    override fun onCleared() {
        listenerRegistration?.unregister()
        super.onCleared()
    }
}

@Composable
fun DocumentsScreen(
    contentType: DaybookContentType,
    modifier: Modifier = Modifier
) {
    val container = LocalContainer.current
    val tablesVm: TablesViewModel = viewModel { TablesViewModel(container.tablesRepo) }
    val vm = viewModel { DocumentsScreenViewModel(container.drawerRepo, container.tablesRepo, container.blobsRepo, tablesVm) }
    
    val documentsState by vm.documentsState.collectAsState()
    val selectedDocId by vm.selectedDocId.collectAsState()
    val selectedDoc by vm.selectedDoc.collectAsState()

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
                            state = documentsState,
                            selectedDocId = selectedDocId,
                            onDocClick = { vm.selectDoc(it.id) }
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
                                blobsRepo = vm.blobsRepo
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
                    onBack = { vm.selectDoc(null) }
                )
            ) {
                Box(modifier = modifier.fillMaxSize()) {
                        DocEditor(
                            doc = selectedDoc,
                            onContentChange = { vm.updateDocContent(it) },
                            modifier = Modifier.padding(16.dp),
                            blobsRepo = vm.blobsRepo
                        )
                }
            }
        } else {
            ProvideChromeState(ChromeState(title = "Documents")) {
                DocList(
                    state = documentsState,
                    selectedDocId = null,
                    onDocClick = { vm.selectDoc(it.id) },
                    modifier = modifier
                )
            }
        }
    }
}

@Composable
fun DocList(
    state: DocumentsState,
    selectedDocId: String?,
    onDocClick: (Doc) -> Unit,
    modifier: Modifier = Modifier
) {
    when (state) {
        is DocumentsState.Loading -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                CircularProgressIndicator()
            }
        }
        is DocumentsState.Error -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                Text("Error: ${state.error.message()}")
            }
        }
        is DocumentsState.Data -> {
            if (state.docs.isEmpty()) {
                Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text("No documents in drawer", style = MaterialTheme.typography.bodyLarge)
                }
            } else {
                LazyColumn(modifier = modifier.fillMaxSize()) {
                    items(state.docs) { doc ->
                        val isSelected = doc.id == selectedDocId
                        ListItem(
                            headlineContent = { 
                                Text(
                                    text = when (val content = doc.content) {
                                        is DocContent.Text -> content.v1.take(50).ifEmpty { "Empty document" }
                                        else -> "Unsupported content"
                                    },
                                    maxLines = 1
                                )
                            },
                            supportingContent = {
                                Text("ID: ${doc.id.take(8)}...")
                            },
                            modifier = Modifier
                                .clickable { onDocClick(doc) }
                                .then(if (isSelected) Modifier.background(MaterialTheme.colorScheme.primaryContainer) else Modifier)
                        )
                        HorizontalDivider()
                    }
                }
            }
        }
    }
}

