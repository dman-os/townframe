@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.capture.screens

import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocContent
import org.example.daybook.uniffi.types.DocPropKey
import org.example.daybook.uniffi.types.DocPropTag
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.WellKnownPropTag
import org.example.daybook.uniffi.types.AddDocArgs
import org.example.daybook.uniffi.core.UpdateDocArgs

import androidx.compose.foundation.layout.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.CameraAlt
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.TextFields
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.unit.dp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.ChromeState
import org.example.daybook.LocalContainer
import org.example.daybook.MainFeatureActionButton
import org.example.daybook.ProvideChromeState
import org.example.daybook.TablesState
import org.example.daybook.TablesViewModel
import org.example.daybook.capture.DaybookCameraPreview
import org.example.daybook.capture.LocalCameraCaptureContext
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.*
import kotlin.time.Clock
import kotlin.uuid.Uuid
import org.example.daybook.ui.DocEditor

sealed interface DocsListState {
    data class Data(val docs: List<Doc>) : DocsListState
    data class Error(val error: FfiException) : DocsListState
    object Loading : DocsListState
}

class CaptureScreenViewModel(
    val drawerRepo: DrawerRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    val tablesVm: TablesViewModel,
    val initialDocId: String? = null
) : ViewModel() {
    private val _captureMode = MutableStateFlow(CaptureMode.TEXT)
    val captureMode = _captureMode.asStateFlow()

    private val _currentDocId = MutableStateFlow<String?>(null)
    val currentDocId = _currentDocId.asStateFlow()

    private val _currentDoc = MutableStateFlow<Doc?>(null)
    val currentDoc = _currentDoc.asStateFlow()

    private val _message = MutableStateFlow<String?>(null)
    val message = _message.asStateFlow()

    private var isCreatingDoc = false

    fun setCaptureMode(mode: CaptureMode) {
        if (_captureMode.value == mode) return
        _captureMode.value = mode
        persistCaptureMode(mode)
    }

    private fun persistCaptureMode(mode: CaptureMode) {
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
                        tablesRepo.setWindow(id, window.copy(lastCaptureMode = mode))
                    }
                }
            }
        }
    }

    fun saveImage(bytes: ByteArray) {
        viewModelScope.launch {
            try {
                val hashStr = blobsRepo.put(bytes)
                
                // Create AddDocArgs
                val args = AddDocArgs(
                    branchPath = "main",
                    props = mapOf(
                        DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.CONTENT)) to "{\"blob\":{\"length_octets\":${bytes.size},\"hash\":\"$hashStr\"}}",
                        DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.IMAGE_METADATA)) to "{\"mime\":\"image/jpeg\",\"width_px\":0,\"height_px\":0}"
                    ),
                    userPath = null
                )
                
                drawerRepo.add(args)
                _message.value = "Photo saved successfully"
            } catch (e: FfiException) {
                println("Error saving image: $e")
                _message.value = "Error saving photo: ${e.message}"
            }
        }
    }

    fun clearMessage() {
        _message.value = null
    }

    fun updateDocContent(content: String) {
        viewModelScope.launch {
            val docId = _currentDocId.value
            if (docId == null) {
                if (isCreatingDoc) return@launch
                isCreatingDoc = true
                // Create new doc
                val args = AddDocArgs(
                    branchPath = "main",
                    props = mapOf(
                        DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.CONTENT)) to "\"$content\""
                    ),
                    userPath = null
                )
                val returnedId = drawerRepo.add(args)
                _currentDocId.value = returnedId
                // We'll let the listener refresh the current doc
            } else {
                // Update existing doc
                val current = _currentDoc.value
                if (current != null) {
                    val patch = DocPatch(
                        id = docId,
                        propsSet = mapOf(
                            DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.CONTENT)) to "\"$content\""
                        ),
                        propsRemove = emptyList(),
                        userPath = null
                    )
                    
                    drawerRepo.updateBatch(listOf(UpdateDocArgs("main", null, patch)))
                }
            }
        }
    }

    fun loadDoc(id: String) {
        viewModelScope.launch {
            val doc = drawerRepo.get(id, "main")
            _currentDocId.value = id
            _currentDoc.value = doc
        }
    }

    private val _docsList = MutableStateFlow(DocsListState.Loading as DocsListState)
    val docsList = _docsList.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    // Listener instance implemented on Kotlin side
    private val listener = object : DrawerEventListener {
        override fun onDrawerEvent(event: DrawerEvent) {
            viewModelScope.launch {
                when (event) {
                    is DrawerEvent.ListChanged -> refreshDocs()
                    is DrawerEvent.DocAdded -> refreshDocs()
                    is DrawerEvent.DocUpdated -> {
                        if (event.id == _currentDocId.value) {
                            loadDoc(event.id)
                        }
                    }
                    else -> {}
                }
            }
        }
    }

    init {
        loadLatestDocs()
        if (initialDocId != null) {
            loadDoc(initialDocId)
        }
        viewModelScope.launch {
            listenerRegistration = drawerRepo.ffiRegisterListener(listener)
        }
        
        // Initialize mode from current window
        viewModelScope.launch {
            tablesVm.tablesState.collect { state ->
                if (state is TablesState.Data) {
                    val selectedTableId = tablesVm.selectedTableId.value
                    val windowId = state.tables[selectedTableId]?.window?.let { windowPolicy ->
                        when (windowPolicy) {
                            is TableWindow.Specific -> windowPolicy.id
                            is TableWindow.AllWindows -> state.windows.keys.firstOrNull()
                        }
                    }
                    windowId?.let { id ->
                        state.windows[id]?.let { window ->
                            _captureMode.value = window.lastCaptureMode
                        }
                    }
                }
            }
        }
    }

    private suspend fun refreshDocs() {
        _docsList.value = DocsListState.Loading
        try {
            val branches = drawerRepo.list()
            val docs = branches.mapNotNull { b ->
                try {
                    drawerRepo.get(b.docId, "main")
                } catch (e: FfiException) {
                    null
                }
            }
            _docsList.value = DocsListState.Data(docs)
        } catch (err: FfiException) {
            _docsList.value = DocsListState.Error(err)
        }
    }

    fun loadLatestDocs() {
        viewModelScope.launch {
            refreshDocs()
        }
    }

    override fun onCleared() {
        listenerRegistration?.unregister()
        super.onCleared()
    }
}


@Composable
fun CaptureScreen(
    modifier: Modifier = Modifier,
    initialDocId: String? = null
) {
    val container = LocalContainer.current
    val tablesVm = viewModel { TablesViewModel(container.tablesRepo) }
    val vm = viewModel {
        CaptureScreenViewModel(
            drawerRepo = container.drawerRepo,
            tablesRepo = container.tablesRepo,
            blobsRepo = container.blobsRepo,
            tablesVm = tablesVm,
            initialDocId = initialDocId
        )
    }

    val captureMode by vm.captureMode.collectAsState()
    val currentDoc by vm.currentDoc.collectAsState()
    
    val captureContext = LocalCameraCaptureContext.current
    val canCapture = if (captureContext != null && captureMode == CaptureMode.CAMERA) {
        captureContext.canCapture.collectAsState().value
    } else {
        false
    }
    val isCapturing = if (captureContext != null && captureMode == CaptureMode.CAMERA) {
        captureContext.isCapturing.collectAsState().value
    } else {
        false
    }

    val snackbarHostState = remember { SnackbarHostState() }
    val message by vm.message.collectAsState()

    LaunchedEffect(message) {
        message?.let {
            snackbarHostState.showSnackbar(it)
            vm.clearMessage()
        }
    }
    
    val chromeState = remember(captureMode, canCapture, isCapturing) {
        if (captureMode == CaptureMode.CAMERA && captureContext != null) {
            val ctx = captureContext
            ChromeState(
                mainFeatureActionButton = MainFeatureActionButton.Button(
                    icon = { Text("📷") },
                    label = { Text(if (isCapturing) "Capturing..." else "Save Photo") },
                    enabled = canCapture && !isCapturing,
                    onClick = { ctx.requestCapture() }
                )
            )
        } else {
            ChromeState.Empty
        }
    }

    ProvideChromeState(chromeState) {
        Box(modifier = modifier.fillMaxSize()) {
            when (captureMode) {
                CaptureMode.CAMERA -> {
                    DaybookCameraPreview(
                        onImageSaved = { byteArray ->
                            vm.saveImage(byteArray)
                        },
                        onCaptureRequested = {}
                    )
                }
                CaptureMode.TEXT -> {
                    DocEditor(
                        doc = currentDoc,
                        onContentChange = { vm.updateDocContent(it) },
                        modifier = Modifier.padding(16.dp)
                    )
                }
                CaptureMode.MIC -> {
                    Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                        Column(horizontalAlignment = Alignment.CenterHorizontally) {
                            Text("🎤", style = MaterialTheme.typography.displayLarge)
                            Text("Mic mode placeholder", style = MaterialTheme.typography.headlineMedium)
                        }
                    }
                }
            }

            // Floating Action Buttons for mode switching
            Column(
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .padding(16.dp),
                verticalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                ModeFab(
                    icon = Icons.Default.TextFields,
                    selected = captureMode == CaptureMode.TEXT,
                    onClick = { vm.setCaptureMode(CaptureMode.TEXT) }
                )
                ModeFab(
                    icon = Icons.Default.CameraAlt,
                    selected = captureMode == CaptureMode.CAMERA,
                    onClick = { vm.setCaptureMode(CaptureMode.CAMERA) }
                )
                ModeFab(
                    icon = Icons.Default.Mic,
                    selected = captureMode == CaptureMode.MIC,
                    onClick = { vm.setCaptureMode(CaptureMode.MIC) }
                )
            }

            SnackbarHost(
                hostState = snackbarHostState,
                modifier = Modifier.align(Alignment.BottomCenter).padding(bottom = 80.dp)
            )
        }
    }
}

@Composable
fun ModeFab(
    icon: ImageVector,
    selected: Boolean,
    onClick: () -> Unit
) {
    FloatingActionButton(
        onClick = onClick,
        containerColor = if (selected) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.secondaryContainer,
        contentColor = if (selected) MaterialTheme.colorScheme.onPrimaryContainer else MaterialTheme.colorScheme.onSecondaryContainer
    ) {
        Icon(icon, contentDescription = null)
    }
}
