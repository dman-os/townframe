@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.capture.screens

import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.AdditionalFeatureButton
import org.example.daybook.ChromeState
import org.example.daybook.LocalContainer
import org.example.daybook.MainFeatureActionButton
import org.example.daybook.ProvideChromeState
// removed Doc/DocContent imports - using Uuid list for drawer
import org.example.daybook.uniffi.core.Doc
import org.example.daybook.uniffi.core.DocContent
import org.example.daybook.uniffi.core.DrawerEvent
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.capture.DaybookCameraPreview
import org.example.daybook.capture.LocalCameraCaptureContext
import kotlin.time.Clock
import kotlin.uuid.Uuid
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier

enum class CaptureMode {
    Text,
    Camera,
    Mic
}

sealed interface DocsListState {
    data class Data(val docs: List<Doc>) : DocsListState
    data class Error(val error: FfiException) : DocsListState
    object Loading : DocsListState
}

class CaptureScreenViewModel(
    val drawerRepo: DrawerRepoFfi,
    val initialMode: CaptureMode = CaptureMode.Camera,
    val availableModes: Set<CaptureMode> = setOf(CaptureMode.Text, CaptureMode.Camera),
) : ViewModel() {
    private val _captureMode = MutableStateFlow(initialMode)
    val captureMode = _captureMode.asStateFlow()
    
    fun setCaptureMode(mode: CaptureMode) {
        _captureMode.value = mode
    }

    private val _docsList = MutableStateFlow(DocsListState.Loading as DocsListState)
    val docsList = _docsList.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    // Listener instance implemented on Kotlin side
    private val listener = object : DrawerEventListener {
        override fun onDrawerEvent(event: DrawerEvent) {
            // Ensure UI updates happen on main thread
            viewModelScope.launch {
                when (event) {
                    DrawerEvent.ListChanged -> {
                        // Refresh from source of truth in Rust
                        refreshDocs()
                    }
                    else -> {

                    }
                }
            }
        }
    }

    init {
        // initial load
        loadLatestDocs()
        // register listener
        viewModelScope.launch {
            listenerRegistration = drawerRepo.ffiRegisterListener(listener)
        }
    }

    private suspend fun refreshDocs() {
        _docsList.value = DocsListState.Loading
        try {
            val ids = drawerRepo.list()
            val docs = ids.mapNotNull { idStr ->
                try {
                    drawerRepo.get(idStr)
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

    fun addOne() {
        viewModelScope.launch {
            val id = Uuid.random()
            // create a new Doc and send as a single-item batch to ffi_update_batch
            val doc = Doc(
                id = id.toString(),
                createdAt = Clock.System.now(),
                updatedAt = Clock.System.now(),
                content = DocContent.Text("hello"),
                tags = listOf()
            )
            drawerRepo.add(doc)
        }
    }

    override fun onCleared() {
        // Clean up registration
        listenerRegistration?.unregister()
        super.onCleared()
    }
}

@Composable
fun CaptureScreen(
    modifier: Modifier = Modifier
) {
    val drawerRepo = LocalContainer.current.drawerRepo
    val vm = viewModel {
        CaptureScreenViewModel(drawerRepo = drawerRepo)
    }

    val captureMode = vm.captureMode.collectAsState().value
    val docsList = vm.docsList.collectAsState().value
    
    // Get capture context for camera mode
    val captureContext = LocalCameraCaptureContext.current
    
    // Get capture state for chrome state button
    val canCapture = if (captureContext != null && captureMode == CaptureMode.Camera) {
        captureContext.canCapture.collectAsState().value
    } else {
        false
    }
    val isCapturing = if (captureContext != null && captureMode == CaptureMode.Camera) {
        captureContext.isCapturing.collectAsState().value
    } else {
        false
    }
    
    // Create chrome state with main feature action button for camera mode
    // Use remember to stabilize the state object - only recreate when actual values change
    val chromeState = remember(captureMode == CaptureMode.Camera, captureContext != null, canCapture, isCapturing) {
        if (captureMode == CaptureMode.Camera && captureContext != null) {
            val ctx = captureContext // Capture for the lambda
            ChromeState(
                mainFeatureActionButton = MainFeatureActionButton.Button(
                    icon = {
                        Text("📷")
                    },
                    label = {
                        Text(if (isCapturing) "Capturing..." else "Save Photo")
                    },
                    enabled = canCapture && !isCapturing,
                    onClick = {
                        ctx.requestCapture()
                    }
                ),
                additionalFeatureButtons = listOf(
                    // Prominent button for switching to text mode
                    AdditionalFeatureButton(
                        key = "capture_switch_text",
                        icon = { Text("📝") },
                        label = { Text("Text Mode") },
                        prominent = true,
                        onClick = {
                            // Switch to text mode
                            vm.setCaptureMode(CaptureMode.Text)
                        }
                    ),
                    // Non-prominent button for settings
                    AdditionalFeatureButton(
                        key = "capture_settings",
                        icon = { Text("⚙️") },
                        label = { Text("Capture Settings") },
                        prominent = false,
                        onClick = {
                            // TODO: Open capture settings
                        }
                    )
                )
            )
        } else {
            ChromeState(
                additionalFeatureButtons = listOf(
                    // Prominent button for switching to camera mode
                    AdditionalFeatureButton(
                        key = "capture_switch_camera",
                        icon = { Text("📷") },
                        label = { Text("Camera Mode") },
                        prominent = true,
                        onClick = {
                            // Switch to camera mode
                            vm.setCaptureMode(CaptureMode.Camera)
                        }
                    ),
                    // Non-prominent button for settings
                    AdditionalFeatureButton(
                        key = "capture_settings",
                        icon = { Text("⚙️") },
                        label = { Text("Capture Settings") },
                        prominent = false,
                        onClick = {
                            // TODO: Open capture settings
                        }
                    )
                )
            )
        }
    }

    ProvideChromeState(chromeState) {
        when (captureMode) {
            CaptureMode.Camera -> {
                DaybookCameraPreview(
                    onImageSaved = { byteArray ->
                        // Optionally save the image as a Doc
                        // For now, just log that it was saved
                        println("Image saved: ${byteArray.size} bytes")
                    },
                    onCaptureRequested = {
                        // This will be handled by the camera preview via the context
                    }
                )
            }
            else -> {
                when (docsList) {
                    is DocsListState.Error -> {
                        Text("error loading docs: ${docsList.error.message()}")
                    }

                    is DocsListState.Loading -> {
                        Text("Loading...")
                    }

                    is DocsListState.Data -> {
                        Button(
                            onClick = {
                                vm.addOne()
                            }
                        ) {
                            Text("Add")
                        }
                        Text("${docsList.docs}")
                    }
                }
            }
        }
    }
}
