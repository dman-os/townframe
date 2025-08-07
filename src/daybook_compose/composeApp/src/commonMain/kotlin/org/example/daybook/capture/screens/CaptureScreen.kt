@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.capture.screens

import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.LocalContainer
import org.example.daybook.uniffi.Doc
import org.example.daybook.uniffi.DocsRepo
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.Uuid
import org.example.daybook.uniffi.DocsEvent
import org.example.daybook.uniffi.DocsListener
import org.example.daybook.uniffi.ListenerRegistration
import kotlin.time.Clock

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
    val docsRepo: DocsRepo,
    val initialMode: CaptureMode = CaptureMode.Text,
    val availableModes: Set<CaptureMode> = setOf(CaptureMode.Text),
) : ViewModel() {
    private val _captureMode = MutableStateFlow(initialMode)
    val captureMode = _captureMode.asStateFlow()

    private val _docsList = MutableStateFlow(DocsListState.Loading as DocsListState)
    val docsList = _docsList.asStateFlow()

    // Registration handle to auto-unregister
    private var listenerRegistration: ListenerRegistration? = null

    // Listener instance implemented on Kotlin side
    private val listener = object : DocsListener {
        override fun onDocsEvent(event: DocsEvent) {
            // Ensure UI updates happen on main thread
            viewModelScope.launch {
                when (event) {
                    DocsEvent.LIST_CHANGED -> {
                        // Refresh from source of truth in Rust
                        refreshDocs()
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
            listenerRegistration = docsRepo.ffiRegisterListener(listener)
        }
    }

    private suspend fun refreshDocs() {
        _docsList.value = DocsListState.Loading
        try {
            _docsList.value = DocsListState.Data(docsRepo.ffiList())
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
            docsRepo.ffiSet(
                id, Doc(id, Clock.System.now())
            )
        }
    }

    override fun onCleared() {
        // Clean up registration
        listenerRegistration?.unregister()
        super.onCleared()
    }
}

@Composable
fun CaptureScreen() {
    val docsRepo = LocalContainer.current.docsRepo
    val vm = viewModel {
        CaptureScreenViewModel(docsRepo = docsRepo)
    }

    val docsList = vm.docsList.collectAsState().value

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
