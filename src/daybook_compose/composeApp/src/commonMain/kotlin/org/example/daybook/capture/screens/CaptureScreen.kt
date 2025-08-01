package org.example.daybook.capture.screens

import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.viewModelFactory
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.LocalContainer
import org.example.daybook.uniffi.Doc
import org.example.daybook.uniffi.DocsRepo
import org.example.daybook.uniffi.FfiException

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

    init {
        loadLatestDocs()
    }

    fun loadLatestDocs() {
        viewModelScope.launch {
            _docsList.value = DocsListState.Loading
            try {
                _docsList.value = DocsListState.Data(docsRepo.list())
            } catch (err: FfiException) {
                _docsList.value = DocsListState.Error(err)
            }
        }
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
            Text("${docsList.docs}")
        }
    }
}
