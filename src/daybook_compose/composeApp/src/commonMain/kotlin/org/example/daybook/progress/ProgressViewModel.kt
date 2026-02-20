package org.example.daybook.progress

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.ProgressEventListener
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.ProgressEvent
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressUpdateEntry

sealed interface ProgressState {
    data object Loading : ProgressState

    data class Data(
        val tasks: List<ProgressTask>,
        val selectedTaskId: String? = null,
        val selectedTaskUpdates: List<ProgressUpdateEntry> = emptyList()
    ) : ProgressState

    data class Error(val error: FfiException) : ProgressState
}

class ProgressViewModel(private val progressRepo: ProgressRepoFfi) : ViewModel() {
    private val _state = MutableStateFlow<ProgressState>(ProgressState.Loading)
    val state = _state.asStateFlow()

    private var listenerRegistration: ListenerRegistration? = null

    private val listener =
        object : ProgressEventListener {
            override fun onProgressEvent(event: ProgressEvent) {
                viewModelScope.launch {
                    when (event) {
                        is ProgressEvent.ListChanged -> refresh()
                        is ProgressEvent.TaskRemoved -> refresh()
                        is ProgressEvent.TaskUpserted -> refresh()
                        is ProgressEvent.UpdateAdded -> refresh()
                    }
                }
            }
        }

    init {
        viewModelScope.launch {
            refresh()
            listenerRegistration = progressRepo.ffiRegisterListener(listener)
        }
    }

    fun refresh() {
        viewModelScope.launch {
            val selectedTaskId = (state.value as? ProgressState.Data)?.selectedTaskId
            try {
                val tasks = progressRepo.list()
                val updates =
                    if (selectedTaskId != null) {
                        progressRepo.listUpdates(selectedTaskId)
                    } else {
                        emptyList()
                    }
                _state.value =
                    ProgressState.Data(
                        tasks = tasks,
                        selectedTaskId = selectedTaskId,
                        selectedTaskUpdates = updates
                    )
            } catch (error: FfiException) {
                _state.value = ProgressState.Error(error)
            }
        }
    }

    fun selectTask(taskId: String?) {
        viewModelScope.launch {
            val current = state.value
            if (current !is ProgressState.Data) {
                return@launch
            }

            // Commit UI selection immediately so listener-driven refreshes don't race and clear it.
            _state.value =
                current.copy(
                    selectedTaskId = taskId,
                    selectedTaskUpdates = if (taskId == null) emptyList() else current.selectedTaskUpdates
                )

            if (taskId == null) {
                return@launch
            }

            val updates =
                try {
                    progressRepo.markViewed(taskId)
                    progressRepo.listUpdates(taskId)
                } catch (_: FfiException) {
                    emptyList()
                }

            val latest = state.value
            if (latest is ProgressState.Data && latest.selectedTaskId == taskId) {
                _state.value = latest.copy(selectedTaskUpdates = updates)
            }
        }
    }

    fun dismiss(taskId: String) {
        viewModelScope.launch {
            try {
                progressRepo.dismiss(taskId)
                refresh()
            } catch (error: FfiException) {
                _state.value = ProgressState.Error(error)
            }
        }
    }

    fun clearCompleted() {
        viewModelScope.launch {
            try {
                progressRepo.clearCompleted()
                refresh()
            } catch (error: FfiException) {
                _state.value = ProgressState.Error(error)
            }
        }
    }

    override fun onCleared() {
        listenerRegistration?.unregister()
        super.onCleared()
    }
}
