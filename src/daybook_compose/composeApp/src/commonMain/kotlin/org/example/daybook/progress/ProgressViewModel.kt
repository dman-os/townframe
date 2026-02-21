package org.example.daybook.progress

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.CoalescingIntentRunner
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

private data class ProgressRefreshIntent(
    val listChanged: Boolean = false,
    val touchedTaskIds: Set<String> = emptySet()
) {
    fun merge(other: ProgressRefreshIntent): ProgressRefreshIntent =
        ProgressRefreshIntent(
            listChanged = listChanged || other.listChanged,
            touchedTaskIds = touchedTaskIds + other.touchedTaskIds
        )

    companion object {
        val ListOnly = ProgressRefreshIntent(listChanged = true)
    }
}

class ProgressViewModel(private val progressRepo: ProgressRepoFfi) : ViewModel() {
    private val _state = MutableStateFlow<ProgressState>(ProgressState.Loading)
    val state = _state.asStateFlow()

    private var listenerRegistration: ListenerRegistration? = null

    private val refreshRunner =
        CoalescingIntentRunner<ProgressRefreshIntent>(
            scope = viewModelScope,
            debounceMs = 80,
            merge = { left: ProgressRefreshIntent, right: ProgressRefreshIntent -> left.merge(right) },
            onIntent = { intent: ProgressRefreshIntent -> applyRefreshIntent(intent) }
        )

    private val listener =
        object : ProgressEventListener {
            override fun onProgressEvent(event: ProgressEvent) {
                when (event) {
                    is ProgressEvent.ListChanged -> refreshRunner.submit(ProgressRefreshIntent.ListOnly)
                    is ProgressEvent.TaskRemoved ->
                        refreshRunner.submit(
                            ProgressRefreshIntent(
                                listChanged = true,
                                touchedTaskIds = setOf(event.id)
                            )
                        )

                    is ProgressEvent.TaskUpserted ->
                        refreshRunner.submit(
                            ProgressRefreshIntent(
                                touchedTaskIds = setOf(event.id)
                            )
                        )

                    is ProgressEvent.UpdateAdded ->
                        refreshRunner.submit(
                            ProgressRefreshIntent(
                                touchedTaskIds = setOf(event.id)
                            )
                        )
                }
            }
        }

    init {
        viewModelScope.launch {
            refreshRunner.submit(ProgressRefreshIntent.ListOnly)
            listenerRegistration = progressRepo.ffiRegisterListener(listener)
        }
    }

    private suspend fun applyRefreshIntent(intent: ProgressRefreshIntent) {
        val current = _state.value as? ProgressState.Data
        val selectedTaskId = current?.selectedTaskId
        val selectedTaskTouched = selectedTaskId != null && intent.touchedTaskIds.contains(selectedTaskId)
        val shouldRefreshSelectedUpdates = selectedTaskId != null && (intent.listChanged || selectedTaskTouched)

        try {
            val tasks = progressRepo.list()
            val updates =
                if (shouldRefreshSelectedUpdates && selectedTaskId != null) {
                    progressRepo.listUpdates(selectedTaskId)
                } else {
                    current?.selectedTaskUpdates ?: emptyList()
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

    fun refresh() {
        refreshRunner.submit(ProgressRefreshIntent.ListOnly)
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
        refreshRunner.cancel()
        listenerRegistration?.unregister()
        super.onCleared()
    }
}
