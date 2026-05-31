@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import java.util.concurrent.ConcurrentHashMap
import org.example.daybook.ui.editor.EditorSessionController
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.core.DrawerEvent
import org.example.daybook.uniffi.core.ListenerRegistration

private data class DocEditorSessionEntry(
    val controller: EditorSessionController,
    var hostCount: Int = 0,
    var lastTouchedMs: Long = Clock.System.now().toEpochMilliseconds()
)

class DocEditorStoreViewModel(
    private val drawerRepo: DrawerRepoFfi
) : ViewModel() {
    private val sessions = ConcurrentHashMap<String, DocEditorSessionEntry>()

    private val _selectedDocId = MutableStateFlow<String?>(null)
    val selectedDocId = _selectedDocId.asStateFlow()

    private val _selectedController = MutableStateFlow<EditorSessionController?>(null)
    val selectedController = _selectedController.asStateFlow()

    private var listenerRegistration: ListenerRegistration? = null
    private var registerJob: Job? = null
    private val evictionTtlMs = 10.minutes.inWholeMilliseconds

    private val listener =
        object : DrawerEventListener {
            override fun onDrawerEvent(event: DrawerEvent) {
                when (event) {
                    is DrawerEvent.DocUpdated -> {
                        if (sessions.containsKey(event.id)) {
                            viewModelScope.launch { refreshDoc(event.id) }
                        }
                    }
                    is DrawerEvent.DocDeleted -> {
                        sessions.remove(event.id)
                        if (_selectedDocId.value == event.id) {
                            _selectedDocId.value = null
                            _selectedController.value = null
                        }
                    }
                    is DrawerEvent.DocAdded -> {}
                }
            }
        }

    init {
        registerJob =
            viewModelScope.launch {
                val registration = drawerRepo.ffiRegisterListener(listener)
                if (!isActive) {
                    registration.unregister()
                    return@launch
                }
                listenerRegistration = registration
            }
        viewModelScope.launch {
            while (true) {
                delay(30_000)
                evictIdleSessions()
            }
        }
    }

    fun selectDoc(docId: String?) {
        _selectedDocId.value = docId
        if (docId == null) {
            _selectedController.value = null
            return
        }

        val entry = createSession(docId)
        entry.lastTouchedMs = nowMs()
        _selectedController.value = entry.controller
        viewModelScope.launch { refreshDoc(docId) }
    }

    fun attachHost(docId: String) {
        val entry = createSession(docId)
        entry.hostCount += 1
        entry.lastTouchedMs = nowMs()
    }

    fun detachHost(docId: String) {
        val entry = sessions[docId] ?: return
        entry.hostCount = (entry.hostCount - 1).coerceAtLeast(0)
        entry.lastTouchedMs = nowMs()
    }

    private fun createSession(docId: String): DocEditorSessionEntry {
        return sessions.computeIfAbsent(docId) {
            val controller =
                EditorSessionController(
                    drawerRepo = drawerRepo,
                    scope = viewModelScope,
                    onDocCreated = { createdId -> selectDoc(createdId) }
                )
            DocEditorSessionEntry(controller = controller)
        }
    }

    private suspend fun refreshDoc(docId: String) {
        val entry = sessions[docId] ?: return
        val bundle = drawerRepo.getBundle(docId, "main")
        entry.controller.bindDoc(bundle?.doc, bundle)
        entry.lastTouchedMs = nowMs()
    }

    private fun evictIdleSessions() {
        val selectedId = _selectedDocId.value
        val now = nowMs()
        val toRemove = mutableListOf<String>()
        for ((docId, entry) in sessions.entries) {
            if (docId == selectedId) {
                continue
            }
            if (entry.hostCount > 0) {
                continue
            }
            val state = entry.controller.state.value
            if (state.isDirty || state.isSaving) {
                continue
            }
            if (now - entry.lastTouchedMs >= evictionTtlMs) {
                toRemove += docId
            }
        }
        toRemove.forEach(sessions::remove)
    }

    private fun nowMs(): Long = Clock.System.now().toEpochMilliseconds()

    override fun onCleared() {
        registerJob?.cancel()
        listenerRegistration?.unregister()
        super.onCleared()
    }
}
