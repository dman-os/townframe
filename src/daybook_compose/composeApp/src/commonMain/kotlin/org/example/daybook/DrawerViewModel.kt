package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.core.DrawerEvent
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.UpdateDocArgsV2
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocPatch

sealed interface DocListState {
    data class Data(val docIds: List<String>) : DocListState

    data class Error(val error: FfiException) : DocListState

    object Loading : DocListState
}

private data class DrawerRefreshIntent(
    val refreshList: Boolean = false,
    val refreshDocIds: Set<String> = emptySet(),
    val refreshSelectedDoc: Boolean = false
) {
    fun merge(other: DrawerRefreshIntent): DrawerRefreshIntent =
        DrawerRefreshIntent(
            refreshList = refreshList || other.refreshList,
            refreshDocIds = refreshDocIds + other.refreshDocIds,
            refreshSelectedDoc = refreshSelectedDoc || other.refreshSelectedDoc
        )

    companion object {
        val ListOnly = DrawerRefreshIntent(refreshList = true)
    }
}

class DrawerViewModel(val drawerRepo: DrawerRepoFfi) : ViewModel() {
    // Document IDs list (loaded lazily)
    private val _docListState = MutableStateFlow<DocListState>(DocListState.Loading)
    val docListState = _docListState.asStateFlow()

    // Map of loaded documents (only documents that have been loaded)
    private val _loadedDocs = MutableStateFlow<Map<String, Doc>>(emptyMap())
    val loadedDocs = _loadedDocs.asStateFlow()

    // Set of document IDs currently being loaded
    private val _loadingDocs = MutableStateFlow<Set<String>>(emptySet())
    val loadingDocs = _loadingDocs.asStateFlow()

    private val _selectedDocId = MutableStateFlow<String?>(null)
    val selectedDocId = _selectedDocId.asStateFlow()

    private val _selectedDoc = MutableStateFlow<Doc?>(null)
    val selectedDoc = _selectedDoc.asStateFlow()

    private val _error = MutableStateFlow<FfiException?>(null)
    val error = _error.asStateFlow()

    // Internal access for optimistic updates
    internal val _selectedDocMutable = _selectedDoc

    private var listenerRegistration: ListenerRegistration? = null

    private val refreshRunner =
        CoalescingIntentRunner<DrawerRefreshIntent>(
            scope = viewModelScope,
            debounceMs = 120,
            merge = { left: DrawerRefreshIntent, right: DrawerRefreshIntent -> left.merge(right) },
            onIntent = { intent: DrawerRefreshIntent -> applyRefreshIntent(intent) }
        )

    private val listener =
        object : DrawerEventListener {
            override fun onDrawerEvent(event: DrawerEvent) {
                viewModelScope.launch {
                    when (event) {
                        is DrawerEvent.ListChanged -> {
                            refreshRunner.submit(DrawerRefreshIntent.ListOnly)
                        }

                        is DrawerEvent.DocAdded -> {
                            refreshRunner.submit(DrawerRefreshIntent.ListOnly)
                        }

                        is DrawerEvent.DocUpdated -> {
                            val shouldRefreshLoaded = _loadedDocs.value.containsKey(event.id)
                            val shouldRefreshSelected = event.id == _selectedDocId.value
                            refreshRunner.submit(
                                DrawerRefreshIntent(
                                    refreshDocIds = if (shouldRefreshLoaded) setOf(event.id) else emptySet(),
                                    refreshSelectedDoc = shouldRefreshSelected
                                )
                            )
                        }

                        is DrawerEvent.DocDeleted -> {
                            _loadedDocs.value = _loadedDocs.value - event.id
                            refreshRunner.submit(DrawerRefreshIntent.ListOnly)

                            if (event.id == _selectedDocId.value) {
                                _selectedDocId.value = null
                                _selectedDoc.value = null
                            }
                        }
                    }
                }
            }
        }

    init {
        refreshRunner.submit(DrawerRefreshIntent.ListOnly)
        viewModelScope.launch {
            listenerRegistration = drawerRepo.ffiRegisterListener(listener)
        }
    }

    fun refreshDocIds() {
        refreshRunner.submit(DrawerRefreshIntent.ListOnly)
    }

    fun loadDoc(id: String) {
        viewModelScope.launch {
            loadDocsInternal(listOf(id), force = false)
        }
    }

    fun loadDocs(ids: List<String>) {
        viewModelScope.launch {
            loadDocsInternal(ids, force = false)
        }
    }

    fun getDoc(id: String): Doc? = _loadedDocs.value[id]

    fun isDocLoaded(id: String): Boolean = _loadedDocs.value.containsKey(id)

    fun isDocLoading(id: String): Boolean = _loadingDocs.value.contains(id)

    private fun emitError(error: FfiException) {
        _error.value = error
    }

    fun selectDoc(id: String?) {
        _selectedDocId.value = id
        if (id != null) {
            viewModelScope.launch {
                refreshSelectedDoc(id)
            }
        } else {
            _selectedDoc.value = null
        }
    }

    private suspend fun applyRefreshIntent(intent: DrawerRefreshIntent) {
        if (intent.refreshList) {
            refreshDocIdsNow()
        }

        if (intent.refreshDocIds.isNotEmpty()) {
            loadDocsInternal(intent.refreshDocIds.toList(), force = true)
        }

        if (intent.refreshSelectedDoc) {
            _selectedDocId.value?.let { id ->
                refreshSelectedDoc(id)
            }
        }
    }

    private suspend fun refreshDocIdsNow() {
        val hadData = _docListState.value is DocListState.Data
        if (!hadData) {
            _docListState.value = DocListState.Loading
        }

        try {
            val branches = drawerRepo.list()
            val ids = branches.map { it.docId }
            _docListState.value = DocListState.Data(ids)
        } catch (e: FfiException) {
            _docListState.value = DocListState.Error(e)
        }
    }

    private suspend fun loadDocsInternal(ids: List<String>, force: Boolean) {
        val uniqueIds = ids.distinct()
        val currentlyLoading = _loadingDocs.value
        val currentLoaded = _loadedDocs.value

        val idsToLoad =
            uniqueIds.filter { id ->
                !currentlyLoading.contains(id) && (force || !currentLoaded.containsKey(id))
            }

        if (idsToLoad.isEmpty()) {
            return
        }

        _loadingDocs.value = _loadingDocs.value + idsToLoad

        try {
            val loaded = mutableMapOf<String, Doc>()
            idsToLoad.forEach { id ->
                try {
                    val doc = drawerRepo.get(id, "main")
                    if (doc != null) {
                        loaded[id] = doc
                    }
                } catch (e: FfiException) {
                    emitError(e)
                }
            }

            if (loaded.isNotEmpty()) {
                _loadedDocs.value = _loadedDocs.value + loaded
            }
        } finally {
            _loadingDocs.value = _loadingDocs.value - idsToLoad.toSet()
        }
    }

    private suspend fun refreshSelectedDoc(id: String) {
        try {
            val doc = drawerRepo.get(id, "main")
            if (doc != null) {
                _selectedDoc.value = doc
                _loadedDocs.value = _loadedDocs.value + (id to doc)
            }
        } catch (e: FfiException) {
            emitError(e)
        }
    }

    private var debounceJob: Job? = null

    fun updateDoc(patch: DocPatch) {
        debounceJob?.cancel()
        debounceJob =
            viewModelScope.launch {
                kotlinx.coroutines.delay(500)
                try {
                    drawerRepo.updateBatch(listOf(UpdateDocArgsV2("main", null, patch)))
                } catch (e: FfiException) {
                    emitError(e)
                }
            }
    }

    fun updateDocs(patches: List<DocPatch>) {
        viewModelScope.launch {
            try {
                drawerRepo.updateBatch(patches.map { UpdateDocArgsV2("main", null, it) })
            } catch (e: FfiException) {
                emitError(e)
            }
        }
    }

    override fun onCleared() {
        refreshRunner.cancel()
        listenerRegistration?.unregister()
        super.onCleared()
    }
}
