package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.example.daybook.uniffi.DrawerEventListener
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.core.DrawerEvent
import org.example.daybook.uniffi.core.ListenerRegistration
import org.example.daybook.uniffi.core.UpdateDocArgs

sealed interface DocListState {
    data class Data(val docIds: List<String>) : DocListState
    data class Error(val error: FfiException) : DocListState
    object Loading : DocListState
}

class DrawerViewModel(
    val drawerRepo: DrawerRepoFfi
) : ViewModel() {
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
    
    // Internal access for optimistic updates
    internal val _selectedDocMutable = _selectedDoc
    
    private var listenerRegistration: ListenerRegistration? = null
    
    private val listener = object : DrawerEventListener {
        override fun onDrawerEvent(event: DrawerEvent) {
            viewModelScope.launch {
                when (event) {
                    is DrawerEvent.ListChanged -> refreshDocIds()
                    is DrawerEvent.DocAdded -> {
                        refreshDocIds()
                    }
                    is DrawerEvent.DocUpdated -> {
                        // Reload the document if it's already loaded
                        val currentLoaded = _loadedDocs.value
                        if (currentLoaded.containsKey(event.id)) {
                            loadDoc(event.id)
                        }
                        
                        // Update selected doc if it's the one that was updated
                        if (event.id == _selectedDocId.value) {
                            loadSelectedDoc(event.id)
                        }
                    }
                    is DrawerEvent.DocDeleted -> {
                        // Remove from loaded docs and doc IDs
                        _loadedDocs.value = _loadedDocs.value - event.id
                        refreshDocIds()
                        
                        // Clear selected doc if it was deleted
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
        refreshDocIds()
        viewModelScope.launch {
            listenerRegistration = drawerRepo.ffiRegisterListener(listener)
        }
    }
    
    fun refreshDocIds() {
        viewModelScope.launch {
            _docListState.value = DocListState.Loading
            try {
                val branches = drawerRepo.list()
                val ids = branches.map { it.docId }
                _docListState.value = DocListState.Data(ids)
            } catch (e: FfiException) {
                _docListState.value = DocListState.Error(e)
            }
        }
    }
    
    fun loadDoc(id: String) {
        // Don't load if already loaded or currently loading
        if (_loadedDocs.value.containsKey(id) || _loadingDocs.value.contains(id)) {
            return
        }
        
        viewModelScope.launch {
            _loadingDocs.value = _loadingDocs.value + id
            try {
                val doc = drawerRepo.get(id, "main")
                if (doc != null) {
                    _loadedDocs.value = _loadedDocs.value + (id to doc)
                }
            } catch (e: FfiException) {
                println("Error loading document $id: ${e.message}")
            } finally {
                _loadingDocs.value = _loadingDocs.value - id
            }
        }
    }
    
    fun loadDocs(ids: List<String>) {
        viewModelScope.launch {
            val idsToLoad = ids.filter { id ->
                !_loadedDocs.value.containsKey(id) && !_loadingDocs.value.contains(id)
            }
            if (idsToLoad.isEmpty()) return@launch
            
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
                        println("Error loading document $id: ${e.message}")
                    }
                }
                if (loaded.isNotEmpty()) {
                    _loadedDocs.value = _loadedDocs.value + loaded
                }
            } finally {
                _loadingDocs.value = _loadingDocs.value - idsToLoad
            }
        }
    }
    
    fun getDoc(id: String): Doc? {
        return _loadedDocs.value[id]
    }
    
    fun isDocLoaded(id: String): Boolean {
        return _loadedDocs.value.containsKey(id)
    }
    
    fun isDocLoading(id: String): Boolean {
        return _loadingDocs.value.contains(id)
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
            try {
                val doc = drawerRepo.get(id, "main")
                if (doc != null) {
                    _selectedDoc.value = doc
                    // Also add to loaded docs
                    _loadedDocs.value = _loadedDocs.value + (id to doc)
                }
            } catch (e: FfiException) {
                println("Error loading document $id: ${e.message}")
            }
        }
    }
    
    private var debounceJob: kotlinx.coroutines.Job? = null
    
    fun updateDoc(patch: DocPatch) {
        // Cancel previous debounce job
        debounceJob?.cancel()
        // Start new debounce job
        debounceJob = viewModelScope.launch {
            kotlinx.coroutines.delay(500) // 500ms debounce
            try {
                drawerRepo.updateBatch(listOf(UpdateDocArgs("main", null, patch)))
                // The listener will handle updating the state
            } catch (e: FfiException) {
                println("Error updating document: ${e.message}")
            }
        }
    }
    
    fun updateDocs(patches: List<DocPatch>) {
        viewModelScope.launch {
            try {
                drawerRepo.updateBatch(patches.map { UpdateDocArgs("main", null, it) })
                // The listener will handle updating the state
            } catch (e: FfiException) {
                println("Error updating documents: ${e.message}")
            }
        }
    }
    
    override fun onCleared() {
        listenerRegistration?.unregister()
        super.onCleared()
    }
}

