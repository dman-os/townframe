package org.example.daybook.ui.editor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.example.daybook.ui.decodeJsonStringFacet
import org.example.daybook.ui.decodeNoteFacet
import org.example.daybook.ui.noteFacetJson
import org.example.daybook.ui.quoteJsonString
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.core.UpdateDocArgsV2
import org.example.daybook.uniffi.types.AddDocArgs
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.FacetKey

data class FacetEditorDescriptor(
    val facetKey: FacetKey,
    val kind: FacetEditorKind,
    val order: Int,
)

enum class FacetEditorKind {
    Image,
    Note,
    Title,
    Unsupported,
}

data class EditorSessionState(
    val doc: Doc?,
    val docId: String?,
    val titleDraft: String,
    val noteDraft: String,
    val titleEditable: Boolean,
    val noteEditable: Boolean,
    val titleNotice: String?,
    val noteNotice: String?,
    val facetRows: List<Pair<FacetKey, String>>,
    val visibleEditors: List<FacetEditorDescriptor>,
    val isDirty: Boolean,
    val isSaving: Boolean,
    val saveError: String?,
)

class EditorSessionController(
    private val drawerRepo: DrawerRepoFfi,
    private val scope: CoroutineScope,
    private val onDocCreated: ((String) -> Unit)? = null,
) {
    private val _state =
        MutableStateFlow(
            EditorSessionState(
                doc = null,
                docId = null,
                titleDraft = "",
                noteDraft = "",
                titleEditable = true,
                noteEditable = true,
                titleNotice = null,
                noteNotice = null,
                facetRows = emptyList(),
                visibleEditors = emptyList(),
                isDirty = false,
                isSaving = false,
                saveError = null,
            )
        )
    val state: StateFlow<EditorSessionState> = _state.asStateFlow()

    private var saveDebounceJob: Job? = null

    fun bindDoc(doc: Doc?) {
        _state.update {
            val titleRawValue = doc?.facets?.get(titleFacetKey())
            val (nextTitle, titleEditable, titleNotice) =
                if (titleRawValue == null) {
                    Triple("", true, null)
                } else {
                    val decodeResult = decodeJsonStringFacet(titleRawValue)
                    if (decodeResult.isSuccess) {
                        Triple(decodeResult.getOrThrow(), true, null)
                    } else {
                        Triple(
                            org.example.daybook.ui.dequoteJson(titleRawValue),
                            false,
                            "Invalid title facet payload; editing disabled to avoid destructive writes.",
                        )
                    }
                }

            val noteRawValue = doc?.facets?.get(noteFacetKey())
            val (nextNote, noteEditable, noteNotice) =
                if (noteRawValue == null) {
                    Triple("", true, null)
                } else {
                    val decodeResult = decodeNoteFacet(noteRawValue)
                    if (decodeResult.isFailure) {
                        Triple(
                            org.example.daybook.ui.dequoteJson(noteRawValue),
                            false,
                            "Invalid note facet payload; editing disabled to avoid destructive writes.",
                        )
                    } else {
                        val note = decodeResult.getOrThrow()
                        if (note.mime == "text/plain") {
                            Triple(note.content, true, null)
                        } else {
                            Triple(
                                note.content,
                                false,
                                "Unsupported note mime '${note.mime}'; editing disabled to avoid destructive writes.",
                            )
                        }
                    }
                }
            it.copy(
                doc = doc,
                docId = doc?.id,
                titleDraft = nextTitle,
                noteDraft = nextNote,
                titleEditable = titleEditable,
                noteEditable = noteEditable,
                titleNotice = titleNotice,
                noteNotice = noteNotice,
                facetRows = doc?.facets?.entries?.sortedBy { entry -> facetKeyString(entry.key) }?.map { entry -> entry.key to entry.value } ?: emptyList(),
                visibleEditors = buildVisibleEditors(doc),
                isDirty = false,
                saveError = null,
            )
        }
    }

    fun setTitleDraft(value: String) {
        if (!_state.value.titleEditable) {
            return
        }
        _state.update { it.copy(titleDraft = value, isDirty = true) }
        scheduleSave()
    }

    fun setNoteDraft(value: String) {
        if (!_state.value.noteEditable) {
            return
        }
        _state.update { it.copy(noteDraft = value, isDirty = true) }
        scheduleSave()
    }

    fun commitNow() {
        saveDebounceJob?.cancel()
        scope.launch {
            persist()
        }
    }

    private fun scheduleSave() {
        saveDebounceJob?.cancel()
        saveDebounceJob =
            scope.launch {
                delay(500)
                persist()
            }
    }

    private suspend fun persist() {
        val snapshot = _state.value
        if (!snapshot.isDirty) {
            return
        }

        _state.update { it.copy(isSaving = true, saveError = null) }
        try {
            val facetsSet = mutableMapOf<FacetKey, String>()
            val facetsRemove = mutableListOf<FacetKey>()

            if (snapshot.titleEditable) {
                if (snapshot.titleDraft.isBlank()) {
                    facetsRemove.add(titleFacetKey())
                } else {
                    facetsSet[titleFacetKey()] = quoteJsonString(snapshot.titleDraft)
                }
            }

            if (snapshot.noteEditable) {
                facetsSet[noteFacetKey()] = noteFacetJson(snapshot.noteDraft)
            }

            val currentDocId = snapshot.docId
            if (currentDocId == null) {
                val addedId =
                    drawerRepo.add(
                        AddDocArgs(
                            branchPath = "main",
                            facets = facetsSet,
                            userPath = null,
                        )
                    )
                onDocCreated?.invoke(addedId)
                val reloadedDoc = drawerRepo.get(addedId, "main")
                bindDoc(reloadedDoc)
            } else {
                val patch =
                    DocPatch(
                        id = currentDocId,
                        facetsSet = facetsSet,
                        facetsRemove = facetsRemove,
                        userPath = null,
                    )
                drawerRepo.updateBatch(listOf(UpdateDocArgsV2("main", null, patch)))
                val reloadedDoc = drawerRepo.get(currentDocId, "main")
                bindDoc(reloadedDoc)
            }
            _state.update { it.copy(isSaving = false, saveError = null, isDirty = false) }
        } catch (error: Throwable) {
            _state.update { it.copy(isSaving = false, saveError = error.message ?: "Failed to save") }
        }
    }

    private fun buildVisibleEditors(doc: Doc?): List<FacetEditorDescriptor> {
        val rows = mutableListOf<FacetEditorDescriptor>()
        if (doc != null) {
            rows.add(FacetEditorDescriptor(titleFacetKey(), FacetEditorKind.Title, 0))
            if (doc.facets.containsKey(blobFacetKey()) || doc.facets.containsKey(imageMetadataFacetKey())) {
                rows.add(FacetEditorDescriptor(blobFacetKey(), FacetEditorKind.Image, 1))
            }
            rows.add(FacetEditorDescriptor(noteFacetKey(), FacetEditorKind.Note, 2))
        } else {
            rows.add(FacetEditorDescriptor(titleFacetKey(), FacetEditorKind.Title, 0))
            rows.add(FacetEditorDescriptor(noteFacetKey(), FacetEditorKind.Note, 2))
        }
        return rows.sortedBy { descriptor -> descriptor.order }
    }

    private fun facetKeyString(key: FacetKey): String {
        val tagString =
            when (val tag = key.tag) {
                is org.example.daybook.uniffi.types.FacetTag.WellKnown -> tag.v1.name.lowercase()
                is org.example.daybook.uniffi.types.FacetTag.Any -> tag.v1
            }
        return if (key.id == "main") tagString else "$tagString:${key.id}"
    }
}
