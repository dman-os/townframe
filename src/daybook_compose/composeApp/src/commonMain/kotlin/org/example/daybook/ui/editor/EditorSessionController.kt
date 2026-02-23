@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.ui.editor

import kotlin.uuid.Uuid
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.example.daybook.ui.buildBodyFacet
import org.example.daybook.ui.buildNoteFacet
import org.example.daybook.ui.buildSelfFacetRefUrl
import org.example.daybook.ui.decodeJsonString
import org.example.daybook.ui.decodeJsonStringOrRaw
import org.example.daybook.ui.decodeWellKnownFacet
import org.example.daybook.ui.encodeJsonString
import org.example.daybook.ui.putWellKnownFacet
import org.example.daybook.ui.stripFacetRefFragment
import org.example.daybook.ui.withFacetRefCommitHeads
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.core.DocEntry
import org.example.daybook.uniffi.core.UpdateDocArgsV2
import org.example.daybook.uniffi.types.AddDocArgs
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacet
import org.example.daybook.uniffi.types.WellKnownFacetTag

data class NoteFacetEditorState(
    val draft: String,
    val editable: Boolean,
    val notice: String?,
)

enum class FacetEditorKind {
    ImageMetadata,
    Note,
    GenericJson,
}

data class FacetViewDescriptor(
    val facetKey: FacetKey,
    val kind: FacetEditorKind,
    val rawValue: String,
    val isPrimary: Boolean = false,
)

data class ScrollToFacetRequest(
    val facetKey: FacetKey,
    val seq: Long,
)

data class EditorSessionState(
    val doc: Doc?,
    val docId: String?,
    val titleDraft: String,
    val titleEditable: Boolean,
    val titleNotice: String?,
    val noteEditors: Map<FacetKey, NoteFacetEditorState>,
    val facetRows: List<Pair<FacetKey, String>>,
    val contentFacetViews: List<FacetViewDescriptor>,
    val docWarnings: List<String>,
    val scrollToFacetRequest: ScrollToFacetRequest?,
    val isDirty: Boolean,
    val isSaving: Boolean,
    val saveError: String?,
)

class EditorSessionController(
    private val drawerRepo: DrawerRepoFfi,
    private val scope: CoroutineScope,
    private val onDocCreated: ((String) -> Unit)? = null,
) {
    private var persistedDocSnapshot: Doc? = null
    private var facetHeadsByKeyString: Map<String, List<String>> = emptyMap()
    private var mainBranchHeads: List<String> = emptyList()

    private val _state =
        MutableStateFlow(
            EditorSessionState(
                doc = null,
                docId = null,
                titleDraft = "",
                titleEditable = true,
                titleNotice = null,
                noteEditors = mapOf(noteFacetKey() to NoteFacetEditorState("", true, null)),
                facetRows = emptyList(),
                contentFacetViews = listOf(
                    FacetViewDescriptor(noteFacetKey(), FacetEditorKind.Note, "")
                ),
                docWarnings = emptyList(),
                scrollToFacetRequest = null,
                isDirty = false,
                isSaving = false,
                saveError = null,
            )
        )
    val state: StateFlow<EditorSessionState> = _state.asStateFlow()

    private var saveDebounceJob: Job? = null
    private var nextScrollRequestSeq: Long = 1

    fun bindDoc(doc: Doc?, entry: DocEntry? = null) {
        persistedDocSnapshot = doc
        facetHeadsByKeyString = entry?.facetBlames?.mapValues { (_, blame) -> blame.heads } ?: emptyMap()
        mainBranchHeads = entry?.branches?.get("main").orEmpty()
        val nextState = buildBoundState(doc)
        _state.value = nextState
    }

    fun setTitleDraft(value: String) {
        if (!_state.value.titleEditable) {
            return
        }
        _state.update { it.copy(titleDraft = value, isDirty = true, saveError = null) }
        scheduleSave()
    }

    fun setNoteDraft(facetKey: FacetKey, value: String) {
        val editorState = _state.value.noteEditors[facetKey] ?: return
        if (!editorState.editable) {
            return
        }
        _state.update { current ->
            current.copy(
                noteEditors = current.noteEditors.toMutableMap().also { map ->
                    map[facetKey] = editorState.copy(draft = value)
                },
                isDirty = true,
                saveError = null,
            )
        }
        scheduleSave()
    }

    fun addNoteFacetAfter(anchorFacetKey: FacetKey) {
        val snapshot = _state.value
        val doc = snapshot.doc ?: return
        val newFacetKey = FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), "note-${bs58UuidId()}")
        val nextFacets = doc.facets.toMutableMap()
        putWellKnownFacet(nextFacets, newFacetKey, buildNoteFacet(""))
        writeBodyOrder(
            facets = nextFacets,
            orderUrls = insertNoteAfter(
                doc = doc,
                currentFacets = nextFacets,
                anchorFacetKey = anchorFacetKey,
                newFacetKey = newFacetKey,
            ),
        )

        val nextDoc = doc.copy(facets = nextFacets)
        val nextNoteEditors = snapshot.noteEditors.toMutableMap()
        nextNoteEditors[newFacetKey] = NoteFacetEditorState("", true, null)
        updateLocalDoc(nextDoc, nextNoteEditors)
        scheduleSave()
    }

    fun makeFacetPrimary(facetKey: FacetKey) {
        val snapshot = _state.value
        val doc = snapshot.doc ?: return
        val nextFacets = doc.facets.toMutableMap()
        val selectedRef = bodyFacetRefUrlForWrite(facetKey)
        val currentUrls = decodeBodyOrderUrls(doc)
            ?: defaultBodyOrderUrls(doc, currentFacets = nextFacets)

        val reordered = makePrimaryOrderUrls(selectedRef, currentUrls)

        writeBodyOrder(nextFacets, reordered)
        val nextDoc = doc.copy(facets = nextFacets)
        updateLocalDoc(nextDoc, snapshot.noteEditors)
        scheduleSave()
    }

    fun moveFacetEarlier(facetKey: FacetKey) {
        moveFacetRelative(facetKey = facetKey, direction = -1)
    }

    fun moveFacetLater(facetKey: FacetKey) {
        moveFacetRelative(facetKey = facetKey, direction = +1)
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
            val nextFacets = snapshot.doc?.facets?.toMutableMap() ?: mutableMapOf()

            if (snapshot.titleEditable) {
                if (snapshot.titleDraft.isBlank()) {
                    nextFacets.remove(titleFacetKey())
                } else {
                    nextFacets[titleFacetKey()] = encodeJsonString(snapshot.titleDraft)
                }
            }

            for ((noteKey, noteState) in snapshot.noteEditors) {
                if (!noteState.editable) {
                    continue
                }
                putWellKnownFacet(nextFacets, noteKey, buildNoteFacet(noteState.draft))
            }

            val currentDocId = snapshot.docId
            if (currentDocId == null) {
                val addedId =
                    drawerRepo.add(
                        AddDocArgs(
                            branchPath = "main",
                            facets = nextFacets,
                            userPath = null,
                        )
                    )
                onDocCreated?.invoke(addedId)
                bindDoc(drawerRepo.get(addedId, "main"), drawerRepo.getEntry(addedId))
                return
            }

            val oldDoc = persistedDocSnapshot ?: Doc(currentDocId, emptyMap())
            val patch = diffDocs(oldDoc, Doc(currentDocId, nextFacets))
            if (!patch.facetsSet.isEmpty() || !patch.facetsRemove.isEmpty()) {
                drawerRepo.updateBatch(listOf(UpdateDocArgsV2("main", null, patch)))
            }
            bindDoc(drawerRepo.get(currentDocId, "main"), drawerRepo.getEntry(currentDocId))
        } catch (error: Throwable) {
            _state.update { it.copy(isSaving = false, saveError = error.message ?: "Failed to save") }
        }
    }

    private fun buildBoundState(doc: Doc?): EditorSessionState {
        val titleRawValue = doc?.facets?.get(titleFacetKey())
        val (nextTitle, titleEditable, titleNotice) =
            if (titleRawValue == null) {
                Triple("", true, null)
            } else {
                val decodeResult = decodeJsonString(titleRawValue)
                if (decodeResult.isSuccess) {
                    Triple(decodeResult.getOrThrow(), true, null)
                } else {
                    Triple(
                        decodeJsonStringOrRaw(titleRawValue),
                        false,
                        "Invalid title facet payload; editing disabled to avoid destructive writes.",
                    )
                }
            }

        val noteEditors = buildNoteEditors(doc)
        return EditorSessionState(
            doc = doc,
            docId = doc?.id,
            titleDraft = nextTitle,
            titleEditable = titleEditable,
            titleNotice = titleNotice,
            noteEditors = noteEditors,
            facetRows = buildSupportedFacetRows(doc),
            contentFacetViews = buildContentFacetViews(doc),
            docWarnings = collectDocWarnings(doc),
            scrollToFacetRequest = null,
            isDirty = false,
            isSaving = false,
            saveError = null,
        )
    }

    private fun updateLocalDoc(
        nextDoc: Doc,
        nextNoteEditors: Map<FacetKey, NoteFacetEditorState>,
        scrollToFacetKey: FacetKey? = null,
    ) {
        _state.update { current ->
            current.copy(
                doc = nextDoc,
                docId = nextDoc.id,
                noteEditors = nextNoteEditors,
                facetRows = buildSupportedFacetRows(nextDoc),
                contentFacetViews = buildContentFacetViews(nextDoc),
                docWarnings = collectDocWarnings(nextDoc),
                scrollToFacetRequest = scrollToFacetKey?.let(::newScrollToFacetRequest),
                isDirty = true,
                saveError = null,
            )
        }
    }

    private fun buildNoteEditors(doc: Doc?): Map<FacetKey, NoteFacetEditorState> {
        if (doc == null) {
            return mapOf(noteFacetKey() to NoteFacetEditorState("", true, null))
        }

        val out = linkedMapOf<FacetKey, NoteFacetEditorState>()
        for ((facetKey, rawValue) in doc.facets.entries.sortedBy { (key, _) -> facetKeyString(key) }) {
            val tag = (facetKey.tag as? FacetTag.WellKnown)?.v1 ?: continue
            if (tag != WellKnownFacetTag.NOTE) {
                continue
            }
            val decodeResult = decodeWellKnownFacet<WellKnownFacet.Note>(rawValue)
            out[facetKey] =
                if (decodeResult.isFailure) {
                    NoteFacetEditorState(
                        draft = decodeJsonStringOrRaw(rawValue),
                        editable = false,
                        notice = "Invalid note facet payload; editing disabled to avoid destructive writes.",
                    )
                } else {
                    val note = decodeResult.getOrThrow().v1
                    if (note.mime == "text/plain") {
                        NoteFacetEditorState(note.content, true, null)
                    } else {
                        NoteFacetEditorState(
                            draft = note.content,
                            editable = false,
                            notice = "Unsupported note mime '${note.mime}'; editing disabled to avoid destructive writes.",
                        )
                    }
                }
        }

        if (out.isEmpty()) {
            out[noteFacetKey()] = NoteFacetEditorState("", true, null)
        }
        return out
    }

    private fun buildContentFacetViews(doc: Doc?): List<FacetViewDescriptor> {
        if (doc == null) {
            return listOf(FacetViewDescriptor(noteFacetKey(), FacetEditorKind.Note, "", isPrimary = true))
        }

        val excludedKeys = setOf(titleFacetKey(), bodyFacetKey(), dmetaFacetKey())
        val displayableKeys = doc.facets.keys
            .filter { key -> key !in excludedKeys }
            .filter(::hasSupportedFacetView)
            .sortedBy(::facetKeyString)

        val bodyOrderUrls = decodeBodyOrderUrls(doc).orEmpty()
        val orderedKeys = orderFacetKeysForDisplay(displayableKeys, bodyOrderUrls)
        val primaryFacetKey = primaryFacetKeyForDisplay(displayableKeys, bodyOrderUrls)

        return orderedKeys.mapNotNull { key ->
            val rawValue = doc.facets[key] ?: return@mapNotNull null
            FacetViewDescriptor(
                facetKey = key,
                kind = facetKindForKey(key),
                rawValue = rawValue,
                isPrimary = (key == primaryFacetKey),
            )
        }
    }

    private fun buildSupportedFacetRows(doc: Doc?): List<Pair<FacetKey, String>> {
        if (doc == null) {
            return emptyList()
        }
        val excludedKeys = setOf(titleFacetKey(), bodyFacetKey(), dmetaFacetKey())
        return doc.facets.entries
            .filter { (key, _) -> key !in excludedKeys }
            .filter { (key, _) -> hasSupportedFacetView(key) }
            .sortedBy { (key, _) -> facetKeyString(key) }
            .map { it.key to it.value }
    }

    private fun facetKindForKey(key: FacetKey): FacetEditorKind {
        val tag = (key.tag as? FacetTag.WellKnown)?.v1
        return when (tag) {
            WellKnownFacetTag.NOTE -> FacetEditorKind.Note
            WellKnownFacetTag.IMAGE_METADATA -> FacetEditorKind.ImageMetadata
            else -> FacetEditorKind.GenericJson
        }
    }

    private fun hasSupportedFacetView(key: FacetKey): Boolean =
        when (facetKindForKey(key)) {
            FacetEditorKind.Note, FacetEditorKind.ImageMetadata -> true
            FacetEditorKind.GenericJson -> false
        }

    private fun decodeBodyOrderUrls(doc: Doc): List<String>? {
        val raw = doc.facets[bodyFacetKey()] ?: return null
        val decoded = decodeWellKnownFacet<WellKnownFacet.Body>(raw)
        if (decoded.isFailure) {
            println(
                "Failed to decode Body facet in editor ordering: docId=${doc.id} facetKey=${bodyFacetKey()} error=${decoded.exceptionOrNull()}"
            )
            return null
        }
        val body = decoded.getOrThrow().v1
        return body.order.filter { it.isNotBlank() }
    }

    private fun collectDocWarnings(doc: Doc?): List<String> {
        if (doc == null) return emptyList()
        val warnings = mutableListOf<String>()

        val titleRaw = doc.facets[titleFacetKey()]
        if (titleRaw != null) {
            val decodedTitle = decodeJsonString(titleRaw)
            if (decodedTitle.isFailure) {
                val message = decodedTitle.exceptionOrNull()?.message ?: "unknown error"
                warnings += "Failed to parse title facet. $message"
            }
        }

        val bodyRaw = doc.facets[bodyFacetKey()]
        if (bodyRaw != null) {
            val decoded = decodeWellKnownFacet<WellKnownFacet.Body>(bodyRaw)
            if (decoded.isFailure) {
                val message = decoded.exceptionOrNull()?.message ?: "unknown error"
                warnings += "Failed to parse Body facet; facet ordering actions will fall back. $message"
            }
        }

        for ((facetKey, rawValue) in doc.facets.entries.sortedBy { (key, _) -> facetKeyString(key) }) {
            when ((facetKey.tag as? FacetTag.WellKnown)?.v1) {
                WellKnownFacetTag.NOTE -> {
                    val decoded = decodeWellKnownFacet<WellKnownFacet.Note>(rawValue)
                    if (decoded.isFailure) {
                        val message = decoded.exceptionOrNull()?.message ?: "unknown error"
                        warnings += "Failed to parse note facet '${facetKeyString(facetKey)}'. $message"
                    }
                }

                WellKnownFacetTag.IMAGE_METADATA -> {
                    val imageDecoded = decodeWellKnownFacet<WellKnownFacet.ImageMetadata>(rawValue)
                    if (imageDecoded.isFailure) {
                        val message = imageDecoded.exceptionOrNull()?.message ?: "unknown error"
                        warnings +=
                            "Failed to parse image metadata facet '${facetKeyString(facetKey)}'. $message"
                        continue
                    }
                    val imageMeta = imageDecoded.getOrThrow().v1
                    val blobKey =
                        doc.facets.keys.firstOrNull { key ->
                            stripFacetRefFragment(buildSelfFacetRefUrl(key)) ==
                                stripFacetRefFragment(imageMeta.facetRef)
                        }
                    if (blobKey == null) {
                        warnings +=
                            "Image facet '${facetKeyString(facetKey)}' references missing blob facet."
                        continue
                    }
                    val blobRaw = doc.facets[blobKey]
                    if (blobRaw == null) {
                        warnings +=
                            "Image facet '${facetKeyString(facetKey)}' references missing blob facet payload."
                        continue
                    }
                    val blobDecoded = decodeWellKnownFacet<WellKnownFacet.Blob>(blobRaw)
                    if (blobDecoded.isFailure) {
                        val message = blobDecoded.exceptionOrNull()?.message ?: "unknown error"
                        warnings +=
                            "Failed to parse referenced blob facet '${facetKeyString(blobKey)}' for image '${facetKeyString(facetKey)}'. $message"
                    }
                }

                else -> Unit
            }
        }
        return warnings
    }

    private fun primaryFacetKeyForDisplay(
        displayableKeys: List<FacetKey>,
        bodyOrderUrls: List<String>,
    ): FacetKey? {
        val availableByRef =
            displayableKeys.associateBy { key -> stripFacetRefFragment(buildSelfFacetRefUrl(key)) }
        for (url in bodyOrderUrls) {
            val key = availableByRef[stripFacetRefFragment(url)] ?: continue
            return key
        }
        return displayableKeys.firstOrNull()
    }

    private fun defaultBodyOrderUrls(doc: Doc, currentFacets: Map<FacetKey, String> = doc.facets): List<String> {
        return currentFacets.keys
            .filter { key -> key != titleFacetKey() && key != bodyFacetKey() }
            .sortedBy(::facetKeyString)
            .map(::bodyFacetRefUrlForWrite)
    }

    private fun writeBodyOrder(facets: MutableMap<FacetKey, String>, orderUrls: List<String>) {
        putWellKnownFacet(facets, bodyFacetKey(), buildBodyFacet(orderUrls))
    }

    private fun insertNoteAfter(
        doc: Doc,
        currentFacets: Map<FacetKey, String>,
        anchorFacetKey: FacetKey,
        newFacetKey: FacetKey,
    ): List<String> {
        val anchorRef = bodyFacetRefUrlForWrite(anchorFacetKey)
        val newRef = bodyFacetRefUrlForWrite(newFacetKey)
        val baseUrls = decodeBodyOrderUrls(doc) ?: defaultBodyOrderUrls(doc, currentFacets)
        return insertRefAfterOrderUrls(baseUrls, anchorRef, newRef)
    }

    private fun bodyFacetRefUrlForWrite(facetKey: FacetKey): String {
        val base = buildSelfFacetRefUrl(facetKey)
        val facetKeyString = facetKeyRefPathString(facetKey)
        val heads = facetHeadsByKeyString[facetKeyString] ?: mainBranchHeads
        return withFacetRefCommitHeads(base, heads)
    }

    private fun moveFacetRelative(facetKey: FacetKey, direction: Int) {
        val snapshot = _state.value
        val doc = snapshot.doc ?: return
        val nextFacets = doc.facets.toMutableMap()
        val selectedRef = bodyFacetRefUrlForWrite(facetKey)
        val baseUrls = decodeBodyOrderUrls(doc).orEmpty()
        val seenFacetRefs =
            snapshot.contentFacetViews.map { descriptor -> bodyFacetRefUrlForWrite(descriptor.facetKey) }
        val reordered =
            reorderBodyOrderPreservingUnseen(
                baseUrls = baseUrls,
                seenUrls = seenFacetRefs,
                selectedRef = selectedRef,
                direction = direction,
            ) ?: return

        writeBodyOrder(nextFacets, reordered)
        val nextDoc = doc.copy(facets = nextFacets)
        updateLocalDoc(nextDoc, snapshot.noteEditors, scrollToFacetKey = facetKey)
        scheduleSave()
    }

    private fun diffDocs(oldDoc: Doc, newDoc: Doc): DocPatch {
        val facetsSet = mutableMapOf<FacetKey, String>()
        val facetsRemove = mutableListOf<FacetKey>()

        for ((key, value) in newDoc.facets) {
            if (oldDoc.facets[key] != value) {
                facetsSet[key] = value
            }
        }
        for (key in oldDoc.facets.keys) {
            if (!newDoc.facets.containsKey(key)) {
                facetsRemove.add(key)
            }
        }

        return DocPatch(
            id = newDoc.id,
            facetsSet = facetsSet,
            facetsRemove = facetsRemove,
            userPath = null,
        )
    }

    private fun newScrollToFacetRequest(facetKey: FacetKey): ScrollToFacetRequest =
        ScrollToFacetRequest(facetKey = facetKey, seq = nextScrollRequestSeq++)
}

private fun bs58UuidId(): String = encodeBase58(Uuid.random().toByteArray())

internal fun makePrimaryOrderUrls(selectedRef: String, currentUrls: List<String>): List<String> =
    buildList {
        val seen = mutableSetOf<String>()
        val selectedCanonical = stripFacetRefFragment(selectedRef)
        if (seen.add(selectedCanonical)) add(selectedRef)
        for (url in currentUrls) {
            if (seen.add(stripFacetRefFragment(url))) add(url)
        }
    }

internal fun insertRefAfterOrderUrls(
    baseUrls: List<String>,
    anchorRef: String,
    newRef: String,
): List<String> {
    val out = mutableListOf<String>()
    var inserted = false
    val seen = mutableSetOf<String>()
    val anchorCanonical = stripFacetRefFragment(anchorRef)
    val newCanonical = stripFacetRefFragment(newRef)
    for (url in baseUrls) {
        val canonical = stripFacetRefFragment(url)
        if (seen.add(canonical)) {
            out.add(url)
        }
        if (!inserted && canonical == anchorCanonical) {
            if (seen.add(newCanonical)) {
                out.add(newRef)
            }
            inserted = true
        }
    }
    if (!inserted && seen.add(newCanonical)) {
        out.add(newRef)
    }
    return out
}

internal fun orderFacetKeysForDisplay(
    displayableKeys: List<FacetKey>,
    bodyOrderUrls: List<String>,
): List<FacetKey> {
    val availableByRef = displayableKeys.associateBy { key -> stripFacetRefFragment(buildSelfFacetRefUrl(key)) }
    val orderedKeys = mutableListOf<FacetKey>()
    val seen = mutableSetOf<FacetKey>()
    for (url in bodyOrderUrls) {
        val key = availableByRef[stripFacetRefFragment(url)] ?: continue
        if (seen.add(key)) {
            orderedKeys.add(key)
        }
    }
    for (key in displayableKeys) {
        if (seen.add(key)) {
            orderedKeys.add(key)
        }
    }
    return orderedKeys
}

internal fun reorderBodyOrderPreservingUnseen(
    baseUrls: List<String>,
    seenUrls: List<String>,
    selectedRef: String,
    direction: Int,
): List<String>? {
    if (direction == 0) return null

    val seenCanonicalOrder = mutableListOf<String>()
    val seenRefByCanonical = linkedMapOf<String, String>()
    for (url in seenUrls) {
        val canonical = stripFacetRefFragment(url)
        if (canonical !in seenRefByCanonical) {
            seenRefByCanonical[canonical] = url
            seenCanonicalOrder += canonical
        }
    }
    if (seenCanonicalOrder.isEmpty()) return null

    val baseDeduped = mutableListOf<String>()
    val seenBaseCanonicals = mutableSetOf<String>()
    val baseSeenCanonicalsInOrder = mutableListOf<String>()
    val baseSeenRefByCanonical = linkedMapOf<String, String>()
    val seenBaseAllCanonicals = mutableSetOf<String>()
    for (url in baseUrls) {
        val canonical = stripFacetRefFragment(url)
        if (!seenBaseAllCanonicals.add(canonical)) continue
        baseDeduped += url
        if (canonical in seenRefByCanonical) {
            seenBaseCanonicals += canonical
            baseSeenCanonicalsInOrder += canonical
            baseSeenRefByCanonical.putIfAbsent(canonical, url)
        }
    }

    val missingSeenCanonicals = seenCanonicalOrder.filter { it !in seenBaseCanonicals }
    val crystallized =
        baseDeduped.toMutableList().apply {
            addAll(missingSeenCanonicals.map { canonical -> seenRefByCanonical.getValue(canonical) })
        }

    val seenCanonicalsInCrystallized =
        crystallized.map(::stripFacetRefFragment).filter { it in seenRefByCanonical.keys }
    val selectedCanonical = stripFacetRefFragment(selectedRef)
    val selectedIndex = seenCanonicalsInCrystallized.indexOf(selectedCanonical)
    if (selectedIndex == -1) return null
    val targetIndex = selectedIndex + direction
    if (targetIndex !in seenCanonicalsInCrystallized.indices) return null

    val swappedSeenCanonicals = seenCanonicalsInCrystallized.toMutableList().also {
        val tmp = it[selectedIndex]
        it[selectedIndex] = it[targetIndex]
        it[targetIndex] = tmp
    }

    val renderRefByCanonical = linkedMapOf<String, String>()
    for ((canonical, url) in baseSeenRefByCanonical) {
        renderRefByCanonical[canonical] = url
    }
    for ((canonical, url) in seenRefByCanonical) {
        renderRefByCanonical[canonical] = url
    }

    val seenQueue = ArrayDeque(swappedSeenCanonicals.map { canonical -> renderRefByCanonical.getValue(canonical) })
    val out = mutableListOf<String>()
    for (url in crystallized) {
        val canonical = stripFacetRefFragment(url)
        if (canonical in seenRefByCanonical.keys) {
            out += seenQueue.removeFirst()
        } else {
            out += url
        }
    }
    return out
}

private fun encodeBase58(bytes: ByteArray): String {
    if (bytes.isEmpty()) {
        return ""
    }
    val alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    var zeros = 0
    while (zeros < bytes.size && bytes[zeros].toInt() == 0) {
        zeros += 1
    }

    val input = bytes.copyOf()
    val encoded = CharArray(bytes.size * 2)
    var outputStart = encoded.size
    var inputStart = zeros
    while (inputStart < input.size) {
        var remainder = 0
        for (index in inputStart until input.size) {
            val value = (input[index].toInt() and 0xff)
            val acc = remainder * 256 + value
            input[index] = (acc / 58).toByte()
            remainder = acc % 58
        }
        encoded[--outputStart] = alphabet[remainder]
        while (inputStart < input.size && input[inputStart].toInt() == 0) {
            inputStart += 1
        }
    }

    while (zeros-- > 0) {
        encoded[--outputStart] = '1'
    }

    return encoded.concatToString(outputStart, encoded.size)
}

private fun facetKeyRefPathString(key: FacetKey): String =
    buildSelfFacetRefUrl(key).removePrefix("db+facet:///self/")
