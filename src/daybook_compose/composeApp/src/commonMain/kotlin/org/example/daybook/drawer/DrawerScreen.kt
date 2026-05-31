@file:OptIn(
    kotlin.uuid.ExperimentalUuidApi::class,
    androidx.compose.material3.ExperimentalMaterial3Api::class,
    kotlin.time.ExperimentalTime::class
)

package org.example.daybook.drawer

import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.background
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.example.daybook.ChromeState
import org.example.daybook.DaybookContentType
import org.example.daybook.DocListState
import org.example.daybook.DocEditorStoreViewModel
import org.example.daybook.DrawerViewModel
import org.example.daybook.LocalDocEditorStore
import org.example.daybook.ProvideChromeState
import org.example.daybook.tables.DockableRegion
import org.example.daybook.ui.DocEditor
import org.example.daybook.ui.DocFacetSidebar
import org.example.daybook.ui.buildSelfFacetRefUrl
import org.example.daybook.ui.decodeJsonStringOrRaw
import org.example.daybook.ui.decodeWellKnownFacet
import org.example.daybook.ui.stripFacetRefFragment
import org.example.daybook.ui.editor.bodyFacetKey
import org.example.daybook.ui.editor.noteFacetKey
import org.example.daybook.ui.editor.titleFacetKey
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacet

@Composable
private fun DrawerDocEditorContent(
    controller: org.example.daybook.ui.editor.EditorSessionController?,
    selectedDocId: String?,
    modifier: Modifier = Modifier,
    showFacetSidebar: Boolean,
    showInlineFacetRack: Boolean = false
) {
    Box(modifier = modifier.fillMaxSize()) {
        if (selectedDocId != null) {
            if (controller == null) {
                Box(
                    modifier = Modifier.fillMaxSize(),
                    contentAlignment = Alignment.Center
                ) {
                    CircularProgressIndicator()
                }
                return@Box
            }
            if (showFacetSidebar) {
                DockableRegion(
                    modifier = Modifier.fillMaxSize().padding(16.dp),
                    orientation = Orientation.Horizontal,
                    initialWeights = mapOf("doc-main" to 0.72f, "doc-facets" to 0.28f)
                ) {
                    pane("doc-main") {
                        DocEditor(
                            controller = controller,
                            modifier = Modifier.fillMaxSize()
                        )
                    }
                    pane("doc-facets") {
                        DocFacetSidebar(
                            controller = controller,
                            modifier = Modifier.fillMaxSize()
                        )
                    }
                }
            } else {
                DocEditor(
                    controller = controller,
                    showInlineFacetRack = showInlineFacetRack,
                    modifier = Modifier.padding(16.dp),
                )
            }
        } else {
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Text("Select a document to view details")
            }
        }
    }
}

@Composable
fun DrawerScreen(
    drawerVm: DrawerViewModel,
    onOpenDoc: (String) -> Unit,
    modifier: Modifier = Modifier
) {
    val docEditorStore: DocEditorStoreViewModel = LocalDocEditorStore.current
    val selectedDocId by docEditorStore.selectedDocId.collectAsState()
    ProvideChromeState(ChromeState(title = "Drawer")) {
        DocList(
            drawerViewModel = drawerVm,
            selectedDocId = selectedDocId,
            onDocClick = { docId ->
                docEditorStore.selectDoc(docId)
                onOpenDoc(docId)
            },
            modifier = modifier
        )
    }
}

@Composable
fun DocEditorScreen(
    contentType: DaybookContentType,
    modifier: Modifier = Modifier
) {
    val docEditorStore: DocEditorStoreViewModel = LocalDocEditorStore.current
    val selectedDocId by docEditorStore.selectedDocId.collectAsState()
    val selectedController by docEditorStore.selectedController.collectAsState()

    DisposableEffect(selectedDocId) {
        if (selectedDocId != null) {
            docEditorStore.attachHost(selectedDocId!!)
        }
        onDispose {
            if (selectedDocId != null) {
                docEditorStore.detachHost(selectedDocId!!)
            }
        }
    }

    Box(
        modifier =
            modifier
                .fillMaxSize()
                .background(MaterialTheme.colorScheme.surface)
    ) {
        DrawerDocEditorContent(
            controller = selectedController,
            selectedDocId = selectedDocId,
            modifier = Modifier.fillMaxSize(),
            showFacetSidebar = contentType == DaybookContentType.LIST_AND_DETAIL,
            showInlineFacetRack = contentType != DaybookContentType.LIST_AND_DETAIL
        )
    }
}

@Composable
fun DocList(
    drawerViewModel: DrawerViewModel,
    selectedDocId: String?,
    onDocClick: (String) -> Unit,
    modifier: Modifier = Modifier
) {
    val docListState by drawerViewModel.docListState.collectAsState()
    val loadedDocs by drawerViewModel.loadedDocs.collectAsState()
    val loadingDocs by drawerViewModel.loadingDocs.collectAsState()

    val listState = rememberLazyListState()

    val currentState = docListState
    when (currentState) {
        is DocListState.Loading -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                CircularProgressIndicator()
            }
        }

        is DocListState.Error -> {
            Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                Text("Error: ${currentState.error.message()}")
            }
        }

        is DocListState.Data -> {
            val docIds = currentState.docIds
            if (docIds.isEmpty()) {
                Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text("No documents in drawer", style = MaterialTheme.typography.bodyLarge)
                }
            } else {
                // Track visible items and preload next documents
                LaunchedEffect(
                    listState.firstVisibleItemIndex,
                    listState.firstVisibleItemScrollOffset
                ) {
                    val layoutInfo = listState.layoutInfo
                    val firstVisible = listState.firstVisibleItemIndex
                    val lastVisible =
                        layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: firstVisible
                    val preloadCount = 10 // Preload next 10 items
                    val endIndex = (lastVisible + preloadCount).coerceAtMost(docIds.size - 1)

                    val idsToLoad = docIds.subList(firstVisible.coerceAtLeast(0), endIndex + 1)
                    drawerViewModel.loadDocs(idsToLoad)
                }

                LazyColumn(
                    state = listState,
                    modifier = modifier.fillMaxSize()
                ) {
                    items(docIds.size, key = { idx -> docIds[idx] }) { index ->
                        val docId = docIds[index]
                        val doc = loadedDocs[docId]
                        val isLoading = loadingDocs.contains(docId)
                        val isSelected = docId == selectedDocId

                        if (doc != null) {
                            // Document is loaded, show it
                            val mainType = drawerMainFacetTypeLabel(doc)
                            val draw = @Composable {
                                ListItem(
                                    headlineContent = {
                                        val titleJson = doc.facets[
                                            titleFacetKey()
                                        ]
                                        val noteJson = doc.facets[noteFacetKey()]
                                        val content =
                                            noteJson?.let { value ->
                                                decodeWellKnownFacet<WellKnownFacet.Note>(value)
                                                    .getOrNull()
                                                    ?.v1
                                                    ?.content
                                            } ?: ""
                                        Text(
                                            text =
                                                titleJson?.let { decodeJsonStringOrRaw(it) }
                                                    ?: content.take(50).ifEmpty {
                                                        "Empty document"
                                                    },
                                            maxLines = 1
                                        )
                                    },
                                    supportingContent = {
                                        val suffix =
                                            mainType?.let { type -> " • $type" } ?: ""
                                        Text("ID: ${doc.id.take(8)}...$suffix")
                                    }
                                )
                            }
                            if (isSelected) {
                                OutlinedCard(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) }
                                ) {
                                    draw()
                                }
                            } else {
                                Card(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) }
                                ) {
                                    draw()
                                }
                            }
                        } else if (isLoading) {
                            // Document is loading, show loading indicator
                            Card(
                                modifier = Modifier.fillMaxWidth()
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Row(
                                            modifier = Modifier.fillMaxWidth(),
                                            horizontalArrangement = Arrangement.SpaceBetween,
                                            verticalAlignment = Alignment.CenterVertically
                                        ) {
                                            Text(
                                                "Loading...",
                                                style = MaterialTheme.typography.bodyMedium
                                            )
                                            CircularProgressIndicator(
                                                modifier = Modifier.size(16.dp)
                                            )
                                        }
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    }
                                )
                            }
                        } else {
                            // Document not loaded yet, trigger load and show placeholder
                            LaunchedEffect(docId) {
                                drawerViewModel.loadDoc(docId)
                            }
                            Card(
                                modifier = Modifier.fillMaxWidth()
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Text(
                                            "Loading...",
                                            style = MaterialTheme.typography.bodyMedium
                                        )
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    }
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

private fun drawerMainFacetTypeLabel(doc: Doc): String? {
    val bodyOrder =
        run {
            val raw = doc.facets[bodyFacetKey()] ?: return@run emptyList()
            val decoded = decodeWellKnownFacet<WellKnownFacet.Body>(raw)
            if (decoded.isFailure) {
                println(
                    "Failed to decode Body facet for drawer main type: docId=${doc.id} facetKey=${bodyFacetKey()} error=${decoded.exceptionOrNull()}"
                )
                return@run emptyList()
            }
            decoded.getOrThrow().v1.order
        }

    val facetByRef = doc.facets.keys.associateBy { key -> stripFacetRefFragment(buildSelfFacetRefUrl(key)) }
    for (url in bodyOrder) {
        val key = facetByRef[stripFacetRefFragment(url)] ?: continue
        if (key == titleFacetKey() || key == bodyFacetKey()) {
            continue
        }
        return drawerFacetTypeLabel(key)
    }

    return doc.facets.keys
        .filter { key -> key != titleFacetKey() && key != bodyFacetKey() }
        .sortedBy(::drawerFacetSortKey)
        .firstOrNull()
        ?.let(::drawerFacetTypeLabel)
}

private fun drawerFacetTypeLabel(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> tag.v1.name.lowercase()
            is FacetTag.Any -> tag.v1
        }
    return if (key.id == "main") tagString else "$tagString:${key.id}"
}

private fun drawerFacetSortKey(key: FacetKey): String = drawerFacetTypeLabel(key)
