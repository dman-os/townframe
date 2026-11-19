@file:Suppress("FunctionNaming")

@file:OptIn(
    kotlin.uuid.ExperimentalUuidApi::class,
    androidx.compose.material3.ExperimentalMaterial3Api::class,
    kotlin.time.ExperimentalTime::class,
)

package org.example.daybook.drawer

import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import org.example.daybook.ConfigViewModel
import org.example.daybook.DaybookContentType
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.DocEditorStoreViewModel
import org.example.daybook.DocListState
import org.example.daybook.DrawerViewModel
import org.example.daybook.LocalBigDialogController
import org.example.daybook.LocalContainer
import org.example.daybook.LocalDocEditorStore
import org.example.daybook.layouts.DaybookScaffold
import org.example.daybook.layouts.DaybookTopBar
import org.example.daybook.layouts.LocalScreenChromeSpec
import org.example.daybook.layouts.ProvideScreenChromeSpec
import org.example.daybook.layouts.ScreenChromeSpec
import org.example.daybook.tables.DockableRegion
import org.example.daybook.ui.DocEditor
import org.example.daybook.ui.DocEditorArgs
import org.example.daybook.ui.DocEditorMediumTopAppBar
import org.example.daybook.ui.DocEditorSelectionState
import org.example.daybook.ui.DocFacetSidebar
import org.example.daybook.ui.buildSelfFacetRefUrl
import org.example.daybook.ui.decodeJsonStringOrRaw
import org.example.daybook.ui.decodeWellKnownFacet
import org.example.daybook.ui.editor.bodyFacetKey
import org.example.daybook.ui.editor.noteFacetKey
import org.example.daybook.ui.editor.titleFacetKey
import org.example.daybook.ui.rememberAddBlockDialogLauncher
import org.example.daybook.ui.rememberDocEditorSelectionState
import org.example.daybook.ui.stripFacetRefFragment
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacet

@Composable
private fun DrawerDocEditorContent(args: DrawerDocEditorContentArgs, modifier: Modifier = Modifier) {
    Box(modifier = modifier.fillMaxSize()) {
        when {
            args.selectedDocId == null -> DrawerDocEditorEmptyState()
            args.controller == null -> DrawerDocEditorLoadingState()
            args.showFacetSidebar -> DrawerDocEditorSplitState(args)
            else -> DrawerDocEditorSingleState(args)
        }
    }
}

@Composable
private fun DrawerDocEditorLoadingState() {
    Box(
        modifier =
        Modifier
            .fillMaxSize()
            .testTag(DaybookEditorSemantics.LOADING)
            .semantics {
                contentDescription = "Loading document"
            },
        contentAlignment = Alignment.Center,
    ) {
        CircularProgressIndicator()
    }
}

@Composable
private fun DrawerDocEditorEmptyState() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center,
    ) {
        Text("Select a document to view details")
    }
}

@Composable
private fun DrawerDocEditorSplitState(args: DrawerDocEditorContentArgs) {
    val controller = args.controller ?: error("controller must be present when selectedDocId is present")
    val addBlockDialogLauncher = rememberAddBlockDialogLauncher(controller)
    val bigDialogController = LocalBigDialogController.current
    DockableRegion(
        modifier = Modifier.fillMaxSize().padding(16.dp),
        orientation = Orientation.Horizontal,
        initialWeights = mapOf("doc-main" to 0.72f, "doc-facets" to 0.28f),
    ) {
        pane("doc-main") {
            DocEditor(
                args =
                org.example.daybook.ui.DocEditorArgs(
                    controller = controller,
                    selectionState = args.selectionState,
                    displayHints = args.displayHints,
                    displayHintsError = args.displayHintsError,
                    isAddBlockPickerOpen = bigDialogController.isShowing,
                    onAddBlockRequested = addBlockDialogLauncher,
                ),
                modifier = Modifier.fillMaxSize(),
            )
        }
        pane("doc-facets") {
            DocFacetSidebar(
                controller = controller,
                modifier = Modifier.fillMaxSize(),
            )
        }
    }
}

@Composable
private fun DrawerDocEditorSingleState(args: DrawerDocEditorContentArgs) {
    val controller = args.controller ?: error("controller must be present when selectedDocId is present")
    val addBlockDialogLauncher = rememberAddBlockDialogLauncher(controller)
    val bigDialogController = LocalBigDialogController.current
    DocEditor(
        args =
        org.example.daybook.ui.DocEditorArgs(
            controller = controller,
            selectionState = args.selectionState,
            showInlineFacetRack = args.showInlineFacetRack,
            displayHints = args.displayHints,
            displayHintsError = args.displayHintsError,
            isAddBlockPickerOpen = bigDialogController.isShowing,
            onAddBlockRequested = addBlockDialogLauncher,
        ),
        modifier = Modifier.fillMaxSize().padding(16.dp),
    )
}

private data class DrawerDocEditorContentArgs(
    val controller: org.example.daybook.ui.editor.EditorSessionController?,
    val selectedDocId: String?,
    val displayHints: Map<String, FacetDisplayHint>,
    val displayHintsError: String?,
    val showFacetSidebar: Boolean,
    val showInlineFacetRack: Boolean = false,
    val selectionState: DocEditorSelectionState,
)

@Composable
fun DrawerScreen(drawerVm: DrawerViewModel, onOpenDoc: (String) -> Unit, modifier: Modifier = Modifier) {
    val docEditorStore: DocEditorStoreViewModel = LocalDocEditorStore.current
    val selectedDocId by docEditorStore.selectedDocId.collectAsState()
    DaybookScaffold(
        modifier = modifier,
    ) { scaffoldPadding ->
        DocList(
            drawerViewModel = drawerVm,
            selectedDocId = selectedDocId,
            onDocClick = { docId ->
                docEditorStore.selectDoc(docId)
                onOpenDoc(docId)
            },
            modifier = Modifier
                .padding(scaffoldPadding)
                .padding(horizontal = 8.dp, vertical = 4.dp),
        )
    }
}

@Composable
fun DocEditorScreen(contentType: DaybookContentType, modifier: Modifier = Modifier) {
    val state = rememberDocEditorScreenState(contentType)
    val selectionState = rememberDocEditorSelectionState(state.selectedDocId)
    val chrome = rememberDocEditorScreenChrome(state = state, selectionState = selectionState)
    val topBarScrollBehavior =
        if (selectionState.isSelectionMode) {
            null
        } else {
            TopAppBarDefaults.exitUntilCollapsedScrollBehavior()
        }
    ProvideScreenChromeSpec(chrome) {
        DaybookScaffold(
            modifier = modifier.fillMaxSize().testTag(DaybookEditorSemantics.SCREEN),
            nestedScrollConnection = topBarScrollBehavior?.nestedScrollConnection,
            topBarContent = { topBarSpec ->
                if (selectionState.isSelectionMode) {
                    DaybookTopBar(
                        chrome = topBarSpec,
                        scrollBehavior = null,
                    )
                } else {
                    DocEditorMediumTopAppBar(
                        chrome = topBarSpec,
                        controller = state.selectedController,
                        scrollBehavior = topBarScrollBehavior,
                    )
                }
            },
        ) { scaffoldPadding ->
            DocEditorScreenContent(
                state = state,
                selectionState = selectionState,
                scaffoldPadding = scaffoldPadding,
            )
        }
    }
}

@Composable
private fun rememberDocEditorScreenChrome(
    state: DocEditorScreenState,
    selectionState: DocEditorSelectionState,
): ScreenChromeSpec {
    val baseChrome = LocalScreenChromeSpec.current
    if (!selectionState.isSelectionMode) {
        return baseChrome
    }

    val contentFacetLabels =
        state.selectedController?.state?.value?.contentFacetViews.orEmpty().map { facetDescriptor ->
            org.example.daybook.ui.editor.facetKeyString(facetDescriptor.facetKey)
        }

    return ScreenChromeSpec(
        topBar = ScreenChromeSpec.TopBarSpec(
            title = "${selectionState.selectedCount} selected",
            showBack = false,
            pinned = true,
            actions = {
                androidx.compose.material3.TextButton(
                    onClick = { selectionState.clear() },
                    modifier = Modifier.testTag(DaybookEditorSemantics.SELECTION_CANCEL_ACTION),
                ) {
                    androidx.compose.material3.Text("Cancel")
                }
                androidx.compose.material3.TextButton(
                    onClick = { selectionState.selectAll(contentFacetLabels) },
                    modifier = Modifier.testTag(DaybookEditorSemantics.SELECTION_SELECT_ALL_ACTION),
                ) {
                    androidx.compose.material3.Text("Select all")
                }
            },
        ),
    )
}

private data class DocEditorScreenState(
    val selectedDocId: String?,
    val selectedController: org.example.daybook.ui.editor.EditorSessionController?,
    val displayHints: Map<String, FacetDisplayHint>,
    val configError: String?,
    val showFacetSidebar: Boolean,
    val showInlineFacetRack: Boolean,
)

@Composable
private fun rememberDocEditorScreenState(contentType: DaybookContentType): DocEditorScreenState {
    val docEditorStore: DocEditorStoreViewModel = LocalDocEditorStore.current
    val container = LocalContainer.current
    val configVm = viewModel { ConfigViewModel(container.configRepo, container.progressRepo) }
    val displayHints by configVm.metaTableKeyConfigs.collectAsState()
    val configError by configVm.error.collectAsState()
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

    return DocEditorScreenState(
        selectedDocId = selectedDocId,
        selectedController = selectedController,
        displayHints = displayHints,
        configError = configError?.message,
        showFacetSidebar = contentType == DaybookContentType.LIST_AND_DETAIL,
        showInlineFacetRack = contentType != DaybookContentType.LIST_AND_DETAIL,
    )
}

@Composable
private fun DocEditorScreenContent(
    state: DocEditorScreenState,
    selectionState: DocEditorSelectionState,
    scaffoldPadding: PaddingValues,
) {
    Box(
        modifier =
        Modifier
            .fillMaxSize()
            .padding(scaffoldPadding)
            .consumeWindowInsets(scaffoldPadding),
    ) {
        val controller = state.selectedController
        if (state.selectedDocId == null) {
            DrawerDocEditorEmptyState()
        } else if (controller == null) {
            DrawerDocEditorLoadingState()
        } else {
            DrawerDocEditorContent(
                args =
                DrawerDocEditorContentArgs(
                    controller = controller,
                    selectedDocId = state.selectedDocId,
                    displayHints = state.displayHints,
                    displayHintsError = state.configError,
                    showFacetSidebar = state.showFacetSidebar,
                    showInlineFacetRack = state.showInlineFacetRack,
                    selectionState = selectionState,
                ),
                modifier = Modifier.fillMaxSize(),
            )
        }
    }
}

@Composable
fun DocList(
    drawerViewModel: DrawerViewModel,
    selectedDocId: String?,
    onDocClick: (String) -> Unit,
    modifier: Modifier = Modifier,
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
                    listState.firstVisibleItemScrollOffset,
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
                    modifier = modifier.fillMaxSize(),
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
                                            titleFacetKey(),
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
                                            maxLines = 1,
                                        )
                                    },
                                    supportingContent = {
                                        val suffix =
                                            mainType?.let { type -> " • $type" } ?: ""
                                        Text("ID: ${doc.id.take(8)}...$suffix")
                                    },
                                )
                            }
                            if (isSelected) {
                                OutlinedCard(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) },
                                ) {
                                    draw()
                                }
                            } else {
                                Card(
                                    modifier = Modifier.fillMaxWidth(),
                                    onClick = { onDocClick(docId) },
                                ) {
                                    draw()
                                }
                            }
                        } else if (isLoading) {
                            // Document is loading, show loading indicator
                            Card(
                                modifier = Modifier.fillMaxWidth(),
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Row(
                                            modifier = Modifier.fillMaxWidth(),
                                            horizontalArrangement = Arrangement.SpaceBetween,
                                            verticalAlignment = Alignment.CenterVertically,
                                        ) {
                                            Text(
                                                "Loading...",
                                                style = MaterialTheme.typography.bodyMedium,
                                            )
                                            CircularProgressIndicator(
                                                modifier = Modifier.size(16.dp),
                                            )
                                        }
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    },
                                )
                            }
                        } else {
                            // Document not loaded yet, trigger load and show placeholder
                            LaunchedEffect(docId) {
                                drawerViewModel.loadDoc(docId)
                            }
                            Card(
                                modifier = Modifier.fillMaxWidth(),
                            ) {
                                ListItem(
                                    headlineContent = {
                                        Text(
                                            "Loading...",
                                            style = MaterialTheme.typography.bodyMedium,
                                        )
                                    },
                                    supportingContent = {
                                        Text("ID: ${docId.take(8)}...")
                                    },
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
                    "Failed to decode Body facet for drawer main type: docId=${doc.id} facetKey=${bodyFacetKey()} error=${decoded.exceptionOrNull()}",
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
