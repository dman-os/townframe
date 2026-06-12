@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListLayoutInfo
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.hoverable
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Error
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material.icons.filled.Sync
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.PlainTooltip
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.material3.TooltipBox
import androidx.compose.material3.TooltipDefaults
import androidx.compose.material3.rememberTooltipState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.ui.editor.EditorSessionController
import org.example.daybook.ui.editor.FacetEditorKind
import org.example.daybook.ui.editor.FacetViewDescriptor
import org.example.daybook.ui.editor.dmetaFacetKey
import org.example.daybook.ui.editor.facetDisplayHintKey
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey

private enum class EditorSaveStatus {
    Idle,
    Saving,
    Error,
}

private data class SaveStatusUi(val icon: ImageVector, val tint: Color, val label: String)

private data class FacetBlockSummary(
    val title: String,
    val preview: String?,
    val contentDescription: String,
)

private data class StickyFacetActionPlacement(
    val descriptor: FacetViewDescriptor,
    val facetIndex: Int,
    val yOffsetPx: Int,
)

@Composable
fun DocEditor(
    controller: EditorSessionController,
    showInlineFacetRack: Boolean = false,
    displayHints: Map<String, FacetDisplayHint> = emptyMap(),
    displayHintsError: String? = null,
    modifier: Modifier = Modifier,
) {
    val state by controller.state.collectAsState()
    val snackbarHostState = remember { SnackbarHostState() }
    val collapsedFacetStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    val blockActionExpandedStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    var uiMessage by remember { mutableStateOf<String?>(null) }
    val saveStatus =
        when {
            state.saveError != null -> EditorSaveStatus.Error
            state.isSaving -> EditorSaveStatus.Saving
            else -> EditorSaveStatus.Idle
        }

    LaunchedEffect(state.saveError) {
        val errorMessage = state.saveError ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(errorMessage)
    }
    LaunchedEffect(uiMessage) {
        val nextMessage = uiMessage ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(nextMessage)
        uiMessage = null
    }

    val listState = rememberLazyListState()
    val hasFacetRows = state.contentFacetViews.isNotEmpty()
    var stickyFacetActionsHeightPx by remember(state.docId) { mutableStateOf(0) }
    val facetListStartIndex =
        remember(state.titleNotice, displayHintsError, hasFacetRows) {
            docEditorFacetListStartIndex(
                titleNotice = state.titleNotice,
                displayHintsError = displayHintsError,
                hasFacetRows = hasFacetRows,
            )
        }
    val stickyFacetPlacement by remember(
        state.contentFacetViews,
        state.titleNotice,
        displayHintsError,
        facetListStartIndex,
        listState,
        stickyFacetActionsHeightPx,
    ) {
        derivedStateOf {
            resolveStickyFacetActionPlacement(
                layoutInfo = listState.layoutInfo,
                facetViews = state.contentFacetViews,
                facetListStartIndex = facetListStartIndex,
                stickyFacetActionsHeightPx = stickyFacetActionsHeightPx,
            )
        }
    }

    LaunchedEffect(state.scrollToFacetRequest?.seq, state.contentFacetViews, state.titleNotice, displayHintsError) {
        val request = state.scrollToFacetRequest ?: return@LaunchedEffect
        val targetIndex = state.contentFacetViews.indexOfFirst { it.facetKey == request.facetKey }
        if (targetIndex >= 0) {
            listState.animateScrollToItem(facetListStartIndex + targetIndex)
        }
    }

    Box(modifier = modifier.fillMaxSize().testTag(DaybookEditorSemantics.Editor)) {
        LazyColumn(
            state = listState,
            modifier =
            Modifier
                .fillMaxSize()
                .testTag(DaybookEditorSemantics.EditorList),
        ) {
            item(key = "title") {
                TextField(
                    value = state.titleDraft,
                    onValueChange = { value -> controller.setTitleDraft(value) },
                    modifier =
                    Modifier
                        .fillMaxWidth()
                        .testTag(DaybookEditorSemantics.TitleField)
                        .semantics {
                            contentDescription = "Document title"
                        },
                    enabled = state.titleEditable,
                    placeholder = { Text("Title") },
                    textStyle = MaterialTheme.typography.titleLarge.copy(fontWeight = FontWeight.Bold),
                    colors =
                    TextFieldDefaults.colors(
                        focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                    ),
                )
            }
            state.titleNotice?.let { titleNotice ->
                item(key = "title-notice") {
                    Text(
                        text = titleNotice,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
            item(key = "title-divider") {
                HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
            }

            displayHintsError?.let { message ->
                item(key = "display-hints-error") {
                    FacetStatusText(
                        text = "Facet display config unavailable: $message",
                        modifier = Modifier.padding(bottom = 8.dp),
                    )
                }
            }

            if (state.contentFacetViews.isEmpty()) {
                item(key = "no-facets") {
                    Text(
                        text = "No facets",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            } else {
                itemsIndexed(
                    items = state.contentFacetViews,
                    key = { _, descriptor -> facetKeyString(descriptor.facetKey) },
                ) { index, descriptor ->
                    val facetKeyLabel = facetKeyString(descriptor.facetKey)
                    val isCollapsed = collapsedFacetStates[facetKeyLabel] == true
                    val actionsExpanded = blockActionExpandedStates[facetKeyLabel] == true
                    val showStickyActions = stickyFacetPlacement?.descriptor?.facetKey == descriptor.facetKey
                    FacetBlock(
                        descriptor = descriptor,
                        doc = state.doc,
                        branchPath = state.branchPath,
                        controller = controller,
                        modifier = Modifier,
                        canShowMenu = state.docId != null,
                        isCollapsed = isCollapsed,
                        actionsExpanded = actionsExpanded,
                        onActionsExpandedChange = { blockActionExpandedStates[facetKeyLabel] = it },
                        showInlineActions = !showStickyActions,
                        noteDraft = state.noteEditors[descriptor.facetKey]?.draft,
                        noteEditable = state.noteEditors[descriptor.facetKey]?.editable ?: false,
                        noteNotice = state.noteEditors[descriptor.facetKey]?.notice,
                        displayHints = displayHints,
                        canMoveUp = index > 0,
                        canMoveDown = index < state.contentFacetViews.lastIndex,
                        onToggleCollapse = {
                            collapsedFacetStates[facetKeyLabel] = !(collapsedFacetStates[facetKeyLabel] == true)
                        },
                        onUiError = { message -> uiMessage = message },
                    )
                    if (index < state.contentFacetViews.lastIndex) {
                        Spacer(modifier = Modifier.height(8.dp))
                    }
                }
            }

            if (showInlineFacetRack) {
                item(key = "details") {
                    Column(
                        modifier =
                        Modifier
                            .fillMaxWidth()
                            .testTag(DaybookEditorSemantics.Details),
                    ) {
                        HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                        Text(
                            text = "Details",
                            style = MaterialTheme.typography.titleSmall,
                            modifier = Modifier.padding(bottom = 8.dp),
                        )
                        DocDetailsSidebar(
                            doc = state.doc,
                            warnings = state.docWarnings,
                            modifier = Modifier.fillMaxWidth(),
                        )
                    }
                }
            }
        }

        stickyFacetPlacement?.let { placement ->
            val descriptor = placement.descriptor
            val facetKeyLabel = facetKeyString(descriptor.facetKey)
            val isCollapsed = collapsedFacetStates[facetKeyLabel] == true
            val actionsExpanded = blockActionExpandedStates[facetKeyLabel] == true
            val stickyActionsHoverSource = remember(descriptor.facetKey) { MutableInteractionSource() }
            val stickyActionsHovered by stickyActionsHoverSource.collectIsHoveredAsState()
            FacetBlockActionsMenu(
                facetKeyLabel = facetKeyLabel,
                isPrimary = descriptor.isPrimary,
                actions =
                FacetBlockActions(
                    canShowMenu = state.docId != null,
                    canMoveUp = placement.facetIndex > 0,
                    canMoveDown = placement.facetIndex < state.contentFacetViews.lastIndex,
                    isCollapsed = isCollapsed,
                    onAddNote = { controller.addNoteFacetAfter(descriptor.facetKey) },
                    onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
                    onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
                    onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
                    onToggleCollapse = {
                        collapsedFacetStates[facetKeyLabel] = !(collapsedFacetStates[facetKeyLabel] == true)
                    },
                ),
                expanded = actionsExpanded,
                visible = true,
                onExpandedChange = { blockActionExpandedStates[facetKeyLabel] = it },
                showQuickAction = actionsExpanded || stickyActionsHovered,
                quickActionIcon =
                if (isCollapsed) {
                    Icons.Default.KeyboardArrowDown
                } else {
                    Icons.Default.KeyboardArrowUp
                },
                blockHovered = true,
                actionsHovered = stickyActionsHovered,
                modifier =
                Modifier
                    .align(Alignment.TopEnd)
                    .offset { IntOffset(0, placement.yOffsetPx) }
                    .padding(top = 2.dp)
                    .onSizeChanged { stickyFacetActionsHeightPx = it.height },
                interactionSource = stickyActionsHoverSource,
            )
        }

        SnackbarHost(
            hostState = snackbarHostState,
            modifier = Modifier.align(Alignment.BottomCenter).padding(8.dp),
        )
    }
}

private fun docEditorFacetListStartIndex(
    titleNotice: String?,
    displayHintsError: String?,
    hasFacetRows: Boolean,
): Int {
    var index = 0
    index += 1
    if (titleNotice != null) {
        index += 1
    }
    index += 1
    if (displayHintsError != null) {
        index += 1
    }
    if (!hasFacetRows) {
        index += 1
    }
    return index
}

private fun resolveStickyFacetActionPlacement(
    layoutInfo: LazyListLayoutInfo,
    facetViews: List<FacetViewDescriptor>,
    facetListStartIndex: Int,
    stickyFacetActionsHeightPx: Int,
): StickyFacetActionPlacement? {
    val visibleFacetItem =
        layoutInfo.visibleItemsInfo.firstOrNull { it.index >= facetListStartIndex } ?: return null
    if (visibleFacetItem.offset >= 0) {
        return null
    }
    val facetIndex = visibleFacetItem.index - facetListStartIndex
    val descriptor = facetViews.getOrNull(facetIndex) ?: return null
    val nextFacetItem =
        if (facetIndex < facetViews.lastIndex) {
            layoutInfo.visibleItemsInfo.firstOrNull { it.index == visibleFacetItem.index + 1 }
        } else {
            null
        }
    val yOffsetPx =
        nextFacetItem?.let { minOf(0, it.offset - stickyFacetActionsHeightPx) } ?: 0
    return StickyFacetActionPlacement(
        descriptor = descriptor,
        facetIndex = facetIndex,
        yOffsetPx = yOffsetPx,
    )
}

@Composable
private fun FacetBlock(
    descriptor: FacetViewDescriptor,
    doc: Doc?,
    branchPath: String,
    controller: EditorSessionController,
    modifier: Modifier = Modifier,
    canShowMenu: Boolean,
    isCollapsed: Boolean,
    actionsExpanded: Boolean,
    onActionsExpandedChange: (Boolean) -> Unit,
    showInlineActions: Boolean,
    noteDraft: String?,
    noteEditable: Boolean,
    noteNotice: String?,
    displayHints: Map<String, FacetDisplayHint>,
    canMoveUp: Boolean,
    canMoveDown: Boolean,
    onToggleCollapse: () -> Unit,
    onUiError: (String) -> Unit,
) {
    val displayHintKey = facetDisplayHintKey(descriptor.facetKey)
    val displayHint = displayHints[displayHintKey]
    val facetKeyLabel = facetKeyString(descriptor.facetKey)
    val blockSummary =
        remember(
            descriptor.facetKey,
            descriptor.kind,
            descriptor.rawValue,
            descriptor.isPrimary,
            displayHint?.displayTitle,
            noteDraft,
        ) {
            buildFacetBlockSummary(
                descriptor = descriptor,
                displayHint = displayHint,
                noteDraft = noteDraft,
            )
        }
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    val blockActionsInteractionSource = remember { MutableInteractionSource() }
    val isActionsHovered by blockActionsInteractionSource.collectIsHoveredAsState()
    val actionsVisible = canShowMenu && (isCollapsed || isHovered || actionsExpanded || isActionsHovered)
    val quickActionVisible = (actionsExpanded || isActionsHovered)

    Box(
        modifier =
        modifier
            .fillMaxWidth()
            .hoverable(interactionSource)
            .semantics {
                customActions =
                    if (canShowMenu) {
                        listOf(
                            CustomAccessibilityAction("Block actions") {
                                onActionsExpandedChange(true)
                                true
                            },
                            CustomAccessibilityAction(
                                if (isCollapsed) {
                                    "Expand block"
                                } else {
                                    "Collapse block"
                                },
                            ) {
                                onToggleCollapse()
                                true
                            },
                        )
                    } else {
                        emptyList<CustomAccessibilityAction>()
                    }
                contentDescription =
                    if (isCollapsed) {
                        blockSummary.contentDescription
                    } else {
                        if (descriptor.isPrimary) {
                            "Primary document block"
                        } else {
                            "Document block"
                        }
                    }
            }
            .testTag(DaybookEditorSemantics.facetRow(facetKeyLabel)),
    ) {
        if (isCollapsed) {
            FacetBlockCollapsedSummary(
                summary = blockSummary,
                facetKeyLabel = facetKeyLabel,
            )
        } else {
            Column(
                modifier =
                Modifier
                    .fillMaxWidth()
                    .padding(vertical = 2.dp)
                    .testTag(DaybookEditorSemantics.facetBlock(facetKeyLabel))
                    .semantics {
                        contentDescription =
                            if (descriptor.isPrimary) {
                                "Primary document block"
                            } else {
                                "Document block"
                            }
                    },
            ) {
                FacetContentHost(
                    descriptor = descriptor,
                    doc = doc,
                    branchPath = branchPath,
                    displayHint = displayHint,
                    noteEditor =
                    FacetNoteEditorProps(
                        draft = noteDraft,
                        editable = noteEditable,
                        notice = noteNotice,
                        onDraftChange = { nextValue ->
                            controller.setNoteDraft(descriptor.facetKey, nextValue)
                        },
                    ),
                    onUiError = onUiError,
                )
            }
        }
        if (showInlineActions) {
            FacetBlockActionsMenu(
                facetKeyLabel = facetKeyLabel,
                isPrimary = descriptor.isPrimary,
                actions =
                FacetBlockActions(
                    canShowMenu = canShowMenu,
                    canMoveUp = canMoveUp,
                    canMoveDown = canMoveDown,
                    isCollapsed = isCollapsed,
                    onAddNote = { controller.addNoteFacetAfter(descriptor.facetKey) },
                    onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
                    onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
                    onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
                    onToggleCollapse = onToggleCollapse,
                ),
                expanded = actionsExpanded,
                visible = actionsVisible,
                onExpandedChange = onActionsExpandedChange,
                showQuickAction = quickActionVisible,
                quickActionIcon =
                if (isCollapsed) {
                    Icons.Default.KeyboardArrowDown
                } else {
                    Icons.Default.KeyboardArrowUp
                },
                blockHovered = isHovered,
                actionsHovered = isActionsHovered,
                modifier = Modifier.align(Alignment.TopEnd).padding(top = 2.dp),
                interactionSource = blockActionsInteractionSource,
            )
        }
    }
}

private data class FacetBlockActions(
    val canShowMenu: Boolean,
    val canMoveUp: Boolean,
    val canMoveDown: Boolean,
    val isCollapsed: Boolean,
    val onAddNote: () -> Unit,
    val onMakePrimary: () -> Unit,
    val onMoveUp: () -> Unit,
    val onMoveDown: () -> Unit,
    val onToggleCollapse: () -> Unit,
)

@Composable
private fun FacetBlockActionsMenu(
    facetKeyLabel: String,
    isPrimary: Boolean,
    actions: FacetBlockActions,
    expanded: Boolean,
    visible: Boolean,
    onExpandedChange: (Boolean) -> Unit,
    showQuickAction: Boolean,
    quickActionIcon: ImageVector,
    blockHovered: Boolean,
    actionsHovered: Boolean,
    interactionSource: MutableInteractionSource,
    modifier: Modifier = Modifier,
) {
    if (!actions.canShowMenu) {
        return
    }

    val rowBackgroundColor =
        if (showQuickAction) {
            MaterialTheme.colorScheme.surfaceContainer
        } else {
            MaterialTheme.colorScheme.surfaceContainer.copy(alpha = 0.48f)
        }

    Box(modifier = modifier) {
        Row(
            modifier =
            Modifier
                .hoverable(interactionSource)
                .background(
                    color = rowBackgroundColor,
                    shape = RoundedCornerShape(percent = 50),
                )
                .padding(4.dp),
            horizontalArrangement = Arrangement.spacedBy(2.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            if (showQuickAction) {
                FacetBlockActionButton(
                    imageVector = quickActionIcon,
                    tooltipText =
                    if (actions.isCollapsed) {
                        "Expand block"
                    } else {
                        "Collapse block"
                    },
                    contentDescription =
                    if (actions.isCollapsed) {
                        "Expand block"
                    } else {
                        "Collapse block"
                    },
                    testTag = DaybookEditorSemantics.toggleBlockCollapseQuickAction(facetKeyLabel),
                    onClick = actions.onToggleCollapse,
                    iconAlpha = 1f,
                )
            }
            if (visible || expanded) {
                FacetBlockActionButton(
                    imageVector = Icons.Default.MoreVert,
                    contentDescription = "Block actions",
                    testTag = DaybookEditorSemantics.blockActions(facetKeyLabel),
                    onClick = { onExpandedChange(true) },
                    iconAlpha = if (blockHovered || expanded || actionsHovered) 1f else 0.48f,
                )
            }
        }
        DropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) },
        ) {
            DropdownMenuItem(
                text = {
                    Text(
                        if (actions.isCollapsed) {
                            "Expand block"
                        } else {
                            "Collapse block"
                        },
                    )
                },
                modifier = Modifier.testTag(DaybookEditorSemantics.toggleBlockCollapseAction(facetKeyLabel)),
                onClick = {
                    onExpandedChange(false)
                    actions.onToggleCollapse()
                },
            )
            DropdownMenuItem(
                text = { Text(if (isPrimary) "Primary block" else "Make primary") },
                enabled = !isPrimary,
                modifier = Modifier.testTag(DaybookEditorSemantics.makePrimaryAction(facetKeyLabel)),
                onClick = {
                    onExpandedChange(false)
                    actions.onMakePrimary()
                },
            )
            DropdownMenuItem(
                text = { Text("Move up") },
                enabled = actions.canMoveUp,
                modifier = Modifier.testTag(DaybookEditorSemantics.moveUpAction(facetKeyLabel)),
                onClick = {
                    onExpandedChange(false)
                    actions.onMoveUp()
                },
            )
            DropdownMenuItem(
                text = { Text("Move down") },
                enabled = actions.canMoveDown,
                modifier = Modifier.testTag(DaybookEditorSemantics.moveDownAction(facetKeyLabel)),
                onClick = {
                    onExpandedChange(false)
                    actions.onMoveDown()
                },
            )
            DropdownMenuItem(
                text = { Text("Add note below") },
                modifier = Modifier.testTag(DaybookEditorSemantics.addNoteAfterAction(facetKeyLabel)),
                onClick = {
                    onExpandedChange(false)
                    actions.onAddNote()
                },
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun FacetBlockActionButton(
    imageVector: ImageVector,
    contentDescription: String,
    testTag: String,
    onClick: () -> Unit,
    tooltipText: String? = null,
    iconAlpha: Float = 1f,
) {
    val buttonContent: @Composable () -> Unit = {
        IconButton(
            onClick = onClick,
            modifier = Modifier.size(36.dp).testTag(testTag),
        ) {
            Icon(
                imageVector = imageVector,
                contentDescription = contentDescription,
                tint = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = iconAlpha),
            )
        }
    }

    if (tooltipText != null) {
        TooltipBox(
            positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
            tooltip = {
                PlainTooltip {
                    Text(tooltipText)
                }
            },
            state = rememberTooltipState(),
        ) {
            buttonContent()
        }
    } else {
        buttonContent()
    }
}

@Composable
private fun FacetBlockCollapsedSummary(summary: FacetBlockSummary, facetKeyLabel: String) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(vertical = 6.dp)
            .testTag(DaybookEditorSemantics.collapsedFacetBlock(facetKeyLabel))
            .semantics {
                contentDescription = summary.contentDescription
            },
    ) {
        Text(
            text = summary.title,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
        summary.preview?.let { preview ->
            Text(
                text = preview,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.padding(top = 2.dp),
            )
        }
    }
}

private fun buildFacetBlockSummary(
    descriptor: FacetViewDescriptor,
    displayHint: FacetDisplayHint?,
    noteDraft: String?,
): FacetBlockSummary {
    val title =
        displayHint?.displayTitle?.takeIf { it.isNotBlank() }
            ?: "${facetKindLabel(descriptor.kind)} · ${descriptor.facetKey.id}"
    val preview =
        when (descriptor.kind) {
            FacetEditorKind.Note -> noteDraft.orEmpty().summaryPreview()
            FacetEditorKind.GenericJson -> decodeJsonStringOrRaw(descriptor.rawValue).summaryPreview()
            FacetEditorKind.ImageMetadata -> null
        }
    val prefix =
        if (descriptor.isPrimary) {
            "Primary collapsed document block"
        } else {
            "Collapsed document block"
        }
    val contentDescription =
        buildString {
            append(prefix)
            append(": ")
            append(title)
            preview?.let {
                append(". ")
                append(it)
            }
        }
    return FacetBlockSummary(title = title, preview = preview, contentDescription = contentDescription)
}

private fun facetKindLabel(kind: FacetEditorKind): String = when (kind) {
    FacetEditorKind.Note -> "Note"
    FacetEditorKind.ImageMetadata -> "Image"
    FacetEditorKind.GenericJson -> "Generic"
}

private fun String.summaryPreview(maxLength: Int = 80): String? {
    val preview = lineSequence().firstOrNull()?.trim().orEmpty()
    if (preview.isBlank()) {
        return null
    }
    return if (preview.length <= maxLength) {
        preview
    } else {
        preview.take(maxLength - 1).trimEnd() + "…"
    }
}

@Composable
private fun toSaveStatusUi(saveStatus: EditorSaveStatus): SaveStatusUi? = when (saveStatus) {
    EditorSaveStatus.Idle -> null

    EditorSaveStatus.Saving ->
        SaveStatusUi(
            icon = Icons.Filled.Sync,
            tint = MaterialTheme.colorScheme.primary,
            label = "Saving",
        )

    EditorSaveStatus.Error ->
        SaveStatusUi(
            icon = Icons.Filled.Error,
            tint = MaterialTheme.colorScheme.error,
            label = "Save failed",
        )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun EditorSaveStatusIndicator(saveStatus: EditorSaveStatus, modifier: Modifier = Modifier) {
    val saveStatusUi = toSaveStatusUi(saveStatus) ?: return
    Row(
        modifier = modifier.padding(horizontal = 12.dp, vertical = 4.dp),
        horizontalArrangement = Arrangement.End,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TooltipBox(
            positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
            tooltip = {
                PlainTooltip {
                    Text(saveStatusUi.label)
                }
            },
            state = rememberTooltipState(),
        ) {
            Icon(
                imageVector = saveStatusUi.icon,
                contentDescription = saveStatusUi.label,
                tint = saveStatusUi.tint,
            )
        }
    }
}

@Composable
fun DocFacetSidebar(controller: EditorSessionController, modifier: Modifier = Modifier) {
    val state by controller.state.collectAsState()
    Column(
        modifier =
        modifier
            .fillMaxSize()
            .padding(8.dp)
            .testTag(DaybookEditorSemantics.Details),
    ) {
        Text(
            text = "Details",
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(bottom = 8.dp),
        )
        DocDetailsSidebar(doc = state.doc, warnings = state.docWarnings, modifier = Modifier.fillMaxSize())
    }
}

@Composable
private fun DocDetailsSidebar(doc: Doc?, warnings: List<String> = emptyList(), modifier: Modifier = Modifier) {
    val dmetaParseWarning = mutableListOf<String>()
    val dmetaDetails = run {
        val raw = doc?.facets?.get(dmetaFacetKey()) ?: return@run null
        val parsed = parseDmetaSidebarDetails(raw)
        if (parsed.isFailure) {
            val message = parsed.exceptionOrNull()?.message ?: "unknown error"
            dmetaParseWarning += "Failed to parse dmeta facet details. $message"
            return@run null
        }
        parsed.getOrThrow()
    }
    val allWarnings = warnings + dmetaParseWarning

    Column(modifier = modifier) {
        if (allWarnings.isNotEmpty()) {
            Text(
                text = "Warnings",
                style = MaterialTheme.typography.titleSmall,
                color = MaterialTheme.colorScheme.error,
                modifier = Modifier.padding(bottom = 4.dp),
            )
            allWarnings.forEach { warning ->
                Text(
                    text = "• $warning",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.error,
                    modifier = Modifier.padding(bottom = 4.dp),
                )
            }
            HorizontalDivider(modifier = Modifier.padding(vertical = 6.dp))
        }
        DetailRow("Doc ID", doc?.id ?: "Unsaved")
        DetailRow("Created", dmetaDetails?.createdAt ?: "Unknown")
        DetailRow("Last modified", dmetaDetails?.lastModifiedAt ?: "Unknown")
        DetailRow(
            "Supported facets",
            (
                doc?.facets?.keys
                    ?.count { key ->
                        when ((key.tag as? org.example.daybook.uniffi.types.FacetTag.WellKnown)?.v1) {
                            org.example.daybook.uniffi.types.WellKnownFacetTag.NOTE,
                            org.example.daybook.uniffi.types.WellKnownFacetTag.IMAGE_METADATA,
                            -> true

                            else -> false
                        }
                    } ?: 0
                ).toString(),
        )
    }
}

@Composable
private fun DetailRow(label: String, value: String) {
    Column(modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp)) {
        Text(
            text = label,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Text(
            text = value,
            style = MaterialTheme.typography.bodySmall,
            maxLines = 3,
            overflow = TextOverflow.Ellipsis,
        )
    }
}

@Composable
private fun InlineFacetRack(facetRows: List<Pair<FacetKey, String>>, modifier: Modifier = Modifier) {
    Column(modifier = modifier) {
        facetRows.forEach { facetRow ->
            Row(
                modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Text(
                    text = facetKeyString(facetRow.first),
                    style = MaterialTheme.typography.bodySmall,
                    modifier = Modifier.weight(0.45f),
                )
                Text(
                    text = previewFacetValue(facetRow.second),
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(0.55f),
                )
            }
        }
    }
}

@Composable
private fun FacetRackList(facetRows: List<Pair<FacetKey, String>>, modifier: Modifier = Modifier) {
    LazyColumn(modifier = modifier) {
        items(facetRows) { facetRow ->
            Row(
                modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Text(
                    text = facetKeyString(facetRow.first),
                    style = MaterialTheme.typography.bodySmall,
                    modifier = Modifier.weight(0.45f),
                )
                Text(
                    text = previewFacetValue(facetRow.second),
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(0.55f),
                )
            }
        }
    }
}
