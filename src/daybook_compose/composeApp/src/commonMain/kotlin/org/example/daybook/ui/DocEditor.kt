@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.ime
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListLayoutInfo
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
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
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TextField
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.material3.TooltipBox
import androidx.compose.material3.TooltipDefaults
import androidx.compose.material3.rememberTooltipState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
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
import org.example.daybook.LocalBigDialogController
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

private data class FacetBlockSummary(val title: String, val preview: String?, val contentDescription: String)

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
    isAddBlockPickerOpen: Boolean = false,
    onAddBlockRequested: (FacetKey) -> Unit,
    modifier: Modifier = Modifier,
) {
    val state by controller.state.collectAsState()
    val snackbarHostState = remember { SnackbarHostState() }
    val collapsedFacetStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    val blockActionExpandedStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    var focusedNoteFacetLabel by remember(state.docId) { mutableStateOf<String?>(null) }
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
    val onFocusedNoteFacetChanged: (FacetKey, Boolean) -> Unit = { facetKey, isFocused ->
        val facetKeyLabel = facetKeyString(facetKey)
        focusedNoteFacetLabel =
            if (isFocused) facetKeyLabel else focusedNoteFacetLabel?.takeUnless { it == facetKeyLabel }
    }
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

    BoxWithConstraints(modifier = modifier.fillMaxSize().imePadding().testTag(DaybookEditorSemantics.Editor)) {
        val narrowScreen = maxWidth < 600.dp
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
                    val isActionMenuOpen = blockActionExpandedStates[facetKeyLabel] == true
                    val isUsingStickyActions = stickyFacetPlacement?.descriptor?.facetKey == descriptor.facetKey
                    FacetBlock(
                        descriptor = descriptor,
                        doc = state.doc,
                        branchPath = state.branchPath,
                        controller = controller,
                        modifier = Modifier,
                        canShowMenu = state.docId != null,
                        isCollapsed = isCollapsed,
                        isActionMenuOpen = isActionMenuOpen,
                        onActionMenuOpenChange = { blockActionExpandedStates[facetKeyLabel] = it },
                        showInlineActions = !isUsingStickyActions,
                        noteDraft = state.noteEditors[descriptor.facetKey]?.draft,
                        noteEditable = state.noteEditors[descriptor.facetKey]?.editable ?: false,
                        noteNotice = state.noteEditors[descriptor.facetKey]?.notice,
                        onNoteFocusChanged = { isFocused ->
                            onFocusedNoteFacetChanged(descriptor.facetKey, isFocused)
                        },
                        displayHints = displayHints,
                        canMoveUp = index > 0,
                        canMoveDown = index < state.contentFacetViews.lastIndex,
                        onAddBlockRequested = {
                            onAddBlockRequested(descriptor.facetKey)
                        },
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
            val isActionMenuOpen = blockActionExpandedStates[facetKeyLabel] == true
            val stickyActionsHoverSource = remember(descriptor.facetKey) { MutableInteractionSource() }
            val isStickyActionRowHovered by stickyActionsHoverSource.collectIsHoveredAsState()
            FacetBlockActionsMenu(
                facetKeyLabel = facetKeyLabel,
                isPrimary = descriptor.isPrimary,
                actions =
                FacetBlockActions(
                    canShowMenu = state.docId != null,
                    canMoveUp = placement.facetIndex > 0,
                    canMoveDown = placement.facetIndex < state.contentFacetViews.lastIndex,
                    isCollapsed = isCollapsed,
                    onAddBlockRequested = {
                        onAddBlockRequested(descriptor.facetKey)
                    },
                    onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
                    onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
                    onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
                    onToggleCollapse = {
                        collapsedFacetStates[facetKeyLabel] = !(collapsedFacetStates[facetKeyLabel] == true)
                    },
                ),
                isMenuOpen = isActionMenuOpen,
                showOverflowButton = isActionMenuOpen || isStickyActionRowHovered,
                onMenuOpenChange = { blockActionExpandedStates[facetKeyLabel] = it },
                showQuickActions = isActionMenuOpen || isStickyActionRowHovered,
                collapseButtonIcon =
                if (isCollapsed) {
                    Icons.Default.KeyboardArrowDown
                } else {
                    Icons.Default.KeyboardArrowUp
                },
                overflowButtonEmphasized = isActionMenuOpen || isStickyActionRowHovered,
                enableInvisibleHoverTarget = true,
                modifier =
                Modifier
                    .align(Alignment.TopEnd)
                    .offset { IntOffset(0, placement.yOffsetPx) }
                    .padding(top = 2.dp)
                    .onSizeChanged { stickyFacetActionsHeightPx = it.height },
                interactionSource = stickyActionsHoverSource,
            )
        }

        EditorBottomOverlayLane(
            narrowScreen = narrowScreen,
            contentFacetViews = state.contentFacetViews,
            focusedNoteFacetLabel = focusedNoteFacetLabel,
            isAddBlockPickerOpen = isAddBlockPickerOpen,
            onAddBlockRequested = onAddBlockRequested,
            snackbarHostState = snackbarHostState,
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
    isActionMenuOpen: Boolean,
    onActionMenuOpenChange: (Boolean) -> Unit,
    showInlineActions: Boolean,
    noteDraft: String?,
    noteEditable: Boolean,
    noteNotice: String?,
    onNoteFocusChanged: (Boolean) -> Unit,
    displayHints: Map<String, FacetDisplayHint>,
    canMoveUp: Boolean,
    canMoveDown: Boolean,
    onAddBlockRequested: () -> Unit,
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
    val isBlockHovered by interactionSource.collectIsHoveredAsState()
    val blockActionsInteractionSource = remember { MutableInteractionSource() }
    val isActionRowHovered by blockActionsInteractionSource.collectIsHoveredAsState()
    val showOverflowButton =
        canShowMenu && (isCollapsed || isBlockHovered || isActionMenuOpen || isActionRowHovered)
    val showQuickActions = isActionMenuOpen || isActionRowHovered
    val customActions =
        blockCustomActions(
            canShowMenu = canShowMenu,
            isCollapsed = isCollapsed,
            onOpenMenu = { onActionMenuOpenChange(true) },
            onAddBlockRequested = onAddBlockRequested,
            onToggleCollapse = onToggleCollapse,
        )

    Box(
        modifier =
        modifier
            .fillMaxWidth()
            .hoverable(interactionSource)
            .semantics {
                this.customActions = customActions
                contentDescription =
                    if (isCollapsed) {
                        blockSummary.contentDescription
                    } else if (descriptor.isPrimary) {
                        "Primary document block"
                    } else {
                        "Document block"
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
                        onFocusChanged = onNoteFocusChanged,
                        onDraftChange = { nextValue ->
                            controller.setNoteDraft(descriptor.facetKey, nextValue)
                        },
                    ),
                    onUiError = onUiError,
                )
            }
        }
        if (showInlineActions) {
            val blockActions =
                FacetBlockActions(
                    canShowMenu = canShowMenu,
                    canMoveUp = canMoveUp,
                    canMoveDown = canMoveDown,
                    isCollapsed = isCollapsed,
                    onAddBlockRequested = onAddBlockRequested,
                    onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
                    onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
                    onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
                    onToggleCollapse = onToggleCollapse,
                )
            FacetBlockActionsMenu(
                facetKeyLabel = facetKeyLabel,
                isPrimary = descriptor.isPrimary,
                actions = blockActions,
                isMenuOpen = isActionMenuOpen,
                showOverflowButton = showOverflowButton,
                onMenuOpenChange = onActionMenuOpenChange,
                showQuickActions = showQuickActions,
                collapseButtonIcon =
                if (isCollapsed) {
                    Icons.Default.KeyboardArrowDown
                } else {
                    Icons.Default.KeyboardArrowUp
                },
                overflowButtonEmphasized = isBlockHovered || isActionMenuOpen || isActionRowHovered,
                enableInvisibleHoverTarget = false,
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
    val onAddBlockRequested: () -> Unit,
    val onMakePrimary: () -> Unit,
    val onMoveUp: () -> Unit,
    val onMoveDown: () -> Unit,
    val onToggleCollapse: () -> Unit,
)

private fun blockCustomActions(
    canShowMenu: Boolean,
    isCollapsed: Boolean,
    onOpenMenu: () -> Unit,
    onAddBlockRequested: () -> Unit,
    onToggleCollapse: () -> Unit,
): List<CustomAccessibilityAction> {
    if (!canShowMenu) {
        return emptyList()
    }
    return listOf(
        CustomAccessibilityAction("Block actions") {
            onOpenMenu()
            true
        },
        CustomAccessibilityAction("Add block below") {
            onAddBlockRequested()
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
}

@Composable
private fun FacetBlockActionsMenu(
    facetKeyLabel: String,
    isPrimary: Boolean,
    actions: FacetBlockActions,
    isMenuOpen: Boolean,
    showOverflowButton: Boolean,
    onMenuOpenChange: (Boolean) -> Unit,
    showQuickActions: Boolean,
    collapseButtonIcon: ImageVector,
    overflowButtonEmphasized: Boolean,
    enableInvisibleHoverTarget: Boolean,
    interactionSource: MutableInteractionSource,
    modifier: Modifier = Modifier,
) {
    if (!actions.canShowMenu) {
        return
    }
    val hasVisibleControls = showQuickActions || showOverflowButton || isMenuOpen
    if (!hasVisibleControls && !enableInvisibleHoverTarget) {
        return
    }

    val rowBackgroundColor =
        if (showQuickActions) {
            MaterialTheme.colorScheme.surfaceContainer
        } else {
            MaterialTheme.colorScheme.surfaceContainer.copy(alpha = 0.48f)
        }

    Box(modifier = modifier) {
        if (hasVisibleControls) {
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
                if (showQuickActions) {
                    FacetBlockActionButton(
                        imageVector = Icons.Default.Add,
                        tooltipText = "Add block below",
                        contentDescription = "Add block below",
                        testTag = DaybookEditorSemantics.addBlockAfterQuickAction(facetKeyLabel),
                        onClick = {
                            onMenuOpenChange(false)
                            actions.onAddBlockRequested()
                        },
                        iconAlpha = 1f,
                    )
                }
                if (showQuickActions) {
                    FacetBlockActionButton(
                        imageVector = collapseButtonIcon,
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
                if (showOverflowButton || isMenuOpen) {
                    FacetBlockActionButton(
                        imageVector = Icons.Default.MoreVert,
                        contentDescription = "Block actions",
                        testTag = DaybookEditorSemantics.blockActions(facetKeyLabel),
                        onClick = { onMenuOpenChange(true) },
                        iconAlpha = if (overflowButtonEmphasized) 1f else 0.48f,
                    )
                }
            }
        } else {
            Box(
                modifier =
                Modifier
                    .size(44.dp)
                    .hoverable(interactionSource),
            )
        }
        DropdownMenu(
            expanded = isMenuOpen,
            onDismissRequest = { onMenuOpenChange(false) },
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
                    onMenuOpenChange(false)
                    actions.onToggleCollapse()
                },
            )
            DropdownMenuItem(
                text = { Text(if (isPrimary) "Primary block" else "Make primary") },
                enabled = !isPrimary,
                modifier = Modifier.testTag(DaybookEditorSemantics.makePrimaryAction(facetKeyLabel)),
                onClick = {
                    onMenuOpenChange(false)
                    actions.onMakePrimary()
                },
            )
            DropdownMenuItem(
                text = { Text("Move up") },
                enabled = actions.canMoveUp,
                modifier = Modifier.testTag(DaybookEditorSemantics.moveUpAction(facetKeyLabel)),
                onClick = {
                    onMenuOpenChange(false)
                    actions.onMoveUp()
                },
            )
            DropdownMenuItem(
                text = { Text("Move down") },
                enabled = actions.canMoveDown,
                modifier = Modifier.testTag(DaybookEditorSemantics.moveDownAction(facetKeyLabel)),
                onClick = {
                    onMenuOpenChange(false)
                    actions.onMoveDown()
                },
            )
            DropdownMenuItem(
                text = { Text("Add block below") },
                modifier = Modifier.testTag(DaybookEditorSemantics.addBlockAfterAction(facetKeyLabel)),
                onClick = {
                    onMenuOpenChange(false)
                    actions.onAddBlockRequested()
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

private data class AddBlockOptionSpec(val id: String, val title: String, val description: String) {
    fun matches(query: String): Boolean {
        val normalized = query.trim()
        if (normalized.isBlank()) {
            return true
        }
        return title.contains(normalized, ignoreCase = true) || description.contains(normalized, ignoreCase = true)
    }
}

private fun addBlockOptions(): List<AddBlockOptionSpec> = listOf(
    AddBlockOptionSpec(
        id = "note",
        title = "Note",
        description = "Plain text note block",
    ),
)

@Composable
internal fun rememberAddBlockDialogLauncher(controller: EditorSessionController): (FacetKey) -> Unit {
    val bigDialogController = LocalBigDialogController.current
    return remember(controller, bigDialogController) {
        { facetKey ->
            bigDialogController.show {
                var searchQuery by remember { mutableStateOf("") }
                AddBlockPickerContent(
                    searchQuery = searchQuery,
                    onSearchQueryChange = { nextValue -> searchQuery = nextValue },
                    autoFocusSearch = false,
                    onOptionSelected = { optionId ->
                        bigDialogController.dismiss()
                        when (optionId) {
                            "note" -> controller.addNoteFacetAfter(facetKey)
                            else -> error("Unknown add-block option: $optionId")
                        }
                    },
                )
            }
        }
    }
}

private sealed interface EditorBottomOverlay {
    data object None : EditorBottomOverlay

    data class FocusedNoteAccessoryBar(val facetKey: FacetKey) : EditorBottomOverlay
}

@Composable
private fun EditorBottomOverlayLane(
    narrowScreen: Boolean,
    contentFacetViews: List<FacetViewDescriptor>,
    focusedNoteFacetLabel: String?,
    isAddBlockPickerOpen: Boolean,
    onAddBlockRequested: (FacetKey) -> Unit,
    snackbarHostState: SnackbarHostState,
) {
    Box(modifier = Modifier.fillMaxSize()) {
        val imeVisible = WindowInsets.ime.getBottom(LocalDensity.current) > 0
        val activeOverlay =
            if (!narrowScreen || !imeVisible || focusedNoteFacetLabel == null || isAddBlockPickerOpen) {
                EditorBottomOverlay.None
            } else {
                val activeFocusedNoteFacetKey =
                    contentFacetViews.firstOrNull { descriptor ->
                        facetKeyString(descriptor.facetKey) == focusedNoteFacetLabel
                    }?.facetKey
                if (activeFocusedNoteFacetKey != null) {
                    EditorBottomOverlay.FocusedNoteAccessoryBar(activeFocusedNoteFacetKey)
                } else {
                    EditorBottomOverlay.None
                }
            }

        Box(modifier = Modifier.fillMaxSize()) {
            when (activeOverlay) {
                is EditorBottomOverlay.FocusedNoteAccessoryBar -> {
                    FocusedNoteAccessoryBar(
                        facetKey = activeOverlay.facetKey,
                        modifier = Modifier.align(Alignment.BottomCenter),
                        onAddBlockRequested = { onAddBlockRequested(activeOverlay.facetKey) },
                    )
                }

                EditorBottomOverlay.None -> Unit
            }

            val snackbarBottomPadding =
                when (activeOverlay) {
                    is EditorBottomOverlay.FocusedNoteAccessoryBar -> 72.dp
                    EditorBottomOverlay.None -> 8.dp
                }

            SnackbarHost(
                hostState = snackbarHostState,
                modifier =
                Modifier
                    .align(Alignment.BottomCenter)
                    .padding(
                        bottom = snackbarBottomPadding,
                        start = 8.dp,
                        end = 8.dp,
                    ),
            )
        }
    }
}

@Composable
private fun FocusedNoteAccessoryBar(
    facetKey: FacetKey,
    modifier: Modifier = Modifier,
    onAddBlockRequested: () -> Unit = {},
) {
    Box(
        modifier =
        modifier
            .padding(horizontal = 12.dp, vertical = 8.dp)
            .testTag(DaybookEditorSemantics.focusedNoteAccessoryBar()),
    ) {
        Surface(
            tonalElevation = 1.dp,
            shadowElevation = 4.dp,
            shape = RoundedCornerShape(999.dp),
            color = MaterialTheme.colorScheme.surfaceContainer,
        ) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(4.dp),
            ) {
                TextButton(
                    onClick = onAddBlockRequested,
                    modifier =
                    Modifier
                        .testTag(DaybookEditorSemantics.focusedNoteAccessoryAddBlockAction(facetKeyString(facetKey)))
                        .semantics {
                            contentDescription = "Add block after note"
                        },
                ) {
                    Text("Add block")
                }
            }
        }
    }
}

@Composable
internal fun AddBlockPickerContent(
    searchQuery: String,
    onSearchQueryChange: (String) -> Unit,
    onOptionSelected: (String) -> Unit,
    autoFocusSearch: Boolean,
    modifier: Modifier = Modifier,
) {
    val options = remember { addBlockOptions() }
    val visibleOptions by remember(searchQuery, options) {
        derivedStateOf { options.filter { option -> option.matches(searchQuery) } }
    }
    val searchFocusRequester = remember { FocusRequester() }

    LaunchedEffect(autoFocusSearch) {
        if (autoFocusSearch) {
            searchFocusRequester.requestFocus()
        }
    }

    Column(
        modifier =
        modifier
            .fillMaxSize()
            .padding(24.dp)
            .testTag(DaybookEditorSemantics.addBlockDialog()),
    ) {
        Text(
            text = "Add block",
            style = MaterialTheme.typography.titleLarge,
        )
        Text(
            text = "Choose a block type",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(top = 4.dp),
        )
        TextField(
            value = searchQuery,
            onValueChange = onSearchQueryChange,
            placeholder = { Text("Search block types") },
            modifier =
            Modifier
                .fillMaxWidth()
                .padding(top = 16.dp)
                .focusRequester(searchFocusRequester)
                .testTag(DaybookEditorSemantics.addBlockSearchField())
                .semantics {
                    contentDescription = "Search block types"
                },
            singleLine = true,
        )
        if (visibleOptions.isEmpty()) {
            Text(
                text = "No block types match",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(top = 24.dp),
            )
        } else {
            LazyColumn(
                modifier =
                Modifier
                    .fillMaxWidth()
                    .padding(top = 16.dp)
                    .heightIn(max = 360.dp),
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                items(
                    items = visibleOptions,
                    key = { option -> option.id },
                ) { option ->
                    AddBlockOptionRow(
                        option = option,
                        onClick = { onOptionSelected(option.id) },
                    )
                }
            }
        }
    }
}

@Composable
private fun AddBlockOptionRow(option: AddBlockOptionSpec, onClick: () -> Unit) {
    Surface(
        shape = RoundedCornerShape(16.dp),
        tonalElevation = 0.dp,
        color = MaterialTheme.colorScheme.surfaceContainerHighest,
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(DaybookEditorSemantics.addBlockOption(option.id))
            .semantics {
                contentDescription = "${option.title}. ${option.description}"
            }
            .clickable(onClick = onClick),
    ) {
        Column(modifier = Modifier.padding(16.dp)) {
            Text(
                text = option.title,
                style = MaterialTheme.typography.titleSmall,
            )
            Text(
                text = option.description,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(top = 4.dp),
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
