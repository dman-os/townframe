@file:Suppress("FunctionNaming")

@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxScope
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
import androidx.compose.foundation.lazy.LazyListScope
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material.icons.filled.MoreVert
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
import org.example.daybook.ui.editor.EditorSessionState
import org.example.daybook.ui.editor.FacetEditorKind
import org.example.daybook.ui.editor.FacetViewDescriptor
import org.example.daybook.ui.editor.dmetaFacetKey
import org.example.daybook.ui.editor.facetDisplayHintKey
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey

private data class FacetBlockSummary(val title: String, val preview: String?, val contentDescription: String)

private data class StickyFacetActionPlacement(
    val descriptor: FacetViewDescriptor,
    val facetIndex: Int,
    val yOffsetPx: Int,
)

internal data class DocEditorArgs(
    val controller: EditorSessionController,
    val showInlineFacetRack: Boolean = false,
    val displayHints: Map<String, FacetDisplayHint> = emptyMap(),
    val displayHintsError: String? = null,
    val isAddBlockPickerOpen: Boolean = false,
    val onAddBlockRequested: (FacetKey) -> Unit,
)

@Composable
internal fun DocEditor(args: DocEditorArgs, modifier: Modifier = Modifier) {
    val layoutArgs = rememberDocEditorLayoutArgs(args)
    DocEditorLayout(args = layoutArgs, modifier = modifier)
}

@Composable
private fun rememberDocEditorLayoutArgs(args: DocEditorArgs): DocEditorLayoutArgs {
    val state by args.controller.state.collectAsState()
    val snackbarHostState = remember { SnackbarHostState() }
    val collapsedFacetStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    val blockActionExpandedStates = remember(state.docId) { mutableStateMapOf<String, Boolean>() }
    var focusedNoteFacetLabel by remember(state.docId) { mutableStateOf<String?>(null) }
    var uiMessage by remember { mutableStateOf<String?>(null) }

    DocEditorSnackbarEffects(
        snackbarHostState = snackbarHostState,
        saveError = state.saveError,
        uiMessage = uiMessage,
        onUiMessageConsumed = { uiMessage = null },
    )

    val listState = rememberLazyListState()
    val facetListStartIndex =
        remember(state.titleNotice, args.displayHintsError, state.contentFacetViews.isNotEmpty()) {
            docEditorFacetListStartIndex(
                titleNotice = state.titleNotice,
                displayHintsError = args.displayHintsError,
                hasFacetRows = state.contentFacetViews.isNotEmpty(),
            )
        }
    var stickyFacetActionsHeightPx by remember(state.docId) { mutableStateOf(0) }
    val onFocusedNoteFacetChanged: (FacetKey, Boolean) -> Unit = { facetKey, isFocused ->
        val facetKeyLabel = facetKeyString(facetKey)
        focusedNoteFacetLabel =
            if (isFocused) facetKeyLabel else focusedNoteFacetLabel?.takeUnless { it == facetKeyLabel }
    }

    DocEditorScrollEffect(
        args =
        DocEditorScrollEffectArgs(
            scrollToFacetRequest = state.scrollToFacetRequest,
            contentFacetViews = state.contentFacetViews,
            titleNotice = state.titleNotice,
            displayHintsError = args.displayHintsError,
            facetListStartIndex = facetListStartIndex,
            listState = listState,
        ),
    )

    return DocEditorLayoutArgs(
        controller = args.controller,
        state = state,
        showInlineFacetRack = args.showInlineFacetRack,
        displayHints = args.displayHints,
        displayHintsError = args.displayHintsError,
        isAddBlockPickerOpen = args.isAddBlockPickerOpen,
        onAddBlockRequested = args.onAddBlockRequested,
        snackbarHostState = snackbarHostState,
        collapsedFacetStates = collapsedFacetStates,
        blockActionExpandedStates = blockActionExpandedStates,
        focusedNoteFacetLabel = focusedNoteFacetLabel,
        onFocusedNoteFacetChanged = onFocusedNoteFacetChanged,
        onUiError = { message -> uiMessage = message },
        listState = listState,
        stickyFacetActionsHeightPx = stickyFacetActionsHeightPx,
        onStickyFacetActionsHeightChanged = { stickyFacetActionsHeightPx = it },
    )
}

@Composable
private fun DocEditorSnackbarEffects(
    snackbarHostState: SnackbarHostState,
    saveError: String?,
    uiMessage: String?,
    onUiMessageConsumed: () -> Unit,
) {
    LaunchedEffect(saveError) {
        val errorMessage = saveError ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(errorMessage)
    }
    LaunchedEffect(uiMessage) {
        val nextMessage = uiMessage ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(nextMessage)
        onUiMessageConsumed()
    }
}

private data class DocEditorLayoutArgs(
    val controller: EditorSessionController,
    val state: EditorSessionState,
    val showInlineFacetRack: Boolean,
    val displayHints: Map<String, FacetDisplayHint>,
    val displayHintsError: String?,
    val isAddBlockPickerOpen: Boolean,
    val onAddBlockRequested: (FacetKey) -> Unit,
    val snackbarHostState: SnackbarHostState,
    val collapsedFacetStates: MutableMap<String, Boolean>,
    val blockActionExpandedStates: MutableMap<String, Boolean>,
    val focusedNoteFacetLabel: String?,
    val onFocusedNoteFacetChanged: (FacetKey, Boolean) -> Unit,
    val onUiError: (String) -> Unit,
    val listState: androidx.compose.foundation.lazy.LazyListState,
    val stickyFacetActionsHeightPx: Int,
    val onStickyFacetActionsHeightChanged: (Int) -> Unit,
)

private data class DocEditorScrollEffectArgs(
    val scrollToFacetRequest: org.example.daybook.ui.editor.ScrollToFacetRequest?,
    val contentFacetViews: List<FacetViewDescriptor>,
    val titleNotice: String?,
    val displayHintsError: String?,
    val facetListStartIndex: Int,
    val listState: androidx.compose.foundation.lazy.LazyListState,
)

@Composable
private fun DocEditorScrollEffect(args: DocEditorScrollEffectArgs) {
    LaunchedEffect(
        args.scrollToFacetRequest?.seq,
        args.contentFacetViews,
        args.titleNotice,
        args.displayHintsError,
    ) {
        val request = args.scrollToFacetRequest ?: return@LaunchedEffect
        val targetIndex = args.contentFacetViews.indexOfFirst { it.facetKey == request.facetKey }
        if (targetIndex >= 0) {
            args.listState.animateScrollToItem(args.facetListStartIndex + targetIndex)
        }
    }
}

@Composable
private fun DocEditorLayout(args: DocEditorLayoutArgs, modifier: Modifier = Modifier) {
    BoxWithConstraints(modifier = modifier.fillMaxSize().imePadding().testTag(DaybookEditorSemantics.EDITOR)) {
        val narrowScreen = maxWidth < 600.dp
        val facetListStartIndex =
            remember(args.state.titleNotice, args.displayHintsError, args.state.contentFacetViews.isNotEmpty()) {
                docEditorFacetListStartIndex(
                    titleNotice = args.state.titleNotice,
                    displayHintsError = args.displayHintsError,
                    hasFacetRows = args.state.contentFacetViews.isNotEmpty(),
                )
            }
        val stickyFacetPlacement by remember(
            args.state.contentFacetViews,
            args.state.titleNotice,
            args.displayHintsError,
            facetListStartIndex,
            args.listState,
            args.stickyFacetActionsHeightPx,
        ) {
            derivedStateOf {
                resolveStickyFacetActionPlacement(
                    layoutInfo = args.listState.layoutInfo,
                    facetViews = args.state.contentFacetViews,
                    facetListStartIndex = facetListStartIndex,
                    stickyFacetActionsHeightPx = args.stickyFacetActionsHeightPx,
                )
            }
        }

        DocEditorFacetList(
            args = args,
            stickyFacetPlacement = stickyFacetPlacement,
        )

        DocEditorStickyFacetOverlay(
            args = args,
            stickyFacetPlacement = stickyFacetPlacement,
        )

        EditorBottomOverlayLane(
            args =
            EditorBottomOverlayLaneArgs(
                narrowScreen = narrowScreen,
                contentFacetViews = args.state.contentFacetViews,
                focusedNoteFacetLabel = args.focusedNoteFacetLabel,
                isAddBlockPickerOpen = args.isAddBlockPickerOpen,
                onAddBlockRequested = args.onAddBlockRequested,
                snackbarHostState = args.snackbarHostState,
            ),
        )
    }
}

@Composable
private fun DocEditorFacetList(args: DocEditorLayoutArgs, stickyFacetPlacement: StickyFacetActionPlacement?) {
    LazyColumn(
        state = args.listState,
        modifier =
        Modifier
            .fillMaxSize()
            .testTag(DaybookEditorSemantics.EDITOR_LIST),
    ) {
        docEditorTitleSection(args.controller, args.state)
        docEditorTitleNoticeSection(args.state.titleNotice)
        docEditorTitleDividerSection()
        docEditorDisplayHintsErrorSection(args.displayHintsError)
        if (args.state.contentFacetViews.isEmpty()) {
            docEditorEmptyFacetSection()
        } else {
            docEditorFacetRows(
                args = args,
                stickyFacetPlacement = stickyFacetPlacement,
            )
        }
        if (args.showInlineFacetRack) {
            docEditorDetailsSection(args.state)
        }
    }
}

private fun LazyListScope.docEditorTitleSection(controller: EditorSessionController, state: EditorSessionState) {
    item(key = "title") {
        TextField(
            value = state.titleDraft,
            onValueChange = { value -> controller.setTitleDraft(value) },
            modifier =
            Modifier
                .fillMaxWidth()
                .testTag(DaybookEditorSemantics.TITLE_FIELD)
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
}

private fun LazyListScope.docEditorTitleNoticeSection(titleNotice: String?) {
    titleNotice?.let { notice ->
        item(key = "title-notice") {
            Text(
                text = notice,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

private fun LazyListScope.docEditorTitleDividerSection() {
    item(key = "title-divider") {
        HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
    }
}

private fun LazyListScope.docEditorDisplayHintsErrorSection(message: String?) {
    message?.let { error ->
        item(key = "display-hints-error") {
            FacetStatusText(
                text = "Facet display config unavailable: $error",
                modifier = Modifier.padding(bottom = 8.dp),
            )
        }
    }
}

private fun LazyListScope.docEditorEmptyFacetSection() {
    item(key = "no-facets") {
        Text(
            text = "No facets",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

private fun LazyListScope.docEditorFacetRows(
    args: DocEditorLayoutArgs,
    stickyFacetPlacement: StickyFacetActionPlacement?,
) {
    itemsIndexed(
        items = args.state.contentFacetViews,
        key = { _, descriptor -> facetKeyString(descriptor.facetKey) },
    ) { index, descriptor ->
        val facetKeyLabel = facetKeyString(descriptor.facetKey)
        val isCollapsed = args.collapsedFacetStates[facetKeyLabel] == true
        val isActionMenuOpen = args.blockActionExpandedStates[facetKeyLabel] == true
        val isUsingStickyActions = stickyFacetPlacement?.descriptor?.facetKey == descriptor.facetKey
        FacetBlock(
            args =
            FacetBlockArgs(
                descriptor = descriptor,
                doc = args.state.doc,
                branchPath = args.state.branchPath,
                controller = args.controller,
                canShowMenu = args.state.docId != null,
                isCollapsed = isCollapsed,
                isActionMenuOpen = isActionMenuOpen,
                onActionMenuOpenChange = { args.blockActionExpandedStates[facetKeyLabel] = it },
                showInlineActions = !isUsingStickyActions,
                noteDraft = args.state.noteEditors[descriptor.facetKey]?.draft,
                noteEditable = args.state.noteEditors[descriptor.facetKey]?.editable ?: false,
                noteNotice = args.state.noteEditors[descriptor.facetKey]?.notice,
                onNoteFocusChanged = { isFocused ->
                    args.onFocusedNoteFacetChanged(descriptor.facetKey, isFocused)
                },
                displayHints = args.displayHints,
                canMoveUp = index > 0,
                canMoveDown = index < args.state.contentFacetViews.lastIndex,
                onAddBlockRequested = { args.onAddBlockRequested(descriptor.facetKey) },
                onToggleCollapse = {
                    args.collapsedFacetStates[facetKeyLabel] = !(args.collapsedFacetStates[facetKeyLabel] == true)
                },
                onUiError = args.onUiError,
            ),
        )
        if (index < args.state.contentFacetViews.lastIndex) {
            Spacer(modifier = Modifier.height(8.dp))
        }
    }
}

private fun LazyListScope.docEditorDetailsSection(state: EditorSessionState) {
    item(key = "details") {
        Column(
            modifier =
            Modifier
                .fillMaxWidth()
                .testTag(DaybookEditorSemantics.DETAILS),
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

@Composable
private fun DocEditorStickyFacetOverlay(args: DocEditorLayoutArgs, stickyFacetPlacement: StickyFacetActionPlacement?) {
    Box(modifier = Modifier.fillMaxSize()) {
        stickyFacetPlacement?.let { placement ->
            val descriptor = placement.descriptor
            val facetKeyLabel = facetKeyString(descriptor.facetKey)
            val isCollapsed = args.collapsedFacetStates[facetKeyLabel] == true
            val isActionMenuOpen = args.blockActionExpandedStates[facetKeyLabel] == true
            val stickyActionsHoverSource = remember(descriptor.facetKey) { MutableInteractionSource() }
            val isStickyActionRowHovered by stickyActionsHoverSource.collectIsHoveredAsState()
            FacetBlockActionsMenu(
                args =
                FacetBlockActionsMenuArgs(
                    facetKeyLabel = facetKeyLabel,
                    isPrimary = descriptor.isPrimary,
                    actions =
                    FacetBlockActions(
                        canShowMenu = args.state.docId != null,
                        canMoveUp = placement.facetIndex > 0,
                        canMoveDown = placement.facetIndex < args.state.contentFacetViews.lastIndex,
                        isCollapsed = isCollapsed,
                        onAddBlockRequested = { args.onAddBlockRequested(descriptor.facetKey) },
                        onMakePrimary = { args.controller.makeFacetPrimary(descriptor.facetKey) },
                        onMoveUp = { args.controller.moveFacetEarlier(descriptor.facetKey) },
                        onMoveDown = { args.controller.moveFacetLater(descriptor.facetKey) },
                        onToggleCollapse = {
                            args.collapsedFacetStates[facetKeyLabel] =
                                !(args.collapsedFacetStates[facetKeyLabel] == true)
                        },
                    ),
                    isMenuOpen = isActionMenuOpen,
                    showOverflowButton = isActionMenuOpen || isStickyActionRowHovered,
                    onMenuOpenChange = { args.blockActionExpandedStates[facetKeyLabel] = it },
                    showQuickActions = isActionMenuOpen || isStickyActionRowHovered,
                    collapseButtonIcon =
                    if (isCollapsed) {
                        Icons.Default.KeyboardArrowDown
                    } else {
                        Icons.Default.KeyboardArrowUp
                    },
                    overflowButtonEmphasized = isActionMenuOpen || isStickyActionRowHovered,
                    enableInvisibleHoverTarget = true,
                    interactionSource = stickyActionsHoverSource,
                ),
                modifier =
                Modifier
                    .align(Alignment.TopEnd)
                    .offset { IntOffset(0, placement.yOffsetPx) }
                    .padding(top = 2.dp)
                    .onSizeChanged { args.onStickyFacetActionsHeightChanged(it.height) },
            )
        }
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
    val facetIndex = visibleFacetItem.index - facetListStartIndex
    val descriptor = facetViews.getOrNull(facetIndex)
    val nextFacetOffset =
        if (visibleFacetItem.offset < 0 && descriptor != null) {
            if (facetIndex < facetViews.lastIndex) {
                layoutInfo.visibleItemsInfo.firstOrNull { it.index == visibleFacetItem.index + 1 }?.offset
            } else {
                null
            }
        } else {
            null
        }
    return descriptor?.takeIf { visibleFacetItem.offset < 0 }?.let {
        StickyFacetActionPlacement(
            descriptor = it,
            facetIndex = facetIndex,
            yOffsetPx = nextFacetOffset?.let { offset -> minOf(0, offset - stickyFacetActionsHeightPx) } ?: 0,
        )
    }
}

private data class FacetBlockArgs(
    val descriptor: FacetViewDescriptor,
    val doc: Doc?,
    val branchPath: String,
    val controller: EditorSessionController,
    val canShowMenu: Boolean,
    val isCollapsed: Boolean,
    val isActionMenuOpen: Boolean,
    val onActionMenuOpenChange: (Boolean) -> Unit,
    val showInlineActions: Boolean,
    val noteDraft: String?,
    val noteEditable: Boolean,
    val noteNotice: String?,
    val onNoteFocusChanged: (Boolean) -> Unit,
    val displayHints: Map<String, FacetDisplayHint>,
    val canMoveUp: Boolean,
    val canMoveDown: Boolean,
    val onAddBlockRequested: () -> Unit,
    val onToggleCollapse: () -> Unit,
    val onUiError: (String) -> Unit,
)

private data class FacetBlockActionsMenuArgs(
    val facetKeyLabel: String,
    val isPrimary: Boolean,
    val actions: FacetBlockActions,
    val isMenuOpen: Boolean,
    val showOverflowButton: Boolean,
    val onMenuOpenChange: (Boolean) -> Unit,
    val showQuickActions: Boolean,
    val collapseButtonIcon: ImageVector,
    val overflowButtonEmphasized: Boolean,
    val enableInvisibleHoverTarget: Boolean,
    val interactionSource: MutableInteractionSource,
)

private data class FacetBlockActionButtonArgs(
    val imageVector: ImageVector,
    val contentDescription: String,
    val testTag: String,
    val onClick: () -> Unit,
    val tooltipText: String? = null,
    val iconAlpha: Float = 1f,
)

private data class EditorBottomOverlayLaneArgs(
    val narrowScreen: Boolean,
    val contentFacetViews: List<FacetViewDescriptor>,
    val focusedNoteFacetLabel: String?,
    val isAddBlockPickerOpen: Boolean,
    val onAddBlockRequested: (FacetKey) -> Unit,
    val snackbarHostState: SnackbarHostState,
)

private data class FacetBlockUiState(
    val blockSummary: FacetBlockSummary,
    val interactionSource: MutableInteractionSource,
    val blockActionsInteractionSource: MutableInteractionSource,
    val isBlockHovered: Boolean,
    val isActionRowHovered: Boolean,
    val showOverflowButton: Boolean,
    val showQuickActions: Boolean,
    val customActions: List<CustomAccessibilityAction>,
)

private data class FacetBlockActionsOverlayArgs(
    val args: FacetBlockArgs,
    val isBlockHovered: Boolean,
    val isActionRowHovered: Boolean,
    val showOverflowButton: Boolean,
    val showQuickActions: Boolean,
    val interactionSource: MutableInteractionSource,
)

@Composable
private fun rememberFacetBlockUiState(args: FacetBlockArgs, displayHint: FacetDisplayHint?): FacetBlockUiState {
    val interactionSource = remember { MutableInteractionSource() }
    val isBlockHovered by interactionSource.collectIsHoveredAsState()
    val blockActionsInteractionSource = remember { MutableInteractionSource() }
    val isActionRowHovered by blockActionsInteractionSource.collectIsHoveredAsState()
    val blockSummary =
        remember(
            args.descriptor.facetKey,
            args.descriptor.kind,
            args.descriptor.rawValue,
            args.descriptor.isPrimary,
            displayHint?.displayTitle,
            args.noteDraft,
        ) {
            buildFacetBlockSummary(
                descriptor = args.descriptor,
                displayHint = displayHint,
                noteDraft = args.noteDraft,
            )
        }
    val showOverflowButton =
        args.canShowMenu &&
            (args.isCollapsed || isBlockHovered || args.isActionMenuOpen || isActionRowHovered)
    val showQuickActions = args.isActionMenuOpen || isActionRowHovered
    val customActions =
        blockCustomActions(
            canShowMenu = args.canShowMenu,
            isCollapsed = args.isCollapsed,
            onOpenMenu = { args.onActionMenuOpenChange(true) },
            onAddBlockRequested = args.onAddBlockRequested,
            onToggleCollapse = args.onToggleCollapse,
        )
    return FacetBlockUiState(
        blockSummary = blockSummary,
        interactionSource = interactionSource,
        blockActionsInteractionSource = blockActionsInteractionSource,
        isBlockHovered = isBlockHovered,
        isActionRowHovered = isActionRowHovered,
        showOverflowButton = showOverflowButton,
        showQuickActions = showQuickActions,
        customActions = customActions,
    )
}

@Composable
private fun FacetBlock(args: FacetBlockArgs, modifier: Modifier = Modifier) {
    val displayHintKey = facetDisplayHintKey(args.descriptor.facetKey)
    val displayHint = args.displayHints[displayHintKey]
    val facetKeyLabel = facetKeyString(args.descriptor.facetKey)
    val uiState = rememberFacetBlockUiState(args = args, displayHint = displayHint)

    Box(
        modifier =
        modifier
            .fillMaxWidth()
            .hoverable(uiState.interactionSource)
            .semantics {
                this.customActions = uiState.customActions
                contentDescription =
                    if (args.isCollapsed) {
                        uiState.blockSummary.contentDescription
                    } else if (args.descriptor.isPrimary) {
                        "Primary document block"
                    } else {
                        "Document block"
                    }
            }
            .testTag(DaybookEditorSemantics.facetRow(facetKeyLabel)),
    ) {
        if (args.isCollapsed) {
            FacetBlockCollapsedSummary(summary = uiState.blockSummary, facetKeyLabel = facetKeyLabel)
        } else {
            FacetBlockContent(args = args, displayHint = displayHint)
        }
        if (args.showInlineActions) {
            FacetBlockActionsOverlay(
                args =
                FacetBlockActionsOverlayArgs(
                    args = args,
                    isBlockHovered = uiState.isBlockHovered,
                    isActionRowHovered = uiState.isActionRowHovered,
                    showOverflowButton = uiState.showOverflowButton,
                    showQuickActions = uiState.showQuickActions,
                    interactionSource = uiState.blockActionsInteractionSource,
                ),
            )
        }
    }
}

@Composable
private fun FacetBlockContent(args: FacetBlockArgs, displayHint: FacetDisplayHint?) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(vertical = 2.dp)
            .testTag(DaybookEditorSemantics.facetBlock(facetKeyString(args.descriptor.facetKey)))
            .semantics {
                contentDescription =
                    if (args.descriptor.isPrimary) {
                        "Primary document block"
                    } else {
                        "Document block"
                    }
            },
    ) {
        FacetContentHost(
            FacetContentHostArgs(
                descriptor = args.descriptor,
                doc = args.doc,
                branchPath = args.branchPath,
                displayHint = displayHint,
                noteEditor =
                FacetNoteEditorProps(
                    draft = args.noteDraft,
                    editable = args.noteEditable,
                    notice = args.noteNotice,
                    onFocusChanged = args.onNoteFocusChanged,
                    onDraftChange = { nextValue ->
                        args.controller.setNoteDraft(args.descriptor.facetKey, nextValue)
                    },
                ),
                onUiError = args.onUiError,
            ),
        )
    }
}

@Composable
private fun BoxScope.FacetBlockActionsOverlay(args: FacetBlockActionsOverlayArgs) {
    val blockActions =
        FacetBlockActions(
            canShowMenu = args.args.canShowMenu,
            canMoveUp = args.args.canMoveUp,
            canMoveDown = args.args.canMoveDown,
            isCollapsed = args.args.isCollapsed,
            onAddBlockRequested = {
                args.args.onAddBlockRequested()
            },
            onMakePrimary = { args.args.controller.makeFacetPrimary(args.args.descriptor.facetKey) },
            onMoveUp = { args.args.controller.moveFacetEarlier(args.args.descriptor.facetKey) },
            onMoveDown = { args.args.controller.moveFacetLater(args.args.descriptor.facetKey) },
            onToggleCollapse = args.args.onToggleCollapse,
        )
    FacetBlockActionsMenu(
        args =
        FacetBlockActionsMenuArgs(
            facetKeyLabel = facetKeyString(args.args.descriptor.facetKey),
            isPrimary = args.args.descriptor.isPrimary,
            actions = blockActions,
            isMenuOpen = args.args.isActionMenuOpen,
            showOverflowButton = args.showOverflowButton,
            onMenuOpenChange = args.args.onActionMenuOpenChange,
            showQuickActions = args.showQuickActions,
            collapseButtonIcon =
            if (args.args.isCollapsed) {
                Icons.Default.KeyboardArrowDown
            } else {
                Icons.Default.KeyboardArrowUp
            },
            overflowButtonEmphasized = args.isBlockHovered || args.args.isActionMenuOpen || args.isActionRowHovered,
            enableInvisibleHoverTarget = false,
            interactionSource = args.interactionSource,
        ),
        modifier = Modifier.align(Alignment.TopEnd).padding(top = 2.dp),
    )
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
private fun FacetBlockActionsMenu(args: FacetBlockActionsMenuArgs, modifier: Modifier = Modifier) {
    if (!args.actions.canShowMenu) {
        return
    }
    val hasVisibleControls = args.showQuickActions || args.showOverflowButton || args.isMenuOpen
    if (!hasVisibleControls && !args.enableInvisibleHoverTarget) {
        return
    }

    val rowBackgroundColor =
        if (args.showQuickActions) {
            MaterialTheme.colorScheme.surfaceContainer
        } else {
            MaterialTheme.colorScheme.surfaceContainer.copy(alpha = 0.48f)
        }

    Box(modifier = modifier) {
        if (hasVisibleControls) {
            FacetBlockActionsRow(
                args = args,
                rowBackgroundColor = rowBackgroundColor,
            )
        } else {
            FacetBlockInvisibleHoverTarget(args.interactionSource)
        }
        FacetBlockActionsDropdown(args = args)
    }
}

@Composable
private fun FacetBlockInvisibleHoverTarget(interactionSource: MutableInteractionSource) {
    Box(
        modifier =
        Modifier
            .size(44.dp)
            .hoverable(interactionSource),
    )
}

@Composable
private fun FacetBlockActionsRow(
    args: FacetBlockActionsMenuArgs,
    rowBackgroundColor: androidx.compose.ui.graphics.Color,
) {
    Row(
        modifier =
        Modifier
            .hoverable(args.interactionSource)
            .background(
                color = rowBackgroundColor,
                shape = RoundedCornerShape(percent = 50),
            )
            .padding(4.dp),
        horizontalArrangement = Arrangement.spacedBy(2.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        if (args.showQuickActions) {
            FacetBlockActionButton(
                args =
                FacetBlockActionButtonArgs(
                    imageVector = Icons.Default.Add,
                    tooltipText = "Add block below",
                    contentDescription = "Add block below",
                    testTag = DaybookEditorSemantics.addBlockAfterQuickAction(args.facetKeyLabel),
                    onClick = {
                        args.onMenuOpenChange(false)
                        args.actions.onAddBlockRequested()
                    },
                    iconAlpha = 1f,
                ),
            )
        }
        if (args.showQuickActions) {
            FacetBlockActionButton(
                args =
                FacetBlockActionButtonArgs(
                    imageVector = args.collapseButtonIcon,
                    tooltipText = if (args.actions.isCollapsed) "Expand block" else "Collapse block",
                    contentDescription = if (args.actions.isCollapsed) "Expand block" else "Collapse block",
                    testTag = DaybookEditorSemantics.toggleBlockCollapseQuickAction(args.facetKeyLabel),
                    onClick = args.actions.onToggleCollapse,
                    iconAlpha = 1f,
                ),
            )
        }
        if (args.showOverflowButton || args.isMenuOpen) {
            FacetBlockActionButton(
                args =
                FacetBlockActionButtonArgs(
                    imageVector = Icons.Default.MoreVert,
                    contentDescription = "Block actions",
                    testTag = DaybookEditorSemantics.blockActions(args.facetKeyLabel),
                    onClick = { args.onMenuOpenChange(true) },
                    iconAlpha = if (args.overflowButtonEmphasized) 1f else 0.48f,
                ),
            )
        }
    }
}

@Composable
private fun FacetBlockActionsDropdown(args: FacetBlockActionsMenuArgs) {
    DropdownMenu(
        expanded = args.isMenuOpen,
        onDismissRequest = { args.onMenuOpenChange(false) },
    ) {
        DropdownMenuItem(
            text = {
                Text(if (args.actions.isCollapsed) "Expand block" else "Collapse block")
            },
            modifier = Modifier.testTag(DaybookEditorSemantics.toggleBlockCollapseAction(args.facetKeyLabel)),
            onClick = {
                args.onMenuOpenChange(false)
                args.actions.onToggleCollapse()
            },
        )
        DropdownMenuItem(
            text = { Text(if (args.isPrimary) "Primary block" else "Make primary") },
            enabled = !args.isPrimary,
            modifier = Modifier.testTag(DaybookEditorSemantics.makePrimaryAction(args.facetKeyLabel)),
            onClick = {
                args.onMenuOpenChange(false)
                args.actions.onMakePrimary()
            },
        )
        DropdownMenuItem(
            text = { Text("Move up") },
            enabled = args.actions.canMoveUp,
            modifier = Modifier.testTag(DaybookEditorSemantics.moveUpAction(args.facetKeyLabel)),
            onClick = {
                args.onMenuOpenChange(false)
                args.actions.onMoveUp()
            },
        )
        DropdownMenuItem(
            text = { Text("Move down") },
            enabled = args.actions.canMoveDown,
            modifier = Modifier.testTag(DaybookEditorSemantics.moveDownAction(args.facetKeyLabel)),
            onClick = {
                args.onMenuOpenChange(false)
                args.actions.onMoveDown()
            },
        )
        DropdownMenuItem(
            text = { Text("Add block below") },
            modifier = Modifier.testTag(DaybookEditorSemantics.addBlockAfterAction(args.facetKeyLabel)),
            onClick = {
                args.onMenuOpenChange(false)
                args.actions.onAddBlockRequested()
            },
        )
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun FacetBlockActionButton(args: FacetBlockActionButtonArgs) {
    val buttonContent: @Composable () -> Unit = {
        IconButton(
            onClick = args.onClick,
            modifier = Modifier.size(36.dp).testTag(args.testTag),
        ) {
            Icon(
                imageVector = args.imageVector,
                contentDescription = args.contentDescription,
                tint = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = args.iconAlpha),
            )
        }
    }

    if (args.tooltipText != null) {
        TooltipBox(
            positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
            tooltip = {
                PlainTooltip {
                    Text(args.tooltipText)
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
private fun EditorBottomOverlayLane(args: EditorBottomOverlayLaneArgs) {
    Box(modifier = Modifier.fillMaxSize()) {
        val imeVisible = WindowInsets.ime.getBottom(LocalDensity.current) > 0
        val activeOverlay = focusedNoteAccessoryBarOverlay(
            narrowScreen = args.narrowScreen,
            imeVisible = imeVisible,
            contentFacetViews = args.contentFacetViews,
            focusedNoteFacetLabel = args.focusedNoteFacetLabel,
            isAddBlockPickerOpen = args.isAddBlockPickerOpen,
        )

        Box(modifier = Modifier.fillMaxSize()) {
            when (activeOverlay) {
                is EditorBottomOverlay.FocusedNoteAccessoryBar -> {
                    FocusedNoteAccessoryBar(
                        facetKey = activeOverlay.facetKey,
                        modifier = Modifier.align(Alignment.BottomCenter),
                        onAddBlockRequested = { args.onAddBlockRequested(activeOverlay.facetKey) },
                    )
                }

                EditorBottomOverlay.None -> Unit
            }

            SnackbarHost(
                hostState = args.snackbarHostState,
                modifier =
                Modifier
                    .align(Alignment.BottomCenter)
                    .padding(
                        bottom =
                        when (activeOverlay) {
                            is EditorBottomOverlay.FocusedNoteAccessoryBar -> 72.dp
                            EditorBottomOverlay.None -> 8.dp
                        },
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
            .testTag(DaybookEditorSemantics.FOCUSED_NOTE_ACCESSORY_BAR),
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

private fun focusedNoteAccessoryBarOverlay(
    narrowScreen: Boolean,
    imeVisible: Boolean,
    contentFacetViews: List<FacetViewDescriptor>,
    focusedNoteFacetLabel: String?,
    isAddBlockPickerOpen: Boolean,
): EditorBottomOverlay {
    val shouldShowFocusedNoteAccessoryBar =
        narrowScreen &&
            imeVisible &&
            focusedNoteFacetLabel != null &&
            !isAddBlockPickerOpen
    if (!shouldShowFocusedNoteAccessoryBar) {
        return EditorBottomOverlay.None
    }
    val activeFocusedNoteFacetKey =
        contentFacetViews.firstOrNull { descriptor ->
            facetKeyString(descriptor.facetKey) == focusedNoteFacetLabel
        }?.facetKey
    return if (activeFocusedNoteFacetKey != null) {
        EditorBottomOverlay.FocusedNoteAccessoryBar(activeFocusedNoteFacetKey)
    } else {
        EditorBottomOverlay.None
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
            .testTag(DaybookEditorSemantics.ADD_BLOCK_DIALOG),
    ) {
        AddBlockPickerHeader()
        TextField(
            value = searchQuery,
            onValueChange = onSearchQueryChange,
            placeholder = { Text("Search block types") },
            modifier =
            Modifier
                .fillMaxWidth()
                .padding(top = 16.dp)
                .focusRequester(searchFocusRequester)
                .testTag(DaybookEditorSemantics.ADD_BLOCK_SEARCH_FIELD)
                .semantics {
                    contentDescription = "Search block types"
                },
            singleLine = true,
        )
        AddBlockPickerOptions(
            visibleOptions = visibleOptions,
            onOptionSelected = onOptionSelected,
        )
    }
}

@Composable
private fun AddBlockPickerHeader() {
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
}

@Composable
private fun AddBlockPickerOptions(visibleOptions: List<AddBlockOptionSpec>, onOptionSelected: (String) -> Unit) {
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

internal data class FacetNoteEditorProps(
    val draft: String?,
    val editable: Boolean,
    val notice: String?,
    val onFocusChanged: (Boolean) -> Unit = {},
    val onDraftChange: (String) -> Unit,
)

@Composable
fun DocFacetSidebar(controller: EditorSessionController, modifier: Modifier = Modifier) {
    val state by controller.state.collectAsState()
    Column(
        modifier =
        modifier
            .fillMaxSize()
            .padding(8.dp)
            .testTag(DaybookEditorSemantics.DETAILS),
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
