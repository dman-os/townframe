@file:Suppress("FunctionNaming")

@file:OptIn(
    androidx.compose.material3.ExperimentalMaterial3Api::class,
    kotlin.time.ExperimentalTime::class,
)

package org.example.daybook.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.awaitEachGesture
import androidx.compose.foundation.gestures.awaitFirstDown
import androidx.compose.foundation.horizontalScroll
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
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.ArrowDownward
import androidx.compose.material.icons.filled.ArrowUpward
import androidx.compose.material.icons.filled.CheckCircle
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material.icons.filled.Star
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.MediumTopAppBar
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
import androidx.compose.material3.TopAppBarScrollBehavior
import androidx.compose.material3.rememberTooltipState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.input.pointer.AwaitPointerEventScope
import androidx.compose.ui.input.pointer.PointerEventPass
import androidx.compose.ui.input.pointer.PointerEventTimeoutCancellationException
import androidx.compose.ui.input.pointer.changedToUp
import androidx.compose.ui.input.pointer.isOutOfBounds
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.onClick
import androidx.compose.ui.semantics.selected
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
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

private const val DOC_EDITOR_TITLE_COLLAPSED_FRACTION_THRESHOLD = 0.5f

private enum class BlockActionSurface {
    Quick,
    Menu,
    SelectionBar,
}

private data class BlockActionSpec(
    val id: String,
    val label: String,
    val icon: ImageVector,
    val contentDescription: String,
    val testTags: Map<BlockActionSurface, String>,
    val surfaces: Set<BlockActionSurface>,
    val enabled: Boolean = true,
    val iconAlpha: Float = 1f,
    val onClick: () -> Unit,
) {
    fun testTag(surface: BlockActionSurface): String = testTags.getValue(surface)

    fun isVisibleOn(surface: BlockActionSurface): Boolean = surface in surfaces
}

private data class BlockSelectionContext(
    val selectedDescriptors: List<FacetViewDescriptor>,
    val collapsedFacetStates: MutableMap<String, Boolean>,
)

private data class BlockActionContext(
    val facetKeyLabel: String,
    val isPrimary: Boolean,
    val isCollapsed: Boolean,
    val isSelected: Boolean,
    val canMoveUp: Boolean,
    val canMoveDown: Boolean,
    val showDetails: Boolean,
    val canShowMenu: Boolean,
    val selectionContext: BlockSelectionContext? = null,
    val onSelectBlock: () -> Unit,
    val onAddBlockRequested: () -> Unit,
    val onToggleCollapse: () -> Unit,
    val onMakePrimary: () -> Unit,
    val onMoveUp: () -> Unit,
    val onMoveDown: () -> Unit,
    val onShowDetails: () -> Unit,
)

private data class BlockActionBuildArgs(
    val id: String,
    val label: String,
    val icon: ImageVector,
    val contentDescription: String,
    val quickTag: String? = null,
    val menuTag: String? = null,
    val selectionBarTag: String? = null,
    val surfaces: Set<BlockActionSurface>,
    val enabled: Boolean = true,
    val iconAlpha: Float = 1f,
    val onClick: () -> Unit,
)

private data class BlockHandleArgs(
    val facetKeyLabel: String,
    val isMenuOpen: Boolean,
    val showOverflowButton: Boolean,
    val onMenuOpenChange: (Boolean) -> Unit,
    val showQuickActions: Boolean,
    val overflowButtonEmphasized: Boolean,
    val enableInvisibleHoverTarget: Boolean,
    val interactionSource: MutableInteractionSource,
    val actions: List<BlockActionSpec>,
)

private fun buildBlockActions(context: BlockActionContext): List<BlockActionSpec> {
    if (!context.canShowMenu) {
        return emptyList()
    }
    val selectionContext = context.selectionContext
    return if (selectionContext != null && selectionContext.selectedDescriptors.size > 1) {
        buildSelectionBlockActions(selectionContext)
    } else {
        buildSingleBlockActions(context)
    }
}

private fun List<BlockActionSpec>.forSurface(surface: BlockActionSurface): List<BlockActionSpec> =
    filter { it.isVisibleOn(surface) }

private fun buildSingleBlockActions(context: BlockActionContext): List<BlockActionSpec> =
    buildBlockQuickActions(context) + buildBlockMenuActions(context) + buildBlockDetailsActions(context)

private fun buildBlockQuickActions(context: BlockActionContext): List<BlockActionSpec> = listOf(
    blockAction(
        BlockActionBuildArgs(
            id = "select",
            label = "Select block",
            icon = Icons.Default.CheckCircle,
            contentDescription = if (context.isSelected) "Block selected" else "Select block",
            quickTag = DaybookEditorSemantics.selectBlockQuickAction(context.facetKeyLabel),
            surfaces = setOf(BlockActionSurface.Quick),
            iconAlpha = if (context.isSelected) 1f else 0.72f,
            onClick = context.onSelectBlock,
        ),
    ),
    blockAction(
        BlockActionBuildArgs(
            id = "add-below",
            label = "Add block below",
            icon = Icons.Default.Add,
            contentDescription = "Add block below",
            quickTag = DaybookEditorSemantics.addBlockAfterQuickAction(context.facetKeyLabel),
            menuTag = DaybookEditorSemantics.addBlockAfterAction(context.facetKeyLabel),
            selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("add-below"),
            surfaces = setOf(BlockActionSurface.Quick, BlockActionSurface.Menu, BlockActionSurface.SelectionBar),
            onClick = context.onAddBlockRequested,
        ),
    ),
    blockAction(
        BlockActionBuildArgs(
            id = "toggle-collapse",
            label = if (context.isCollapsed) "Expand block" else "Collapse block",
            icon = if (context.isCollapsed) Icons.Default.KeyboardArrowUp else Icons.Default.KeyboardArrowDown,
            contentDescription = if (context.isCollapsed) "Expand block" else "Collapse block",
            quickTag = DaybookEditorSemantics.toggleBlockCollapseQuickAction(context.facetKeyLabel),
            menuTag = DaybookEditorSemantics.toggleBlockCollapseAction(context.facetKeyLabel),
            selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("toggle-collapse"),
            surfaces = setOf(BlockActionSurface.Quick, BlockActionSurface.Menu, BlockActionSurface.SelectionBar),
            onClick = context.onToggleCollapse,
        ),
    ),
)

private fun buildBlockMenuActions(context: BlockActionContext): List<BlockActionSpec> = listOf(
    blockAction(
        BlockActionBuildArgs(
            id = "make-primary",
            label = if (context.isPrimary) "Primary block" else "Make primary",
            icon = Icons.Default.Star,
            contentDescription = if (context.isPrimary) "Primary block" else "Make primary",
            menuTag = DaybookEditorSemantics.makePrimaryAction(context.facetKeyLabel),
            selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("make-primary"),
            surfaces = setOf(BlockActionSurface.Menu, BlockActionSurface.SelectionBar),
            enabled = !context.isPrimary,
            onClick = context.onMakePrimary,
        ),
    ),
    blockAction(
        BlockActionBuildArgs(
            id = "move-up",
            label = "Move up",
            icon = Icons.Default.ArrowUpward,
            contentDescription = "Move block up",
            menuTag = DaybookEditorSemantics.moveUpAction(context.facetKeyLabel),
            selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("move-up"),
            surfaces = setOf(BlockActionSurface.Menu, BlockActionSurface.SelectionBar),
            enabled = context.canMoveUp,
            onClick = context.onMoveUp,
        ),
    ),
    blockAction(
        BlockActionBuildArgs(
            id = "move-down",
            label = "Move down",
            icon = Icons.Default.ArrowDownward,
            contentDescription = "Move block down",
            menuTag = DaybookEditorSemantics.moveDownAction(context.facetKeyLabel),
            selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("move-down"),
            surfaces = setOf(BlockActionSurface.Menu, BlockActionSurface.SelectionBar),
            enabled = context.canMoveDown,
            onClick = context.onMoveDown,
        ),
    ),
)

private fun buildBlockDetailsActions(context: BlockActionContext): List<BlockActionSpec> = if (context.showDetails) {
    listOf(
        blockAction(
            BlockActionBuildArgs(
                id = "details",
                label = "Details",
                icon = Icons.Default.Info,
                contentDescription = "Show details",
                selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("details"),
                surfaces = setOf(BlockActionSurface.SelectionBar),
                onClick = context.onShowDetails,
            ),
        ),
    )
} else {
    emptyList()
}

private fun buildSelectionBlockActions(context: BlockSelectionContext): List<BlockActionSpec> {
    val selectedFacetLabels = context.selectedDescriptors.map { facetKeyString(it.facetKey) }
    val shouldExpand = context.selectedDescriptors.areAllCollapsed(context.collapsedFacetStates)
    return listOf(
        blockAction(
            BlockActionBuildArgs(
                id = "collapse-selected",
                label = if (shouldExpand) "Expand selected" else "Collapse selected",
                icon = if (shouldExpand) Icons.Default.KeyboardArrowUp else Icons.Default.KeyboardArrowDown,
                contentDescription = "Collapse or expand selected blocks",
                selectionBarTag = DaybookEditorSemantics.selectionActionBarAction("collapse-selected"),
                surfaces = setOf(BlockActionSurface.SelectionBar),
                onClick = {
                    selectedFacetLabels.forEach { facetKeyLabel ->
                        context.collapsedFacetStates[facetKeyLabel] = !shouldExpand
                    }
                },
            ),
        ),
    )
}

private fun blockAction(args: BlockActionBuildArgs): BlockActionSpec = BlockActionSpec(
    id = args.id,
    label = args.label,
    icon = args.icon,
    contentDescription = args.contentDescription,
    testTags =
    buildMap {
        args.quickTag?.let { put(BlockActionSurface.Quick, it) }
        args.menuTag?.let { put(BlockActionSurface.Menu, it) }
        args.selectionBarTag?.let { put(BlockActionSurface.SelectionBar, it) }
    },
    surfaces = args.surfaces,
    enabled = args.enabled,
    iconAlpha = args.iconAlpha,
    onClick = args.onClick,
)

private fun buildBlockCustomActions(
    canShowMenu: Boolean,
    actions: List<BlockActionSpec>,
    onOpenMenu: () -> Unit,
): List<CustomAccessibilityAction> {
    if (!canShowMenu) {
        return emptyList()
    }
    return buildList {
        actions.forEach { action ->
            if (action.isVisibleOn(BlockActionSurface.Quick)) {
                add(
                    CustomAccessibilityAction(action.contentDescription) {
                        action.onClick()
                        true
                    },
                )
            }
        }
        add(
            CustomAccessibilityAction("Block actions") {
                onOpenMenu()
                true
            },
        )
    }
}

internal class DocEditorSelectionState {
    private val selectedFacetLabelsState = mutableStateOf<Set<String>>(emptySet())

    val selectedFacetLabels: Set<String>
        get() = selectedFacetLabelsState.value

    val isSelectionMode: Boolean
        get() = selectedFacetLabels.isNotEmpty()

    val selectedCount: Int
        get() = selectedFacetLabels.size

    fun isSelected(facetKeyLabel: String): Boolean = facetKeyLabel in selectedFacetLabels

    fun select(facetKeyLabel: String) {
        selectedFacetLabelsState.value = selectedFacetLabels + facetKeyLabel
    }

    fun toggle(facetKeyLabel: String) {
        selectedFacetLabelsState.value =
            if (isSelected(facetKeyLabel)) {
                selectedFacetLabels - facetKeyLabel
            } else {
                selectedFacetLabels + facetKeyLabel
            }
    }

    fun clear() {
        selectedFacetLabelsState.value = emptySet()
    }

    fun selectAll(facetKeyLabels: Collection<String>) {
        selectedFacetLabelsState.value = facetKeyLabels.toSet()
    }
}

@Composable
internal fun rememberDocEditorSelectionState(docId: String?): DocEditorSelectionState = remember(docId) {
    DocEditorSelectionState()
}

internal data class DocEditorArgs(
    val controller: EditorSessionController,
    val selectionState: DocEditorSelectionState,
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
    var hoveredFacetLabel by remember(state.docId) { mutableStateOf<String?>(null) }
    var uiMessage by remember { mutableStateOf<String?>(null) }
    val onFacetHoverChanged: (String, Boolean) -> Unit = { facetKeyLabel, isHovered ->
        hoveredFacetLabel = updateHoveredFacetLabel(hoveredFacetLabel, facetKeyLabel, isHovered)
    }

    DocEditorSnackbarEffects(
        snackbarHostState = snackbarHostState,
        saveError = state.saveError,
        uiMessage = uiMessage,
        onUiMessageConsumed = { uiMessage = null },
    )

    val listState = rememberLazyListState()
    val facetListStartIndex = rememberDocEditorFacetListStartIndex(state, args.displayHintsError)
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
        selectionState = args.selectionState,
        showInlineFacetRack = args.showInlineFacetRack,
        displayHints = args.displayHints,
        displayHintsError = args.displayHintsError,
        isAddBlockPickerOpen = args.isAddBlockPickerOpen,
        onAddBlockRequested = args.onAddBlockRequested,
        snackbarHostState = snackbarHostState,
        collapsedFacetStates = collapsedFacetStates,
        blockActionExpandedStates = blockActionExpandedStates,
        focusedNoteFacetLabel = focusedNoteFacetLabel,
        hoveredFacetLabel = hoveredFacetLabel,
        onFocusedNoteFacetChanged = onFocusedNoteFacetChanged,
        onFacetHoverChanged = onFacetHoverChanged,
        onUiError = { message -> uiMessage = message },
        listState = listState,
        stickyFacetActionsHeightPx = stickyFacetActionsHeightPx,
        onStickyFacetActionsHeightChanged = { stickyFacetActionsHeightPx = it },
    )
}

private fun updateHoveredFacetLabel(
    currentFacetKeyLabel: String?,
    changedFacetKeyLabel: String,
    isHovered: Boolean,
): String? = if (isHovered) {
    changedFacetKeyLabel
} else {
    currentFacetKeyLabel?.takeUnless { it == changedFacetKeyLabel }
}

@Composable
private fun rememberDocEditorFacetListStartIndex(state: EditorSessionState, displayHintsError: String?): Int =
    remember(state.titleNotice, displayHintsError, state.contentFacetViews.isNotEmpty()) {
        docEditorFacetListStartIndex(
            titleNotice = state.titleNotice,
            displayHintsError = displayHintsError,
            hasFacetRows = state.contentFacetViews.isNotEmpty(),
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
    val selectionState: DocEditorSelectionState,
    val showInlineFacetRack: Boolean,
    val displayHints: Map<String, FacetDisplayHint>,
    val displayHintsError: String?,
    val isAddBlockPickerOpen: Boolean,
    val onAddBlockRequested: (FacetKey) -> Unit,
    val snackbarHostState: SnackbarHostState,
    val collapsedFacetStates: MutableMap<String, Boolean>,
    val blockActionExpandedStates: MutableMap<String, Boolean>,
    val focusedNoteFacetLabel: String?,
    val hoveredFacetLabel: String?,
    val onFocusedNoteFacetChanged: (FacetKey, Boolean) -> Unit,
    val onFacetHoverChanged: (String, Boolean) -> Unit,
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
    BoxWithConstraints(
        modifier =
        modifier
            .fillMaxSize()
            .imePadding()
            .testTag(DaybookEditorSemantics.EDITOR),
    ) {
        val narrowScreen = maxWidth < 600.dp
        val facetListStartIndex = rememberDocEditorFacetListStartIndex(args.state, args.displayHintsError)
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
                controller = args.controller,
                state = args.state,
                selectionState = args.selectionState,
                collapsedFacetStates = args.collapsedFacetStates,
                facetListStartIndex = facetListStartIndex,
                listState = args.listState,
                showInlineFacetRack = args.showInlineFacetRack,
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
        docEditorTitleNoticeSection(args.state.titleNotice)
        // docEditorTitleDividerSection()
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

@Composable
internal fun DocEditorMediumTopAppBar(
    chrome: org.example.daybook.layouts.ScreenChromeSpec.TopBarSpec,
    controller: EditorSessionController?,
    scrollBehavior: TopAppBarScrollBehavior? = null,
) {
    val state = controller?.state?.collectAsState()?.value
    val collapsedFraction = scrollBehavior?.state?.collapsedFraction ?: 0f
    val titleTextStyle =
        if (collapsedFraction > DOC_EDITOR_TITLE_COLLAPSED_FRACTION_THRESHOLD) {
            MaterialTheme.typography.titleLarge.copy(fontWeight = FontWeight.Bold)
        } else {
            MaterialTheme.typography.headlineSmall.copy(fontWeight = FontWeight.Bold)
        }

    MediumTopAppBar(
        title = {
            DocEditorTopBarTitleContent(
                chrome = chrome,
                controllerState = state,
                titleTextStyle = titleTextStyle,
                onTitleChange = { value -> controller?.setTitleDraft(value) },
            )
        },
        navigationIcon = {
            check(!chrome.showBack || chrome.onBack != null) {
                "inconsistent top bar chrome: showBack=${chrome.showBack} onBack=${chrome.onBack}"
            }
            if (chrome.showBack) {
                val onBack =
                    chrome.onBack
                        ?: error(
                            "inconsistent top bar chrome: showBack=${chrome.showBack} onBack=${chrome.onBack}",
                        )
                IconButton(onClick = onBack) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            }
        },
        actions = {
            chrome.actions?.invoke(this)
        },
        scrollBehavior = scrollBehavior,
    )
}

@Composable
private fun DocEditorTopBarTitleContent(
    chrome: org.example.daybook.layouts.ScreenChromeSpec.TopBarSpec,
    controllerState: org.example.daybook.ui.editor.EditorSessionState?,
    titleTextStyle: androidx.compose.ui.text.TextStyle,
    onTitleChange: (String) -> Unit,
) {
    if (controllerState != null) {
        TextField(
            value = controllerState.titleDraft,
            onValueChange = onTitleChange,
            modifier =
            Modifier
                .fillMaxWidth()
                .testTag(DaybookEditorSemantics.TITLE_FIELD)
                .semantics {
                    contentDescription = "Document title"
                },
            enabled = controllerState.titleEditable,
            singleLine = true,
            placeholder = {
                Text(
                    "Untitled",
                    style = titleTextStyle,
                    color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.48f),
                )
            },
            textStyle = titleTextStyle,
            colors =
            TextFieldDefaults.colors(
                focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
            ),
        )
    } else {
        Text(
            text = chrome.title ?: "Document",
            style = titleTextStyle,
        )
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
        val showBlockHandle = !args.selectionState.isSelectionMode
        FacetBlock(
            args =
            FacetBlockArgs(
                descriptor = descriptor,
                doc = args.state.doc,
                branchPath = args.state.branchPath,
                controller = args.controller,
                canShowMenu = args.state.docId != null,
                showInlineFacetRack = args.showInlineFacetRack,
                selectionState = args.selectionState,
                isCollapsed = isCollapsed,
                isActionMenuOpen = isActionMenuOpen,
                onActionMenuOpenChange = { args.blockActionExpandedStates[facetKeyLabel] = it },
                showInlineActions = showBlockHandle && !isUsingStickyActions,
                onHoverChanged = { isHovered -> args.onFacetHoverChanged(facetKeyLabel, isHovered) },
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
    if (args.selectionState.isSelectionMode) {
        return
    }
    Box(modifier = Modifier.fillMaxSize()) {
        stickyFacetPlacement?.let { placement ->
            val descriptor = placement.descriptor
            val facetKeyLabel = facetKeyString(descriptor.facetKey)
            val isCollapsed = args.collapsedFacetStates[facetKeyLabel] == true
            val isActionMenuOpen = args.blockActionExpandedStates[facetKeyLabel] == true
            val isStickyFacetHovered = args.hoveredFacetLabel == facetKeyLabel
            val stickyActionsHoverSource = remember(descriptor.facetKey) { MutableInteractionSource() }
            val isStickyActionRowHovered by stickyActionsHoverSource.collectIsHoveredAsState()
            val showStickyOverflowButton = isActionMenuOpen || isStickyFacetHovered || isStickyActionRowHovered
            val showStickyQuickActions = isActionMenuOpen || isStickyActionRowHovered
            BlockHandle(
                args =
                BlockHandleArgs(
                    facetKeyLabel = facetKeyLabel,
                    actions =
                    buildBlockActions(
                        BlockActionContext(
                            facetKeyLabel = facetKeyLabel,
                            isPrimary = descriptor.isPrimary,
                            isCollapsed = isCollapsed,
                            isSelected = args.selectionState.isSelected(facetKeyLabel),
                            canMoveUp = placement.facetIndex > 0,
                            canMoveDown = placement.facetIndex < args.state.contentFacetViews.lastIndex,
                            showDetails = args.showInlineFacetRack,
                            canShowMenu = args.state.docId != null,
                            onSelectBlock = { args.selectionState.select(facetKeyLabel) },
                            onAddBlockRequested = { args.onAddBlockRequested(descriptor.facetKey) },
                            onToggleCollapse = {
                                args.collapsedFacetStates[facetKeyLabel] =
                                    !(args.collapsedFacetStates[facetKeyLabel] == true)
                            },
                            onMakePrimary = { args.controller.makeFacetPrimary(descriptor.facetKey) },
                            onMoveUp = { args.controller.moveFacetEarlier(descriptor.facetKey) },
                            onMoveDown = { args.controller.moveFacetLater(descriptor.facetKey) },
                            onShowDetails = {},
                        ),
                    ),
                    isMenuOpen = isActionMenuOpen,
                    showOverflowButton = showStickyOverflowButton,
                    onMenuOpenChange = { args.blockActionExpandedStates[facetKeyLabel] = it },
                    showQuickActions = showStickyQuickActions,
                    overflowButtonEmphasized = showStickyOverflowButton,
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
    if (titleNotice != null) {
        index += 1
    }
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
    val showInlineFacetRack: Boolean,
    val selectionState: DocEditorSelectionState,
    val isCollapsed: Boolean,
    val isActionMenuOpen: Boolean,
    val onActionMenuOpenChange: (Boolean) -> Unit,
    val showInlineActions: Boolean,
    val onHoverChanged: (Boolean) -> Unit,
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

private data class EditorBottomOverlayLaneArgs(
    val narrowScreen: Boolean,
    val controller: EditorSessionController,
    val state: EditorSessionState,
    val selectionState: DocEditorSelectionState,
    val collapsedFacetStates: MutableMap<String, Boolean>,
    val facetListStartIndex: Int,
    val listState: androidx.compose.foundation.lazy.LazyListState,
    val showInlineFacetRack: Boolean,
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
    val isSelected: Boolean,
    val blockActions: List<BlockActionSpec>,
    val customActions: List<CustomAccessibilityAction>,
)

private data class FacetBlockActionState(
    val blockActions: List<BlockActionSpec>,
    val customActions: List<CustomAccessibilityAction>,
)

private data class BlockHandleOverlayArgs(
    val args: FacetBlockArgs,
    val isBlockHovered: Boolean,
    val isActionRowHovered: Boolean,
    val showOverflowButton: Boolean,
    val showQuickActions: Boolean,
    val interactionSource: MutableInteractionSource,
    val actions: List<BlockActionSpec>,
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
    val facetKeyLabel = facetKeyString(args.descriptor.facetKey)
    val isSelected = args.selectionState.isSelected(facetKeyLabel)
    val actionState =
        buildFacetBlockActionState(
            args = args,
            facetKeyLabel = facetKeyLabel,
            isSelected = isSelected,
        )
    return FacetBlockUiState(
        blockSummary = blockSummary,
        interactionSource = interactionSource,
        blockActionsInteractionSource = blockActionsInteractionSource,
        isBlockHovered = isBlockHovered,
        isActionRowHovered = isActionRowHovered,
        showOverflowButton = showOverflowButton,
        showQuickActions = showQuickActions,
        isSelected = isSelected,
        blockActions = actionState.blockActions,
        customActions = actionState.customActions,
    )
}

private fun buildFacetBlockActionState(
    args: FacetBlockArgs,
    facetKeyLabel: String,
    isSelected: Boolean,
): FacetBlockActionState {
    val blockActions =
        buildBlockActions(
            BlockActionContext(
                facetKeyLabel = facetKeyLabel,
                isPrimary = args.descriptor.isPrimary,
                isCollapsed = args.isCollapsed,
                isSelected = isSelected,
                canMoveUp = args.canMoveUp,
                canMoveDown = args.canMoveDown,
                showDetails = args.showInlineFacetRack,
                canShowMenu = args.canShowMenu,
                onSelectBlock = { args.selectionState.select(facetKeyLabel) },
                onAddBlockRequested = args.onAddBlockRequested,
                onToggleCollapse = args.onToggleCollapse,
                onMakePrimary = { args.controller.makeFacetPrimary(args.descriptor.facetKey) },
                onMoveUp = { args.controller.moveFacetEarlier(args.descriptor.facetKey) },
                onMoveDown = { args.controller.moveFacetLater(args.descriptor.facetKey) },
                onShowDetails = { args.onActionMenuOpenChange(false) },
            ),
        )
    return FacetBlockActionState(
        blockActions = blockActions,
        customActions = buildBlockCustomActions(args.canShowMenu, blockActions) { args.onActionMenuOpenChange(true) },
    )
}

@Composable
private fun FacetBlock(args: FacetBlockArgs, modifier: Modifier = Modifier) {
    val displayHintKey = facetDisplayHintKey(args.descriptor.facetKey)
    val displayHint = args.displayHints[displayHintKey]
    val facetKeyLabel = facetKeyString(args.descriptor.facetKey)
    val uiState = rememberFacetBlockUiState(args = args, displayHint = displayHint)

    LaunchedEffect(uiState.isBlockHovered) {
        args.onHoverChanged(uiState.isBlockHovered)
    }

    val blockBackgroundColor = facetBlockBackgroundColor(uiState)

    Surface(
        modifier =
        modifier
            .fillMaxWidth()
            .hoverable(uiState.interactionSource)
            .blockSelectionEntryGesture(facetKeyLabel = facetKeyLabel, selectionState = args.selectionState)
            .semantics {
                this.customActions = uiState.customActions
                selected = uiState.isSelected
                contentDescription = facetBlockContentDescription(args, uiState)
            }
            .testTag(DaybookEditorSemantics.facetRow(facetKeyLabel)),
        shape = RoundedCornerShape(18.dp),
        color = blockBackgroundColor,
        contentColor = facetBlockContentColor(uiState),
    ) {
        Box(modifier = Modifier.fillMaxWidth()) {
            if (args.isCollapsed) {
                FacetBlockCollapsedSummary(
                    summary = uiState.blockSummary,
                    facetKeyLabel = facetKeyLabel,
                    contentColor = facetBlockCollapsedContentColor(uiState),
                )
            } else {
                FacetBlockContent(args = args, displayHint = displayHint)
            }
            if (args.showInlineActions) {
                BlockHandleOverlay(
                    args =
                    BlockHandleOverlayArgs(
                        args = args,
                        isBlockHovered = uiState.isBlockHovered,
                        isActionRowHovered = uiState.isActionRowHovered,
                        showOverflowButton = uiState.showOverflowButton,
                        showQuickActions = uiState.showQuickActions,
                        interactionSource = uiState.blockActionsInteractionSource,
                        actions = uiState.blockActions,
                    ),
                )
            }
            if (args.selectionState.isSelectionMode) {
                Box(
                    modifier =
                    Modifier
                        .matchParentSize()
                        .clickable(onClick = { args.selectionState.toggle(facetKeyLabel) }),
                )
            }
        }
    }
}

@Composable
private fun facetBlockBackgroundColor(uiState: FacetBlockUiState): androidx.compose.ui.graphics.Color =
    if (uiState.isSelected) {
        MaterialTheme.colorScheme.primaryContainer
    } else {
        androidx.compose.ui.graphics.Color.Transparent
    }

@Composable
private fun facetBlockContentColor(uiState: FacetBlockUiState): androidx.compose.ui.graphics.Color =
    if (uiState.isSelected) {
        MaterialTheme.colorScheme.onPrimaryContainer
    } else {
        MaterialTheme.colorScheme.onSurface
    }

@Composable
private fun facetBlockCollapsedContentColor(uiState: FacetBlockUiState): androidx.compose.ui.graphics.Color =
    if (uiState.isSelected) {
        MaterialTheme.colorScheme.onPrimaryContainer
    } else {
        MaterialTheme.colorScheme.onSurfaceVariant
    }

private fun facetBlockContentDescription(args: FacetBlockArgs, uiState: FacetBlockUiState): String =
    if (args.isCollapsed) {
        uiState.blockSummary.contentDescription
    } else if (args.descriptor.isPrimary) {
        "Primary document block"
    } else {
        "Document block"
    }

private fun Modifier.blockSelectionEntryGesture(
    facetKeyLabel: String,
    selectionState: DocEditorSelectionState,
): Modifier = pointerInput(facetKeyLabel) {
    awaitEachGesture {
        awaitFirstDown(requireUnconsumed = false, pass = PointerEventPass.Initial)
        if (waitForBlockSelectionLongPress()) {
            selectionState.select(facetKeyLabel)
        }
    }
}

private suspend fun AwaitPointerEventScope.waitForBlockSelectionLongPress(): Boolean {
    var canceled = false
    return try {
        withTimeout(viewConfiguration.longPressTimeoutMillis) {
            while (true) {
                val event = awaitPointerEvent(PointerEventPass.Initial)
                if (event.changes.all { it.changedToUp() }) {
                    canceled = true
                    return@withTimeout
                }
                if (event.changes.any { it.isConsumed || it.isOutOfBounds(size, extendedTouchPadding) }) {
                    canceled = true
                    return@withTimeout
                }

                val consumeCheck = awaitPointerEvent(PointerEventPass.Final)
                if (consumeCheck.changes.any { it.isConsumed }) {
                    canceled = true
                    return@withTimeout
                }
            }
        }
        !canceled
    } catch (_: PointerEventTimeoutCancellationException) {
        true
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
                    readOnly = args.selectionState.isSelectionMode,
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
private fun BoxScope.BlockHandleOverlay(args: BlockHandleOverlayArgs) {
    BlockHandle(
        args =
        BlockHandleArgs(
            facetKeyLabel = facetKeyString(args.args.descriptor.facetKey),
            isMenuOpen = args.args.isActionMenuOpen,
            showOverflowButton = args.showOverflowButton,
            onMenuOpenChange = args.args.onActionMenuOpenChange,
            showQuickActions = args.showQuickActions,
            overflowButtonEmphasized = args.isBlockHovered || args.args.isActionMenuOpen || args.isActionRowHovered,
            enableInvisibleHoverTarget = false,
            interactionSource = args.interactionSource,
            actions = args.actions,
        ),
        modifier = Modifier.align(Alignment.TopEnd).padding(top = 2.dp),
    )
}

@Composable
private fun BlockHandle(args: BlockHandleArgs, modifier: Modifier = Modifier) {
    if (args.actions.isEmpty()) {
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
            MaterialTheme.colorScheme.surfaceContainer
        }

    Box(modifier = modifier) {
        if (hasVisibleControls) {
            BlockHandleActionRow(args = args, rowBackgroundColor = rowBackgroundColor)
        } else {
            BlockHandleInvisibleHoverTarget(args.interactionSource)
        }
        BlockHandleDropdown(args = args)
    }
}

@Composable
private fun BlockHandleInvisibleHoverTarget(interactionSource: MutableInteractionSource) {
    Box(
        modifier =
        Modifier
            .size(44.dp)
            .hoverable(interactionSource),
    )
}

@Composable
private fun BlockHandleActionRow(args: BlockHandleArgs, rowBackgroundColor: androidx.compose.ui.graphics.Color) {
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
            args.actions.forSurface(BlockActionSurface.Quick).forEach { action ->
                BlockHandleActionButton(
                    action = action,
                    testTag = action.testTag(BlockActionSurface.Quick),
                )
            }
        }
        BlockHandleOverflowActionButton(args)
    }
}

@Composable
private fun BlockHandleOverflowActionButton(args: BlockHandleArgs) {
    if (!args.showOverflowButton && !args.isMenuOpen) {
        return
    }
    BlockHandleActionButton(
        action =
        BlockActionSpec(
            id = "open-menu",
            label = "Block actions",
            icon = Icons.Default.MoreVert,
            contentDescription = "Block actions",
            testTags = mapOf(BlockActionSurface.Quick to DaybookEditorSemantics.blockActions(args.facetKeyLabel)),
            surfaces = setOf(BlockActionSurface.Quick),
            iconAlpha = if (args.overflowButtonEmphasized) 1f else 0.48f,
            onClick = { args.onMenuOpenChange(true) },
        ),
        testTag = DaybookEditorSemantics.blockActions(args.facetKeyLabel),
    )
}

@Composable
private fun BlockHandleDropdown(args: BlockHandleArgs) {
    DropdownMenu(
        expanded = args.isMenuOpen,
        onDismissRequest = { args.onMenuOpenChange(false) },
    ) {
        args.actions.forSurface(BlockActionSurface.Menu).forEach { action ->
            DropdownMenuItem(
                text = { Text(action.label) },
                enabled = action.enabled,
                modifier = Modifier.testTag(action.testTag(BlockActionSurface.Menu)),
                onClick = {
                    args.onMenuOpenChange(false)
                    action.onClick()
                },
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun BlockHandleActionButton(action: BlockActionSpec, testTag: String? = null) {
    val buttonContent: @Composable () -> Unit = {
        IconButton(
            onClick = action.onClick,
            enabled = action.enabled,
            modifier = Modifier.size(36.dp).testTag(testTag ?: action.testTag(BlockActionSurface.Quick)),
        ) {
            Icon(
                imageVector = action.icon,
                contentDescription = action.contentDescription,
                tint = LocalContentColor.current.copy(alpha = action.iconAlpha),
            )
        }
    }

    if (action.isVisibleOn(BlockActionSurface.Quick)) {
        TooltipBox(
            positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
            tooltip = {
                PlainTooltip {
                    Text(action.label)
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
private fun FacetBlockCollapsedSummary(
    summary: FacetBlockSummary,
    facetKeyLabel: String,
    contentColor: androidx.compose.ui.graphics.Color,
) {
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
            color = contentColor,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
        summary.preview?.let { preview ->
            Text(
                text = preview,
                style = MaterialTheme.typography.bodySmall,
                color = contentColor,
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

    data class BlockSelectionActionBar(val actions: List<BlockActionSpec>) : EditorBottomOverlay
}

@Composable
private fun EditorBottomOverlayLane(args: EditorBottomOverlayLaneArgs) {
    val scope = rememberCoroutineScope()
    Box(modifier = Modifier.fillMaxSize()) {
        val imeVisible = WindowInsets.ime.getBottom(LocalDensity.current) > 0
        val selectedFacetLabels = args.selectionState.selectedFacetLabels
        val activeOverlay =
            blockSelectionActionBarOverlay(
                selectedFacetLabels = selectedFacetLabels,
                args = args,
                scope = scope,
            ) ?: focusedNoteAccessoryBarOverlay(
                narrowScreen = args.narrowScreen,
                imeVisible = imeVisible,
                contentFacetViews = args.contentFacetViews,
                focusedNoteFacetLabel = args.focusedNoteFacetLabel,
                isAddBlockPickerOpen = args.isAddBlockPickerOpen,
            )

        Box(modifier = Modifier.fillMaxSize()) {
            when (activeOverlay) {
                is EditorBottomOverlay.BlockSelectionActionBar -> {
                    BlockSelectionActionBar(
                        actions = activeOverlay.actions,
                        modifier = Modifier.align(Alignment.BottomCenter),
                    )
                }

                is EditorBottomOverlay.FocusedNoteAccessoryBar -> {
                    FocusedNoteAccessoryBar(
                        facetKey = activeOverlay.facetKey,
                        modifier = Modifier.align(Alignment.BottomCenter),
                        onAddBlockRequested = { args.onAddBlockRequested(activeOverlay.facetKey) },
                        onSelectBlockRequested = {
                            args.selectionState.select(facetKeyString(activeOverlay.facetKey))
                        },
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
                            is EditorBottomOverlay.BlockSelectionActionBar -> 96.dp
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
    onSelectBlockRequested: () -> Unit = {},
) {
    val selectBlockActionTag =
        DaybookEditorSemantics.focusedNoteAccessorySelectBlockAction(facetKeyString(facetKey))
    val addBlockActionTag =
        DaybookEditorSemantics.focusedNoteAccessoryAddBlockAction(facetKeyString(facetKey))
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
                BlockHandleActionButton(
                    action =
                    BlockActionSpec(
                        id = "select-focused-note",
                        label = "Select block",
                        icon = Icons.Default.CheckCircle,
                        contentDescription = "Select block",
                        testTags = mapOf(BlockActionSurface.Quick to selectBlockActionTag),
                        surfaces = setOf(BlockActionSurface.Quick),
                        onClick = onSelectBlockRequested,
                    ),
                    testTag = selectBlockActionTag,
                )
                TextButton(
                    onClick = onAddBlockRequested,
                    modifier =
                    Modifier
                        .testTag(addBlockActionTag)
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

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun BlockSelectionActionBar(actions: List<BlockActionSpec>, modifier: Modifier = Modifier) {
    if (actions.isEmpty()) {
        return
    }
    Box(
        modifier =
        modifier
            .padding(horizontal = 12.dp, vertical = 8.dp)
            .testTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR),
    ) {
        Surface(
            tonalElevation = 1.dp,
            shadowElevation = 0.dp,
            shape = RoundedCornerShape(24.dp),
            color = MaterialTheme.colorScheme.surfaceContainerLow,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Column(
                modifier =
                Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp, vertical = 10.dp),
            ) {
                Row(
                    modifier =
                    Modifier
                        .fillMaxWidth()
                        .horizontalScroll(rememberScrollState())
                        .padding(vertical = 2.dp),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    actions.forEach { action ->
                        SelectionActionButton(action = action)
                    }
                }
            }
        }
    }
}

@Composable
private fun SelectionActionButton(action: BlockActionSpec) {
    FilledTonalButton(
        onClick = action.onClick,
        enabled = action.enabled,
        shape = RoundedCornerShape(16.dp),
        contentPadding = androidx.compose.foundation.layout.PaddingValues(horizontal = 12.dp, vertical = 8.dp),
        modifier =
        Modifier
            .testTag(action.testTag(BlockActionSurface.SelectionBar))
            .semantics {
                contentDescription = action.contentDescription
            },
    ) {
        Row(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Icon(
                imageVector = action.icon,
                contentDescription = null,
            )
            Text(text = action.label, style = MaterialTheme.typography.labelLarge)
        }
    }
}

private fun blockSelectionActionBarOverlay(
    selectedFacetLabels: Set<String>,
    args: EditorBottomOverlayLaneArgs,
    scope: kotlinx.coroutines.CoroutineScope,
): EditorBottomOverlay.BlockSelectionActionBar? = selectedFacetLabels
    .takeIf { it.isNotEmpty() }
    ?.let { selectedLabels ->
        args.contentFacetViews
            .filter { descriptor ->
                facetKeyString(descriptor.facetKey) in selectedLabels
            }
            .takeIf { it.isNotEmpty() }
            ?.let { selectedDescriptors ->
                EditorBottomOverlay.BlockSelectionActionBar(
                    actions = buildBlockSelectionActionBarActions(args, selectedDescriptors, scope),
                )
            }
    }

private fun buildBlockSelectionActionBarActions(
    args: EditorBottomOverlayLaneArgs,
    selectedDescriptors: List<FacetViewDescriptor>,
    scope: kotlinx.coroutines.CoroutineScope,
): List<BlockActionSpec> {
    val selectedDescriptor = selectedDescriptors.first()
    val selectedFacetKey = selectedDescriptor.facetKey
    val selectedFacetKeyLabel = facetKeyString(selectedFacetKey)
    val selectedIndex = args.contentFacetViews.indexOfFirst { it.facetKey == selectedFacetKey }
    val selectedIsCollapsed = args.collapsedFacetStates[selectedFacetKeyLabel] == true
    val nextCollapsed = !selectedIsCollapsed
    val selectionContext =
        if (selectedDescriptors.size > 1) {
            BlockSelectionContext(
                selectedDescriptors = selectedDescriptors,
                collapsedFacetStates = args.collapsedFacetStates,
            )
        } else {
            null
        }

    return buildBlockActions(
        BlockActionContext(
            facetKeyLabel = selectedFacetKeyLabel,
            isPrimary = selectedDescriptor.isPrimary,
            isCollapsed = selectedIsCollapsed,
            isSelected = true,
            canMoveUp = selectedIndex > 0,
            canMoveDown = selectedIndex >= 0 && selectedIndex < args.contentFacetViews.lastIndex,
            showDetails = args.showInlineFacetRack,
            canShowMenu = true,
            selectionContext = selectionContext,
            onSelectBlock = {},
            onAddBlockRequested = { args.onAddBlockRequested(selectedFacetKey) },
            onToggleCollapse = {
                args.collapsedFacetStates[selectedFacetKeyLabel] = nextCollapsed
            },
            onMakePrimary = { args.controller.makeFacetPrimary(selectedFacetKey) },
            onMoveUp = { args.controller.moveFacetEarlier(selectedFacetKey) },
            onMoveDown = { args.controller.moveFacetLater(selectedFacetKey) },
            onShowDetails = {
                scope.launch {
                    args.listState.animateScrollToItem(
                        docEditorDetailsItemIndex(
                            state = args.state,
                            facetListStartIndex = args.facetListStartIndex,
                        ),
                    )
                }
            },
        ),
    ).forSurface(BlockActionSurface.SelectionBar)
}

private fun List<FacetViewDescriptor>.areAllCollapsed(collapsedFacetStates: Map<String, Boolean>): Boolean =
    all { collapsedFacetStates[facetKeyString(it.facetKey)] == true }

private fun docEditorDetailsItemIndex(state: EditorSessionState, facetListStartIndex: Int): Int =
    if (state.contentFacetViews.isNotEmpty()) {
        facetListStartIndex + state.contentFacetViews.size
    } else {
        facetListStartIndex + 1
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
    val readOnly: Boolean = false,
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
