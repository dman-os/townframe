@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.relocation.BringIntoViewRequester
import androidx.compose.foundation.relocation.bringIntoViewRequester
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.hoverable
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Error
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
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.ui.editor.EditorSessionController
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

    Box(modifier = modifier.fillMaxSize().testTag(DaybookEditorSemantics.Editor)) {
        Column(
            modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState()),
        ) {
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
            state.titleNotice?.let { titleNotice ->
                Text(
                    text = titleNotice,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))

            displayHintsError?.let { message ->
                FacetStatusText(
                    text = "Facet display config unavailable: $message",
                    modifier = Modifier.padding(bottom = 8.dp),
                )
            }

            if (state.contentFacetViews.isEmpty()) {
                Text(
                    text = "No facets",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }

            state.contentFacetViews.forEachIndexed { index, descriptor ->
                val bringIntoViewRequester = remember(descriptor.facetKey) { BringIntoViewRequester() }
                LaunchedEffect(state.scrollToFacetRequest?.seq, descriptor.facetKey) {
                    val request = state.scrollToFacetRequest ?: return@LaunchedEffect
                    if (request.facetKey == descriptor.facetKey) {
                        bringIntoViewRequester.bringIntoView()
                    }
                }
                FacetBlock(
                    descriptor = descriptor,
                    doc = state.doc,
                    branchPath = state.branchPath,
                    controller = controller,
                    modifier = Modifier.bringIntoViewRequester(bringIntoViewRequester),
                    canShowMenu = state.docId != null,
                    noteDraft = state.noteEditors[descriptor.facetKey]?.draft,
                    noteEditable = state.noteEditors[descriptor.facetKey]?.editable ?: false,
                    noteNotice = state.noteEditors[descriptor.facetKey]?.notice,
                    displayHints = displayHints,
                    canMoveUp = index > 0,
                    canMoveDown = index < state.contentFacetViews.lastIndex,
                    onUiError = { message -> uiMessage = message },
                )
                if (index < state.contentFacetViews.lastIndex) {
                    Spacer(modifier = Modifier.height(4.dp))
                }
            }

            if (showInlineFacetRack) {
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

        SnackbarHost(
            hostState = snackbarHostState,
            modifier = Modifier.align(Alignment.BottomCenter).padding(8.dp),
        )
    }
}

@Composable
private fun FacetBlock(
    descriptor: FacetViewDescriptor,
    doc: Doc?,
    branchPath: String,
    controller: EditorSessionController,
    modifier: Modifier = Modifier,
    canShowMenu: Boolean,
    noteDraft: String?,
    noteEditable: Boolean,
    noteNotice: String?,
    displayHints: Map<String, FacetDisplayHint>,
    canMoveUp: Boolean,
    canMoveDown: Boolean,
    onUiError: (String) -> Unit,
) {
    val displayHintKey = facetDisplayHintKey(descriptor.facetKey)
    val displayHint = displayHints[displayHintKey]
    val facetKeyLabel = facetKeyString(descriptor.facetKey)
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    var actionsExpanded by remember { mutableStateOf(false) }
    val actionsVisible = canShowMenu && (isHovered || actionsExpanded)

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
                                actionsExpanded = true
                                true
                            },
                        )
                    } else {
                        emptyList<CustomAccessibilityAction>()
                    }
            }
            .testTag(DaybookEditorSemantics.facetRow(facetKeyLabel)),
    ) {
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
        FacetBlockActionsMenu(
            facetKeyLabel = facetKeyLabel,
            isPrimary = descriptor.isPrimary,
            actions =
            FacetBlockActions(
                canShowMenu = canShowMenu,
                canMoveUp = canMoveUp,
                canMoveDown = canMoveDown,
                onAddNote = { controller.addNoteFacetAfter(descriptor.facetKey) },
                onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
                onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
                onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
            ),
            expanded = actionsExpanded,
            visible = actionsVisible,
            onExpandedChange = { actionsExpanded = it },
            modifier = Modifier.align(Alignment.TopEnd).padding(top = 2.dp),
        )
    }
}

private data class FacetBlockActions(
    val canShowMenu: Boolean,
    val canMoveUp: Boolean,
    val canMoveDown: Boolean,
    val onAddNote: () -> Unit,
    val onMakePrimary: () -> Unit,
    val onMoveUp: () -> Unit,
    val onMoveDown: () -> Unit,
)

@Composable
private fun FacetBlockActionsMenu(
    facetKeyLabel: String,
    isPrimary: Boolean,
    actions: FacetBlockActions,
    expanded: Boolean,
    visible: Boolean,
    onExpandedChange: (Boolean) -> Unit,
    modifier: Modifier = Modifier,
) {
    if (!actions.canShowMenu) {
        return
    }

    Box(modifier = modifier) {
        if (visible || expanded) {
            IconButton(
                onClick = { onExpandedChange(true) },
                modifier =
                Modifier
                    .size(36.dp)
                    .testTag(DaybookEditorSemantics.blockActions(facetKeyLabel)),
            ) {
                Icon(
                    imageVector = Icons.Default.MoreVert,
                    contentDescription = "Block actions",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        }
        DropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) },
        ) {
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
