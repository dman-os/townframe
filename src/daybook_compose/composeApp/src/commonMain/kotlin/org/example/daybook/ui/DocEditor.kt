@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ArrowDownward
import androidx.compose.material.icons.filled.ArrowUpward
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material.icons.filled.Star
import androidx.compose.material.icons.outlined.StarOutline
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.PlainTooltip
import androidx.compose.material3.Surface
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
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.draw.clip
import coil3.compose.AsyncImage
import org.example.daybook.LocalContainer
import org.example.daybook.ui.editor.EditorSessionController
import org.example.daybook.ui.editor.FacetEditorKind
import org.example.daybook.ui.editor.FacetViewDescriptor
import org.example.daybook.ui.editor.dmetaFacetKey
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.uniffi.types.Blob
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.WellKnownFacet

@Composable
fun DocEditor(
    controller: EditorSessionController,
    showInlineFacetRack: Boolean = false,
    modifier: Modifier = Modifier,
) {
    val state by controller.state.collectAsState()
    val snackbarHostState = remember { SnackbarHostState() }
    var uiMessage by remember { mutableStateOf<String?>(null) }

    LaunchedEffect(state.saveError) {
        val errorMessage = state.saveError ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(errorMessage)
    }
    LaunchedEffect(uiMessage) {
        val nextMessage = uiMessage ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(nextMessage)
        uiMessage = null
    }

    Box(modifier = modifier.fillMaxSize()) {
        Column(modifier = Modifier.fillMaxSize()) {
            TextField(
                value = state.titleDraft,
                onValueChange = { value -> controller.setTitleDraft(value) },
                modifier = Modifier.fillMaxWidth(),
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
                    )
            )
            state.titleNotice?.let { titleNotice ->
                Text(
                    text = titleNotice,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))

            BoxWithConstraints(modifier = Modifier.weight(1f).fillMaxWidth()) {
                val facetViewportHeight = maxHeight
                Column(
                    modifier = Modifier.fillMaxWidth().verticalScroll(rememberScrollState())
                ) {
                    if (state.contentFacetViews.isEmpty()) {
                        Text(
                            text = "No facets",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }

                    state.contentFacetViews.forEachIndexed { index, descriptor ->
                        FacetListItem(
                            descriptor = descriptor,
                            doc = state.doc,
                            controller = controller,
                            canShowMenu = state.docId != null,
                            noteDraft = state.noteEditors[descriptor.facetKey]?.draft,
                            noteEditable = state.noteEditors[descriptor.facetKey]?.editable ?: false,
                            noteNotice = state.noteEditors[descriptor.facetKey]?.notice,
                            canMoveUp = index > 0,
                            canMoveDown = index < state.contentFacetViews.lastIndex,
                            noteMinHeight = facetViewportHeight * 0.75f,
                            onUiError = { message -> uiMessage = message },
                        )
                        if (index < state.contentFacetViews.lastIndex) {
                            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                        }
                    }

                    if (state.isSaving) {
                        Text(
                            text = "Saving…",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(top = 8.dp),
                        )
                    }

                    if (showInlineFacetRack) {
                        HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                        Text(
                            text = "Details",
                            style = MaterialTheme.typography.titleSmall,
                            modifier = Modifier.padding(bottom = 8.dp)
                        )
                        DocDetailsSidebar(
                            doc = state.doc,
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                }
            }
        }

        SnackbarHost(
            hostState = snackbarHostState,
            modifier = Modifier.align(Alignment.BottomCenter).padding(8.dp)
        )
    }
}

@Composable
private fun FacetListItem(
    descriptor: FacetViewDescriptor,
    doc: Doc?,
    controller: EditorSessionController,
    canShowMenu: Boolean,
    noteDraft: String?,
    noteEditable: Boolean,
    noteNotice: String?,
    canMoveUp: Boolean,
    canMoveDown: Boolean,
    noteMinHeight: androidx.compose.ui.unit.Dp,
    onUiError: (String) -> Unit,
) {
    Column(modifier = Modifier.fillMaxWidth()) {
        FacetHeader(
            descriptor = descriptor,
            canShowMenu = canShowMenu,
            onAddNote = { controller.addNoteFacetAfter(descriptor.facetKey) },
            onMakePrimary = { controller.makeFacetPrimary(descriptor.facetKey) },
            onMoveUp = { controller.moveFacetEarlier(descriptor.facetKey) },
            onMoveDown = { controller.moveFacetLater(descriptor.facetKey) },
            canMoveUp = canMoveUp,
            canMoveDown = canMoveDown,
        )
        when (descriptor.kind) {
            FacetEditorKind.Note -> {
                val value = noteDraft ?: ""
                val noteLineCount = value.count { character -> character == '\n' } + 1
                val noteMinLines = 6
                val noteMaxLines = if (noteLineCount < noteMinLines) noteMinLines else noteLineCount
                TextField(
                    value = value,
                    onValueChange = { nextValue ->
                        controller.setNoteDraft(descriptor.facetKey, nextValue)
                    },
                    modifier = Modifier.fillMaxWidth().heightIn(min = noteMinHeight),
                    enabled = noteEditable,
                    minLines = noteMinLines,
                    maxLines = noteMaxLines,
                    placeholder = { Text("Start typing...") },
                    colors =
                        TextFieldDefaults.colors(
                            focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                        )
                )
                noteNotice?.let {
                    Text(
                        text = it,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
            FacetEditorKind.ImageMetadata -> {
                ImageFacetView(
                    descriptor = descriptor,
                    doc = doc,
                    onError = onUiError,
                )
            }
            FacetEditorKind.GenericJson -> {
                GenericFacetView(rawValue = descriptor.rawValue)
            }
        }
    }
}

@Composable
private fun FacetHeader(
    descriptor: FacetViewDescriptor,
    canShowMenu: Boolean,
    onAddNote: () -> Unit,
    onMakePrimary: () -> Unit,
    onMoveUp: () -> Unit,
    onMoveDown: () -> Unit,
    canMoveUp: Boolean,
    canMoveDown: Boolean,
) {
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    Row(
        modifier = Modifier.fillMaxWidth().hoverable(interactionSource),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(
            text = facetKeyString(descriptor.facetKey),
            style = MaterialTheme.typography.titleSmall,
        )
        if (canShowMenu) {
            var expanded by remember { mutableStateOf(false) }
            val showPriorityActions = isHovered || expanded
            Row(
                verticalAlignment = Alignment.CenterVertically,
            ) {
                if (showPriorityActions) {
                    FacetActionIconButton(
                        label = if (descriptor.isPrimary) "Facet is primary" else "Make this facet primary",
                        onClick = onMakePrimary,
                    ) {
                        Icon(
                            if (descriptor.isPrimary) Icons.Filled.Star else Icons.Outlined.StarOutline,
                            contentDescription = null,
                        )
                    }
                    FacetActionIconButton(
                        label = "Move facet up",
                        onClick = onMoveUp,
                        enabled = canMoveUp,
                    ) {
                        Icon(Icons.Filled.ArrowUpward, contentDescription = null)
                    }
                    FacetActionIconButton(
                        label = "Move facet down",
                        onClick = onMoveDown,
                        enabled = canMoveDown,
                    ) {
                        Icon(Icons.Filled.ArrowDownward, contentDescription = null)
                    }
                    Spacer(modifier = Modifier.width(4.dp))
                }

                Box {
                    IconButton(onClick = { expanded = true }) {
                        Icon(Icons.Default.MoreVert, contentDescription = "Facet actions")
                    }
                    DropdownMenu(
                        expanded = expanded,
                        onDismissRequest = { expanded = false },
                    ) {
                        Text(
                            text = "Add new facet",
                            style = MaterialTheme.typography.labelSmall,
                            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
                        )
                        DropdownMenuItem(
                            text = { Text("Note") },
                            onClick = {
                                expanded = false
                                onAddNote()
                            },
                        )
                    }
                }
            }
        }
    }
}

@Composable
@OptIn(ExperimentalMaterial3Api::class)
private fun FacetActionIconButton(
    label: String,
    onClick: () -> Unit,
    enabled: Boolean = true,
    content: @Composable () -> Unit,
) {
    TooltipBox(
        positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
        tooltip = { PlainTooltip { Text(label) } },
        state = rememberTooltipState(),
    ) {
        IconButton(
            onClick = onClick,
            enabled = enabled,
        ) {
            content()
        }
    }
}

@Composable
private fun GenericFacetView(rawValue: String) {
    Text(
        text = rawValue,
        style = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
        modifier = Modifier.fillMaxWidth().padding(top = 4.dp),
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
}

@Composable
private fun ImageFacetView(
    descriptor: FacetViewDescriptor,
    doc: Doc?,
    onError: (String) -> Unit,
) {
    val blobsRepo = LocalContainer.current.blobsRepo
    val imageMeta =
        decodeWellKnownFacet<WellKnownFacet.ImageMetadata>(descriptor.rawValue)
            .getOrElse {
                ImageFacetError(
                    "Invalid image metadata facet payload; image preview disabled to avoid destructive edits."
                )
                return
            }
            .v1

    val resolvedDoc =
        doc ?: run {
            ImageFacetError("Referenced blob facet not found.")
            return
        }

    val blobKey =
        resolvedDoc.facets.keys.firstOrNull { key ->
            stripFacetRefFragment(buildSelfFacetRefUrl(key)) ==
                stripFacetRefFragment(imageMeta.facetRef)
        } ?: run {
            ImageFacetError("Referenced blob facet not found.")
            return
        }

    val blobValue =
        resolvedDoc.facets[blobKey] ?: run {
            ImageFacetError("Referenced blob facet not found.")
            return
        }

    val blobFacet =
        decodeWellKnownFacet<WellKnownFacet.Blob>(blobValue)
            .getOrElse {
                ImageFacetError(
                    "Invalid blob facet payload; image preview disabled to avoid destructive edits."
                )
                return
            }

    val blobHash =
        blobHash(blobFacet.v1) ?: run {
            ImageFacetError("Blob facet has no resolvable local hash URL.")
            return
        }

    val imagePath by
        produceState<String?>(initialValue = null, blobHash) {
            value =
                try {
                    blobsRepo.getPath(blobHash)
                } catch (error: Throwable) {
                    onError(error.message ?: "Failed to resolve image path")
                    null
                }
        }

    Column(modifier = Modifier.fillMaxWidth().padding(top = 4.dp)) {
        if (imagePath != null) {
            val imageModifier =
                if (descriptor.isPrimary) {
                    Modifier.fillMaxWidth().heightIn(min = 260.dp)
                } else {
                    Modifier.fillMaxWidth().height(200.dp)
                }

            Box(modifier = imageModifier) {
                AsyncImage(
                    model = "file://$imagePath",
                    contentDescription = "Document image",
                    modifier = Modifier.fillMaxSize().clip(MaterialTheme.shapes.medium),
                    contentScale = ContentScale.FillWidth,
                )

                val width = imageMeta.widthPx.toString()
                val height = imageMeta.heightPx.toString()
                Surface(
                    tonalElevation = 2.dp,
                    shape = MaterialTheme.shapes.small,
                    color = MaterialTheme.colorScheme.surface.copy(alpha = 0.85f),
                    modifier = Modifier.align(Alignment.BottomEnd).padding(8.dp),
                ) {
                    Text(
                        text = "${width}×${height}",
                        style = MaterialTheme.typography.labelSmall,
                        modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp),
                    )
                }
            }
        } else {
            Text(
                "Image path unavailable",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun ImageFacetError(message: String) {
    Text(
        message,
        style = MaterialTheme.typography.bodySmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
        modifier = Modifier.fillMaxWidth().padding(top = 4.dp),
    )
}

@Composable
fun DocFacetSidebar(
    controller: EditorSessionController,
    modifier: Modifier = Modifier,
) {
    val state by controller.state.collectAsState()
    Column(modifier = modifier.fillMaxSize().padding(8.dp)) {
        Text(
            text = "Details",
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(bottom = 8.dp)
        )
        DocDetailsSidebar(doc = state.doc, modifier = Modifier.fillMaxSize())
    }
}

@Composable
private fun DocDetailsSidebar(
    doc: Doc?,
    modifier: Modifier = Modifier,
) {
    val dmetaDetails =
        run {
            val raw = doc?.facets?.get(dmetaFacetKey()) ?: return@run null
            parseDmetaSidebarDetails(raw).getOrElse { return@run null }
        }

    Column(modifier = modifier) {
        DetailRow("Doc ID", doc?.id ?: "Unsaved")
        DetailRow("Created", dmetaDetails?.createdAt ?: "Unknown")
        DetailRow("Last modified", dmetaDetails?.lastModifiedAt ?: "Unknown")
        DetailRow(
            "Supported facets",
            (doc?.facets?.keys
                ?.count { key ->
                    when ((key.tag as? org.example.daybook.uniffi.types.FacetTag.WellKnown)?.v1) {
                        org.example.daybook.uniffi.types.WellKnownFacetTag.NOTE,
                        org.example.daybook.uniffi.types.WellKnownFacetTag.IMAGE_METADATA -> true
                        else -> false
                    }
                } ?: 0).toString()
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
private fun InlineFacetRack(
    facetRows: List<Pair<FacetKey, String>>,
    modifier: Modifier = Modifier,
) {
    Column(modifier = modifier) {
        facetRows.forEach { facetRow ->
            Row(
                modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(
                    text = facetKeyString(facetRow.first),
                    style = MaterialTheme.typography.bodySmall,
                    modifier = Modifier.weight(0.45f)
                )
                Text(
                    text = previewFacetValue(facetRow.second),
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(0.55f)
                )
            }
        }
    }
}

@Composable
private fun FacetRackList(
    facetRows: List<Pair<FacetKey, String>>,
    modifier: Modifier = Modifier,
) {
    LazyColumn(modifier = modifier) {
        items(facetRows) { facetRow ->
            Row(
                modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(
                    text = facetKeyString(facetRow.first),
                    style = MaterialTheme.typography.bodySmall,
                    modifier = Modifier.weight(0.45f)
                )
                Text(
                    text = previewFacetValue(facetRow.second),
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(0.55f)
                )
            }
        }
    }
}

private fun blobHash(blob: Blob): String? {
    val fromUrl =
        blob.urls?.firstNotNullOfOrNull { url ->
            if (!url.startsWith("db+blob:///")) {
                null
            } else {
                val hashValue = url.removePrefix("db+blob:///")
                if (hashValue.isBlank()) null else hashValue
            }
        }
    if (!fromUrl.isNullOrBlank()) {
        return fromUrl
    }
    return blob.digest.ifBlank { null }
}
