package org.example.daybook.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import coil3.compose.AsyncImage
import org.example.daybook.LocalContainer
import org.example.daybook.ui.editor.FacetEditorKind
import org.example.daybook.ui.editor.EditorSessionController
import org.example.daybook.ui.editor.imageMetadataFacetKey
import org.example.daybook.ui.editor.blobFacetKey
import org.example.daybook.uniffi.types.Blob
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

    if (state.doc == null && state.docId == null) {
        // Unsaved doc state (Capture text mode style): still show editor so first input creates doc.
    }

    Box(modifier = modifier.fillMaxSize()) {
        val editors = state.visibleEditors
        val titleDescriptor = editors.firstOrNull { descriptor -> descriptor.kind == FacetEditorKind.Title }
        val bodyDescriptors = editors.filter { descriptor -> descriptor.kind != FacetEditorKind.Title }
        val noteLineCount = state.noteDraft.count { character -> character == '\n' } + 1
        val noteMinLines = 6
        val noteMaxLines = if (noteLineCount < noteMinLines) noteMinLines else noteLineCount

        Column(modifier = Modifier.fillMaxSize()) {
            if (titleDescriptor != null) {
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
                val titleNotice = state.titleNotice
                if (titleNotice != null) {
                    Text(
                        text = titleNotice,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
            }

            Column(
                modifier = Modifier.weight(1f).fillMaxWidth().verticalScroll(rememberScrollState())
            ) {
                bodyDescriptors.forEach { descriptor ->
                    when (descriptor.kind) {
                        FacetEditorKind.Image -> {
                            ImageFacetEditor(
                                controller = controller,
                                onError = { message -> uiMessage = message },
                            )
                            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                        }
                        FacetEditorKind.Note -> {
                            TextField(
                                value = state.noteDraft,
                                onValueChange = { value -> controller.setNoteDraft(value) },
                                modifier = Modifier.fillMaxWidth().heightIn(min = 160.dp),
                                enabled = state.noteEditable,
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
                            val noteNotice = state.noteNotice
                            if (noteNotice != null) {
                                Text(
                                    text = noteNotice,
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                        }
                        else -> Unit
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
                        text = "Facets",
                        style = MaterialTheme.typography.titleSmall,
                        modifier = Modifier.padding(bottom = 8.dp)
                    )
                    InlineFacetRack(
                        facetRows = state.facetRows,
                        modifier = Modifier.fillMaxWidth()
                    )
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
fun DocFacetSidebar(
    controller: EditorSessionController,
    modifier: Modifier = Modifier,
) {
    val state by controller.state.collectAsState()
    Column(modifier = modifier.fillMaxSize().padding(8.dp)) {
        Text(
            text = "Facets",
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(bottom = 8.dp)
        )
        FacetRackList(
            facetRows = state.facetRows,
            modifier = Modifier.fillMaxSize()
        )
    }
}

@Composable
private fun InlineFacetRack(
    facetRows: List<Pair<org.example.daybook.uniffi.types.FacetKey, String>>,
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
    facetRows: List<Pair<org.example.daybook.uniffi.types.FacetKey, String>>,
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

@Composable
private fun ImageFacetEditor(controller: EditorSessionController, onError: (String) -> Unit) {
    val state by controller.state.collectAsState()
    val blobsRepo = LocalContainer.current.blobsRepo
    val doc = state.doc
    val blobValue = doc?.facets?.get(blobFacetKey())
    val imageMetaValue = doc?.facets?.get(imageMetadataFacetKey())
    val blobDecodeResult = blobValue?.let { value -> decodeWellKnownFacet<WellKnownFacet.Blob>(value) }
    val imageMetaDecodeResult =
        imageMetaValue?.let { value -> decodeWellKnownFacet<WellKnownFacet.ImageMetadata>(value) }
    val blobHash = blobDecodeResult?.getOrNull()?.let { facet -> blobHash(facet.v1) }
    val imageErrorNotice =
        when {
            blobDecodeResult?.isFailure == true ->
                "Invalid blob facet payload; image preview disabled to avoid destructive edits."
            imageMetaDecodeResult?.isFailure == true ->
                "Invalid image metadata facet payload; image preview disabled to avoid destructive edits."
            blobValue != null && blobHash.isNullOrBlank() ->
                "Blob facet has no resolvable local hash URL."
            else -> null
        }
    val imagePath by
        produceState<String?>(initialValue = null, blobHash) {
            value =
                if (blobHash.isNullOrBlank() || imageErrorNotice != null) {
                    null
                } else {
                    try {
                        blobsRepo.getPath(blobHash)
                    } catch (error: Throwable) {
                        onError(error.message ?: "Failed to resolve image path")
                        null
                    }
                }
        }

    Box(modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp), contentAlignment = Alignment.CenterStart) {
        Column(modifier = Modifier.fillMaxWidth()) {
            Text("Image", style = MaterialTheme.typography.titleSmall)
            if (blobValue == null && imageMetaValue == null) {
                Text(
                    "No image facets",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            } else {
                if (imageErrorNotice != null) {
                    Text(
                        imageErrorNotice,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                } else if (imagePath != null) {
                    AsyncImage(
                        model = "file://$imagePath",
                        contentDescription = "Document image",
                        modifier = Modifier.fillMaxWidth().height(220.dp),
                        contentScale = ContentScale.Fit,
                    )
                } else {
                    Text(
                        "Image path unavailable",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                if (imageMetaValue != null) {
                    Text("meta: ${previewFacetValue(imageMetaValue)}", style = MaterialTheme.typography.bodySmall)
                }
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

private fun facetKeyString(key: org.example.daybook.uniffi.types.FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is org.example.daybook.uniffi.types.FacetTag.WellKnown -> tag.v1.name.lowercase()
            is org.example.daybook.uniffi.types.FacetTag.Any -> tag.v1
        }
    return if (key.id == "main") tagString else "$tagString:${key.id}"
}
