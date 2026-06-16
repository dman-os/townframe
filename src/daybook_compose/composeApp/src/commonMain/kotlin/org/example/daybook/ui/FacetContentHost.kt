@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.ui

import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsFocusedAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import coil3.compose.AsyncImage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.LocalContainer
import org.example.daybook.ui.editor.FacetEditorKind
import org.example.daybook.ui.editor.FacetViewDescriptor
import org.example.daybook.ui.editor.facetKeyRefPathString
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.ui.view.DaybookView
import org.example.daybook.uniffi.types.Blob
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetDisplayDeets
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.ViewSpec
import org.example.daybook.uniffi.types.WellKnownFacet
import kotlin.coroutines.cancellation.CancellationException

internal data class FacetNoteEditorProps(
    val draft: String?,
    val editable: Boolean,
    val notice: String?,
    val onFocusChanged: (Boolean) -> Unit = {},
    val onDraftChange: (String) -> Unit,
)

private sealed interface PluginFacetViewState {
    data object Loading : PluginFacetViewState
    data object RuntimeUnavailable : PluginFacetViewState
    data class Ready(val spec: ViewSpec) : PluginFacetViewState
    data class Failed(val message: String) : PluginFacetViewState
}

@Composable
internal fun FacetContentHost(
    descriptor: FacetViewDescriptor,
    doc: Doc?,
    branchPath: String,
    displayHint: FacetDisplayHint?,
    noteEditor: FacetNoteEditorProps,
    onUiError: (String) -> Unit,
) {
    val customView = displayHint?.deets as? FacetDisplayDeets.CustomView
    when {
        customView != null -> {
            PluginFacetView(
                docId = doc?.id,
                branchPath = branchPath,
                facetKey = descriptor.facetKey,
                customView = customView,
            )
        }

        descriptor.kind == FacetEditorKind.Note -> {
            NoteFacetView(
                descriptor = descriptor,
                noteEditor = noteEditor,
            )
        }

        descriptor.kind == FacetEditorKind.ImageMetadata -> {
            ImageFacetView(
                descriptor = descriptor,
                doc = doc,
                onError = onUiError,
            )
        }

        descriptor.kind == FacetEditorKind.GenericJson -> {
            GenericFacetView(rawValue = descriptor.rawValue)
        }
    }
}

@Composable
private fun NoteFacetView(descriptor: FacetViewDescriptor, noteEditor: FacetNoteEditorProps) {
    val value = noteEditor.draft ?: ""
    val interactionSource = remember { MutableInteractionSource() }
    val isFocused by interactionSource.collectIsFocusedAsState()

    LaunchedEffect(isFocused) {
        noteEditor.onFocusChanged(isFocused)
    }

    TextField(
        value = value,
        onValueChange = noteEditor.onDraftChange,
        interactionSource = interactionSource,
        modifier =
        Modifier
            .fillMaxWidth()
            .then(if (descriptor.isPrimary) Modifier.heightIn(min = 260.dp) else Modifier)
            .testTag(DaybookEditorSemantics.noteField(facetKeyString(descriptor.facetKey)))
            .semantics {
                contentDescription = "Note facet ${facetKeyString(descriptor.facetKey)}"
            },
        enabled = noteEditor.editable,
        minLines = 1,
        maxLines = Int.MAX_VALUE,
        placeholder = { Text("Start typing...") },
        colors =
        TextFieldDefaults.colors(
            focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
            unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
        ),
    )
    noteEditor.notice?.let {
        Text(
            text = it,
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

@Composable
private fun PluginFacetView(
    docId: String?,
    branchPath: String,
    facetKey: FacetKey,
    customView: FacetDisplayDeets.CustomView,
) {
    val rtFfi = LocalContainer.current.rtFfi
    val facetKeyLabel = facetKeyString(facetKey)
    val facetKeyRefPath = facetKeyRefPathString(facetKey)
    var viewState by remember { mutableStateOf<PluginFacetViewState>(PluginFacetViewState.Loading) }

    LaunchedEffect(
        docId,
        branchPath,
        facetKeyRefPath,
        customView.view.plugId,
        customView.view.viewKey,
        rtFfi,
    ) {
        viewState = PluginFacetViewState.Loading
        when {
            docId == null -> viewState = PluginFacetViewState.Failed("Document unavailable")

            rtFfi == null -> viewState = PluginFacetViewState.RuntimeUnavailable

            else -> {
                viewState =
                    try {
                        val record =
                            withContext(Dispatchers.IO) {
                                rtFfi.renderFacetView(
                                    docId = docId,
                                    branchPath = branchPath,
                                    facetKey = facetKeyRefPath,
                                    requestedView = customView.view,
                                    uiStateJson = null,
                                )
                            }
                        PluginFacetViewState.Ready(record.view)
                    } catch (throwable: Throwable) {
                        if (throwable is CancellationException) {
                            throw throwable
                        }
                        PluginFacetViewState.Failed(throwable.message ?: throwable::class.simpleName.orEmpty())
                    }
            }
        }
    }

    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(DaybookEditorSemantics.pluginFacet(facetKeyLabel)),
    ) {
        when (val state = viewState) {
            PluginFacetViewState.Loading -> {
                FacetStatusText(
                    text = "Loading plugin view...",
                    modifier = Modifier.testTag(DaybookEditorSemantics.pluginFacetState(facetKeyLabel)),
                )
            }

            PluginFacetViewState.RuntimeUnavailable -> {
                FacetStatusText(
                    text = "Plugin runtime unavailable",
                    modifier = Modifier.testTag(DaybookEditorSemantics.pluginFacetState(facetKeyLabel)),
                )
            }

            is PluginFacetViewState.Failed -> {
                FacetStatusText(
                    text = "Plugin view failed: ${state.message}",
                    modifier = Modifier.testTag(DaybookEditorSemantics.pluginFacetState(facetKeyLabel)),
                )
            }

            is PluginFacetViewState.Ready -> {
                DaybookView(spec = state.spec)
            }
        }
    }
}

@Composable
internal fun FacetStatusText(text: String, modifier: Modifier = Modifier) {
    Surface(
        modifier = modifier.fillMaxWidth(),
        color = MaterialTheme.colorScheme.surfaceVariant,
        shape = MaterialTheme.shapes.small,
    ) {
        Text(
            text = text,
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(12.dp),
        )
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
private fun ImageFacetView(descriptor: FacetViewDescriptor, doc: Doc?, onError: (String) -> Unit) {
    val blobsRepo = LocalContainer.current.blobsRepo
    val imageMeta =
        decodeWellKnownFacet<WellKnownFacet.ImageMetadata>(descriptor.rawValue)
            .getOrElse {
                ImageFacetError(
                    "Invalid image metadata facet payload; image preview disabled to avoid destructive edits.",
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
                    "Invalid blob facet payload; image preview disabled to avoid destructive edits.",
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
                        text = "$width×$height",
                        style = MaterialTheme.typography.labelSmall,
                        modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp),
                    )
                }
            }
        } else {
            Text(
                "Image path unavailable",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
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
