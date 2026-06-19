@file:Suppress("FunctionNaming")

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
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.LocalContainer
import org.example.daybook.ui.buildSelfFacetRefUrl
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

private sealed interface PluginFacetViewState {
    data object Loading : PluginFacetViewState
    data object RuntimeUnavailable : PluginFacetViewState
    data class Ready(val spec: ViewSpec) : PluginFacetViewState
    data class Failed(val message: String) : PluginFacetViewState
}

@Composable
internal fun FacetContentHost(args: FacetContentHostArgs) {
    val customView = args.displayHint?.deets as? FacetDisplayDeets.CustomView
    when {
        customView != null -> {
            PluginFacetView(
                docId = args.doc?.id,
                branchPath = args.branchPath,
                facetKey = args.descriptor.facetKey,
                customView = customView,
            )
        }

        args.descriptor.kind == FacetEditorKind.Note -> {
            NoteFacetView(
                descriptor = args.descriptor,
                noteEditor = args.noteEditor,
            )
        }

        args.descriptor.kind == FacetEditorKind.ImageMetadata -> {
            ImageFacetView(
                descriptor = args.descriptor,
                doc = args.doc,
                onError = args.onUiError,
            )
        }

        args.descriptor.kind == FacetEditorKind.GenericJson -> {
            GenericFacetView(rawValue = args.descriptor.rawValue)
        }
    }
}

internal data class FacetContentHostArgs(
    val descriptor: FacetViewDescriptor,
    val doc: Doc?,
    val branchPath: String,
    val displayHint: FacetDisplayHint?,
    val noteEditor: FacetNoteEditorProps,
    val onUiError: (String) -> Unit,
)

@Composable
private fun NoteFacetView(descriptor: FacetViewDescriptor, noteEditor: FacetNoteEditorProps) {
    val value = noteEditor.draft.orEmpty()
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
        placeholder = {
            Text(
                "Note",
                color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.48f),
            )
        },
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
    val viewState by
        produceState<PluginFacetViewState>(
            initialValue = PluginFacetViewState.Loading,
            docId,
            branchPath,
            facetKeyRefPath,
            customView.view.plugId,
            customView.view.viewKey,
            rtFfi,
        ) {
            value =
                loadPluginFacetViewState(
                    PluginFacetLoadArgs(
                        docId = docId,
                        branchPath = branchPath,
                        facetKeyRefPath = facetKeyRefPath,
                        customView = customView,
                        rtFfi = rtFfi,
                    ),
                )
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

private data class PluginFacetLoadArgs(
    val docId: String?,
    val branchPath: String,
    val facetKeyRefPath: String,
    val customView: FacetDisplayDeets.CustomView,
    val rtFfi: org.example.daybook.uniffi.RtFfi?,
    val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
)

private suspend fun loadPluginFacetViewState(args: PluginFacetLoadArgs): PluginFacetViewState = when {
    args.docId == null -> PluginFacetViewState.Failed("Document unavailable")

    args.rtFfi == null -> PluginFacetViewState.RuntimeUnavailable

    else ->
        runCatching {
            withContext(args.ioDispatcher) {
                args.rtFfi.renderFacetView(
                    docId = args.docId,
                    branchPath = args.branchPath,
                    facetKey = args.facetKeyRefPath,
                    requestedView = args.customView.view,
                    uiStateJson = null,
                )
            }
        }.fold(
            onSuccess = { record -> PluginFacetViewState.Ready(record.view) },
            onFailure = { exception ->
                if (exception is CancellationException) {
                    throw exception
                }
                PluginFacetViewState.Failed(exception.message ?: exception::class.simpleName.orEmpty())
            },
        )
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

private sealed interface ImageFacetResolution {
    data class Ready(
        val descriptor: FacetViewDescriptor,
        val imageMeta: org.example.daybook.uniffi.types.ImageMetadata,
        val blobHash: String,
    ) : ImageFacetResolution

    data class Error(val message: String) : ImageFacetResolution
}

private fun resolveImageFacet(descriptor: FacetViewDescriptor, doc: Doc?): ImageFacetResolution {
    val imageMeta =
        decodeWellKnownFacet<WellKnownFacet.ImageMetadata>(descriptor.rawValue)
            .getOrNull()
            ?.v1
    val resolvedDoc = doc
    val blobKey =
        imageMeta?.let { imageMetaValue ->
            resolvedDoc?.facets?.keys?.firstOrNull { key ->
                stripFacetRefFragment(buildSelfFacetRefUrl(key)) ==
                    stripFacetRefFragment(imageMetaValue.facetRef)
            }
        }
    val blobValue = blobKey?.let { key -> resolvedDoc?.facets?.get(key) }
    val blobFacet =
        blobValue?.let { blobValueString ->
            decodeWellKnownFacet<WellKnownFacet.Blob>(blobValueString).getOrNull()
        }
    val blobHash = blobFacet?.v1?.let(::blobHash)

    return when {
        imageMeta == null -> ImageFacetResolution.Error(
            "Invalid image metadata facet payload; image preview disabled to avoid destructive edits.",
        )

        resolvedDoc == null -> ImageFacetResolution.Error("Referenced blob facet not found.")

        blobKey == null -> ImageFacetResolution.Error("Referenced blob facet not found.")

        blobValue == null -> ImageFacetResolution.Error("Referenced blob facet not found.")

        blobFacet == null -> ImageFacetResolution.Error(
            "Invalid blob facet payload; image preview disabled to avoid destructive edits.",
        )

        blobHash == null -> ImageFacetResolution.Error("Blob facet has no resolvable local hash URL.")

        else -> ImageFacetResolution.Ready(
            descriptor = descriptor,
            imageMeta = imageMeta,
            blobHash = blobHash,
        )
    }
}

@Composable
private fun ImageFacetView(descriptor: FacetViewDescriptor, doc: Doc?, onError: (String) -> Unit) {
    when (val resolution = resolveImageFacet(descriptor = descriptor, doc = doc)) {
        is ImageFacetResolution.Error -> ImageFacetError(resolution.message)
        is ImageFacetResolution.Ready -> ImageFacetBody(resolution, onError)
    }
}

@Composable
private fun ImageFacetBody(resolution: ImageFacetResolution.Ready, onError: (String) -> Unit) {
    val blobsRepo = LocalContainer.current.blobsRepo
    val imagePath by rememberImagePath(
        blobHash = resolution.blobHash,
        blobsRepo = blobsRepo,
        onError = onError,
    )

    Column(modifier = Modifier.fillMaxWidth().padding(top = 4.dp)) {
        if (imagePath != null) {
            ImageFacetPreview(resolution = resolution, imagePath = imagePath)
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
private fun rememberImagePath(
    blobHash: String,
    blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi,
    onError: (String) -> Unit,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) = produceState<String?>(initialValue = null, blobHash) {
    value =
        runCatching {
            withContext(ioDispatcher) { blobsRepo.getPath(blobHash) }
        }.getOrElse { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            onError(exception.message ?: "Failed to resolve image path")
            null
        }
}

@Composable
private fun ImageFacetPreview(resolution: ImageFacetResolution.Ready, imagePath: String?) {
    val imageModifier =
        if (resolution.descriptor.isPrimary) {
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

        val width = resolution.imageMeta.widthPx.toString()
        val height = resolution.imageMeta.heightPx.toString()
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
