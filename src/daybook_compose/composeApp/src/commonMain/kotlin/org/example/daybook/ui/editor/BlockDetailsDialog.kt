@file:Suppress(
    "FunctionNaming",
    "LongMethod",
    "CyclomaticComplexMethod",
    "TooGenericExceptionCaught",
    "InstanceOfCheckForException",
)

package org.example.daybook.ui.editor

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.CheckCircle
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.selected
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.example.daybook.DaybookEditorSemantics
import org.example.daybook.LocalBigDialogController
import org.example.daybook.LocalContainer
import org.example.daybook.ui.decodeWellKnownFacet
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.ui.editor.facetTagDisplayString
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.types.Dmeta
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetMeta
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.NoteEditorConfig
import org.example.daybook.uniffi.types.NoteMimeOption
import org.example.daybook.uniffi.types.WellKnownFacet

private val noteEditorConfigJson =
    Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

private const val CORE_PLUG_ID = "@daybook/core"
private const val NOTE_EDITOR_CONFIG_FACET_TAG = "org.example.daybook.note-editor-config"
private const val NOTE_EDITOR_CONFIG_FACET_ID = "main"
private const val CURRENT_CUSTOM_NOTE_FORMAT_LABEL = "Current custom format"
private const val CURRENT_CUSTOM_NOTE_FORMAT_DESCRIPTION =
    "This note uses a MIME type not listed in note editor config."

@Composable
internal fun rememberBlockDetailsDialogLauncher(controller: EditorSessionController): (FacetViewDescriptor) -> Unit {
    val bigDialogController = LocalBigDialogController.current
    return remember(controller, bigDialogController) {
        { descriptor ->
            bigDialogController.show {
                BlockDetailsDialogContent(
                    controller = controller,
                    descriptor = descriptor,
                    onClose = bigDialogController::dismiss,
                )
            }
        }
    }
}

@Composable
private fun BlockDetailsDialogContent(
    controller: EditorSessionController,
    descriptor: FacetViewDescriptor,
    onClose: () -> Unit,
) {
    val container = LocalContainer.current
    val state by controller.state.collectAsState()
    val noteEditor = state.noteEditors[descriptor.facetKey]
    val currentMime = noteEditor?.mime
    val sourceFacetKeyText = facetKeyString(descriptor.facetKey)
    val configState by
        produceState<BlockDetailsConfigState>(
            initialValue = BlockDetailsConfigState.Loading,
            container.drawerRepo,
        ) {
            value =
                loadNoteEditorConfig(container.drawerRepo)
        }

    val noteMimeOptions = remember(configState, currentMime) {
        mergeNoteMimeOptions(
            when (val value = configState) {
                is BlockDetailsConfigState.Ready -> value.config
                BlockDetailsConfigState.Loading -> null
                is BlockDetailsConfigState.Failed -> null
            },
            currentMime,
        )
    }
    var formatPage: BlockDetailsFormatPage by rememberSaveable(sourceFacetKeyText) {
        mutableStateOf(BlockDetailsFormatPage.Details)
    }
    var formatSearchQuery by rememberSaveable(sourceFacetKeyText) { mutableStateOf("") }
    var customMimeInput by rememberSaveable(sourceFacetKeyText) { mutableStateOf("") }
    var customMimeError by rememberSaveable(sourceFacetKeyText) { mutableStateOf<String?>(null) }

    val dmeta = remember(state.doc) { loadDmeta(state.doc) }
    val facetMeta: FacetMeta? = remember(dmeta, descriptor.facetKey) { dmeta?.facets?.get(descriptor.facetKey) }
    val sourceFacets = remember(descriptor.facetKey) { listOf(descriptor.facetKey) }
    val currentMimeText = currentMime ?: "Unknown"
    val facetCreatedText = facetMeta?.createdAt?.toString() ?: "Unknown"
    val facetLastModifiedText = facetMeta?.updatedAt?.maxOrNull()?.toString() ?: "Unknown"
    val sourceFacetCount = sourceFacets.size
    val currentFormatOption = remember(noteMimeOptions, currentMime) {
        noteMimeOptions.firstOrNull { it.mime == currentMime }
    }

    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .verticalScroll(rememberScrollState())
            .padding(24.dp)
            .testTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Box(modifier = Modifier.fillMaxWidth()) {
            Column(modifier = Modifier.padding(end = 72.dp)) {
                Text(
                    text =
                    when (formatPage) {
                        BlockDetailsFormatPage.Details -> "Block details"
                        BlockDetailsFormatPage.Picker -> "Choose note format"
                        BlockDetailsFormatPage.CustomMime -> "Use custom MIME"
                    },
                    style = MaterialTheme.typography.titleLarge,
                )
                Text(
                    text = facetKeyString(descriptor.facetKey),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 4.dp),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
            }
            TextButton(
                onClick = onClose,
                modifier =
                Modifier
                    .align(Alignment.TopEnd)
                    .testTag("doc-editor-block-details-close"),
            ) {
                Text("Close")
            }
            if (formatPage != BlockDetailsFormatPage.Details) {
                TextButton(
                    onClick = {
                        formatPage =
                            when (formatPage) {
                                BlockDetailsFormatPage.Details -> BlockDetailsFormatPage.Details
                                BlockDetailsFormatPage.Picker -> BlockDetailsFormatPage.Details
                                BlockDetailsFormatPage.CustomMime -> BlockDetailsFormatPage.Picker
                            }
                    },
                    modifier =
                    Modifier
                        .align(Alignment.TopEnd)
                        .padding(end = 72.dp)
                        .testTag(DaybookEditorSemantics.blockDetailsFormatPickerBackAction(sourceFacetKeyText)),
                ) {
                    Text("Back")
                }
            }
        }

        when (formatPage) {
            BlockDetailsFormatPage.Details -> {
                BlockDetailsSection(
                    title = "Block",
                    sectionTag = DaybookEditorSemantics.BLOCK_DETAILS_BLOCK_SECTION,
                ) {
                    BlockDetailsMetadataRow("Editor kind", facetKindLabel(descriptor.kind))
                    BlockDetailsMetadataRow("Source facet count", sourceFacetCount.toString())
                    BlockDetailsMetadataRow("Primary block", if (descriptor.isPrimary) "Yes" else "No")
                }

                BlockDetailsSection(
                    title = "Source facets",
                    sectionTag = DaybookEditorSemantics.BLOCK_DETAILS_SOURCE_SECTION,
                ) {
                    Surface(
                        shape = MaterialTheme.shapes.medium,
                        color = MaterialTheme.colorScheme.surfaceContainerHighest,
                        modifier =
                        Modifier
                            .fillMaxWidth()
                            .testTag(DaybookEditorSemantics.blockDetailsSourceFacetCard(sourceFacetKeyText)),
                    ) {
                        Column(modifier = Modifier.padding(16.dp)) {
                            Text(
                                text = "Current source facet",
                                style = MaterialTheme.typography.titleSmall,
                            )
                            Spacer(modifier = Modifier.size(8.dp))
                            BlockDetailsMetadataRow("Facet key", sourceFacetKeyText)
                            BlockDetailsMetadataRow("Facet tag", facetTagDisplayString(descriptor.facetKey.tag))
                            BlockDetailsMetadataRow("Facet id", descriptor.facetKey.id)
                            BlockDetailsMetadataRow("Facet created", facetCreatedText)
                            BlockDetailsMetadataRow("Facet last modified", facetLastModifiedText)
                            BlockDetailsMetadataRow("Current MIME", currentMimeText)
                        }
                    }
                }

                if (descriptor.kind == FacetEditorKind.Note) {
                    BlockDetailsSection(
                        title = "Note format",
                        sectionTag = DaybookEditorSemantics.BLOCK_DETAILS_FORMAT_SECTION,
                    ) {
                        BlockDetailsCurrentFormatSummary(
                            facetKey = descriptor.facetKey,
                            currentFormatOption = currentFormatOption,
                            currentMimeText = currentMimeText,
                        )
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.End,
                        ) {
                            TextButton(
                                onClick = {
                                    formatSearchQuery = ""
                                    customMimeError = null
                                    formatPage = BlockDetailsFormatPage.Picker
                                },
                                modifier =
                                Modifier.testTag(
                                    DaybookEditorSemantics.blockDetailsChangeFormatAction(sourceFacetKeyText),
                                ),
                            ) {
                                Text("Change format")
                            }
                        }
                    }
                }
            }

            BlockDetailsFormatPage.Picker -> {
                BlockDetailsFormatPickerPage(
                    state =
                    BlockDetailsFormatPickerState(
                        facetKey = descriptor.facetKey,
                        currentMime = currentMime,
                        options = noteMimeOptions,
                        searchQuery = formatSearchQuery,
                    ),
                    onSearchQueryChange = { formatSearchQuery = it },
                    onOptionSelected = { mime ->
                        controller.setNoteMime(descriptor.facetKey, mime)
                        formatPage = BlockDetailsFormatPage.Details
                    },
                    onUseCustomMime = {
                        customMimeInput = ""
                        customMimeError = null
                        formatPage = BlockDetailsFormatPage.CustomMime
                    },
                )
            }

            BlockDetailsFormatPage.CustomMime -> {
                BlockDetailsCustomMimePage(
                    facetKey = descriptor.facetKey,
                    inputValue = customMimeInput,
                    errorText = customMimeError,
                    onInputValueChange = { nextValue ->
                        customMimeInput = nextValue
                        customMimeError = null
                    },
                    onApply = {
                        val normalizedMime = customMimeInput.trim()
                        val validationError = validateCustomMime(normalizedMime)
                        if (validationError != null) {
                            customMimeError = validationError
                        } else {
                            controller.setNoteMime(descriptor.facetKey, normalizedMime)
                            formatPage = BlockDetailsFormatPage.Details
                        }
                    },
                )
            }
        }
    }
}

@Composable
private fun BlockDetailsSection(title: String, sectionTag: String, content: @Composable ColumnScope.() -> Unit) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(sectionTag),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text(
            text = title,
            style = MaterialTheme.typography.titleSmall,
        )
        Surface(
            shape = MaterialTheme.shapes.medium,
            color = MaterialTheme.colorScheme.surfaceContainerLowest,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Column(
                modifier = Modifier.fillMaxWidth().padding(16.dp),
                content = content,
            )
        }
    }
}

@Composable
private fun BlockDetailsMetadataRow(label: String, value: String) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp)
            .testTag(DaybookEditorSemantics.blockDetailsMetadataRow(label)),
    ) {
        Text(
            text = label,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Text(
            text = value,
            style = MaterialTheme.typography.bodySmall,
            maxLines = 2,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.testTag(DaybookEditorSemantics.blockDetailsMetadataValue(label)),
        )
    }
}

@Composable
private fun BlockDetailsCurrentFormatSummary(
    facetKey: FacetKey,
    currentFormatOption: NoteMimeOption?,
    currentMimeText: String,
) {
    val summary = currentFormatOption ?: NoteMimeOption(
        mime = currentMimeText,
        label = if (currentMimeText == "Unknown") "Unknown" else CURRENT_CUSTOM_NOTE_FORMAT_LABEL,
        description = if (currentMimeText == "Unknown") {
            "No note MIME is currently selected."
        } else {
            CURRENT_CUSTOM_NOTE_FORMAT_DESCRIPTION
        },
    )
    Surface(
        shape = MaterialTheme.shapes.medium,
        color = MaterialTheme.colorScheme.surfaceContainerHighest,
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(DaybookEditorSemantics.blockDetailsCurrentFormatSummary(facetKeyString(facetKey))),
    ) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            Text(
                text = "Current format",
                style = MaterialTheme.typography.titleSmall,
            )
            BlockDetailsMetadataRow("Format label", summary.label)
            BlockDetailsMetadataRow("Format description", summary.description)
            BlockDetailsMetadataRow("Raw MIME", summary.mime)
        }
    }
}

@Composable
private fun BlockDetailsFormatPickerPage(
    state: BlockDetailsFormatPickerState,
    onSearchQueryChange: (String) -> Unit,
    onOptionSelected: (String) -> Unit,
    onUseCustomMime: () -> Unit,
) {
    val filteredOptions = remember(state.options, state.searchQuery) {
        filterNoteMimeOptions(state.options, state.searchQuery)
    }
    BlockDetailsSection(
        title = "Choose note format",
        sectionTag = DaybookEditorSemantics.BLOCK_DETAILS_FORMAT_PICKER_SECTION,
    ) {
        Text(
            text = "Search by label, description, or MIME.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        OutlinedTextField(
            value = state.searchQuery,
            onValueChange = onSearchQueryChange,
            modifier =
            Modifier
                .fillMaxWidth()
                .padding(top = 12.dp)
                .testTag(DaybookEditorSemantics.blockDetailsFormatSearchField(facetKeyString(state.facetKey)))
                .semantics {
                    contentDescription = "Search note formats"
                },
            singleLine = true,
            label = { Text("Search formats") },
            placeholder = { Text("Label, description, or MIME") },
        )

        if (filteredOptions.isEmpty()) {
            Text(
                text = "No formats match this search.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        } else {
            LazyColumn(
                modifier =
                Modifier
                    .fillMaxWidth()
                    .heightIn(max = 360.dp),
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                items(filteredOptions, key = { it.mime }) { option ->
                    val isSelected = state.currentMime == option.mime
                    NoteMimeOptionRow(
                        facetKey = state.facetKey,
                        option = option,
                        isSelected = isSelected,
                        onClick = { onOptionSelected(option.mime) },
                    )
                }
            }
        }

        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.End,
        ) {
            TextButton(
                onClick = onUseCustomMime,
                modifier =
                Modifier
                    .testTag(DaybookEditorSemantics.blockDetailsCustomMimeAction(facetKeyString(state.facetKey))),
            ) {
                Text("Use custom MIME")
            }
        }
    }
}

@Composable
private fun BlockDetailsCustomMimePage(
    facetKey: FacetKey,
    inputValue: String,
    errorText: String?,
    onInputValueChange: (String) -> Unit,
    onApply: () -> Unit,
) {
    BlockDetailsSection(
        title = "Use custom MIME",
        sectionTag = DaybookEditorSemantics.BLOCK_DETAILS_CUSTOM_MIME_SECTION,
    ) {
        Text(
            text = "Enter a raw MIME type for this note.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        OutlinedTextField(
            value = inputValue,
            onValueChange = onInputValueChange,
            modifier =
            Modifier
                .fillMaxWidth()
                .padding(top = 12.dp)
                .testTag(DaybookEditorSemantics.blockDetailsCustomMimeInput(facetKeyString(facetKey)))
                .semantics {
                    contentDescription = "Custom note MIME"
                },
            singleLine = true,
            label = { Text("Raw MIME") },
            placeholder = { Text("text/x-custom-note") },
            isError = errorText != null,
        )
        if (errorText != null) {
            Text(
                text = errorText,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
                modifier =
                Modifier
                    .padding(top = 8.dp)
                    .testTag(DaybookEditorSemantics.blockDetailsCustomMimeError(facetKeyString(facetKey))),
            )
        }
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.End,
        ) {
            TextButton(
                onClick = onApply,
                modifier =
                Modifier
                    .testTag(DaybookEditorSemantics.blockDetailsCustomMimeConfirmAction(facetKeyString(facetKey))),
            ) {
                Text("Apply MIME")
            }
        }
    }
}

@Composable
private fun NoteMimeOptionRow(facetKey: FacetKey, option: NoteMimeOption, isSelected: Boolean, onClick: () -> Unit) {
    Surface(
        shape = MaterialTheme.shapes.medium,
        color =
        if (isSelected) {
            MaterialTheme.colorScheme.primaryContainer
        } else {
            MaterialTheme.colorScheme.surfaceContainerHighest
        },
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(DaybookEditorSemantics.blockDetailsFormatOption(facetKeyString(facetKey), option.mime))
            .semantics {
                selected = isSelected
                contentDescription = "${option.label}. ${option.description}. MIME ${option.mime}"
            }
            .clickable(role = Role.Button, onClick = onClick),
    ) {
        Row(
            modifier = Modifier.padding(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            if (isSelected) {
                Icon(
                    imageVector = Icons.Default.CheckCircle,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.primary,
                )
                Spacer(modifier = Modifier.width(12.dp))
            } else {
                Spacer(modifier = Modifier.width(28.dp))
                Spacer(modifier = Modifier.width(4.dp))
            }
            Column(modifier = Modifier.fillMaxWidth()) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(
                        text = option.label,
                        style = MaterialTheme.typography.titleSmall,
                        fontWeight = FontWeight.Medium,
                        modifier = Modifier.weight(1f),
                    )
                    Text(
                        text = option.mime,
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                Text(
                    text = option.description,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 4.dp),
                )
            }
        }
    }
}

private enum class BlockDetailsFormatPage {
    Details,
    Picker,
    CustomMime,
}

private sealed interface BlockDetailsConfigState {
    data object Loading : BlockDetailsConfigState
    data class Ready(val config: NoteEditorConfig?) : BlockDetailsConfigState
    data class Failed(val message: String) : BlockDetailsConfigState
}

private fun defaultNoteMimeOptions(): List<NoteMimeOption> = listOf(
    NoteMimeOption(
        mime = "text/plain",
        label = "Plain text",
        description = "Basic plain text notes.",
    ),
    NoteMimeOption(
        mime = "text/markdown",
        label = "Markdown",
        description = "Rich text formatting with Markdown syntax.",
    ),
)

private fun mergeNoteMimeOptions(config: NoteEditorConfig?, currentMime: String?): List<NoteMimeOption> {
    val merged: LinkedHashMap<String, NoteMimeOption> = linkedMapOf()
    defaultNoteMimeOptions().forEach { option ->
        merged[option.mime] = option
    }
    config?.mimeOptions.orEmpty().forEach { option ->
        merged[option.mime] = option
    }
    if (!currentMime.isNullOrBlank() && merged[currentMime] == null) {
        merged[currentMime] =
            NoteMimeOption(
                mime = currentMime,
                label = CURRENT_CUSTOM_NOTE_FORMAT_LABEL,
                description = CURRENT_CUSTOM_NOTE_FORMAT_DESCRIPTION,
            )
    }
    return merged.values.toList()
}

private fun filterNoteMimeOptions(options: List<NoteMimeOption>, query: String): List<NoteMimeOption> {
    val normalizedQuery = query.trim()
    if (normalizedQuery.isEmpty()) {
        return options
    }
    return options.filter { option ->
        option.mime.contains(normalizedQuery, ignoreCase = true) ||
            option.label.contains(normalizedQuery, ignoreCase = true) ||
            option.description.contains(normalizedQuery, ignoreCase = true)
    }
}

private data class BlockDetailsFormatPickerState(
    val facetKey: FacetKey,
    val currentMime: String?,
    val options: List<NoteMimeOption>,
    val searchQuery: String,
)

private fun validateCustomMime(rawMime: String): String? {
    val trimmed = rawMime.trim()
    val parts = trimmed.split('/')
    return when {
        trimmed.isBlank() -> "MIME must not be blank."

        trimmed.any { it.isWhitespace() } -> "MIME must not contain whitespace."

        parts.size != 2 || parts.any { it.isEmpty() } ->
            "MIME must be a non-empty type and subtype separated by a single '/'."

        else -> null
    }
}

private fun loadDmeta(doc: Doc?): Dmeta? = doc?.facets?.get(dmetaFacetKey())
    ?.let { raw -> decodeWellKnownFacet<WellKnownFacet.Dmeta>(raw).getOrNull()?.v1 }

private suspend fun loadNoteEditorConfig(drawerRepo: DrawerRepoFfi): BlockDetailsConfigState = try {
    val configDocId = drawerRepo.getOrInitPlugConfigDocId(CORE_PLUG_ID)
    val doc = drawerRepo.get(configDocId, "main")
    val raw =
        doc?.facets?.get(
            FacetKey(FacetTag.Any(NOTE_EDITOR_CONFIG_FACET_TAG), NOTE_EDITOR_CONFIG_FACET_ID),
        )
    val config = raw?.let { decodeNoteEditorConfig(it) }
    BlockDetailsConfigState.Ready(config)
} catch (exception: Throwable) {
    if (exception is CancellationException) {
        throw exception
    }
    BlockDetailsConfigState.Failed(exception.message ?: exception::class.simpleName.orEmpty())
}

private fun decodeNoteEditorConfig(raw: String): NoteEditorConfig {
    val parsed = noteEditorConfigJson.parseToJsonElement(raw)
    val root =
        when (parsed) {
            is JsonObject -> parsed
            is JsonPrimitive -> noteEditorConfigJson.parseToJsonElement(parsed.content).jsonObject
            else -> error("note editor config facet must be a JSON object")
        }
    val mimeOptions =
        root["mimeOptions"]
            ?.jsonArray
            ?.map { element -> decodeNoteMimeOption(element.jsonObject) }
            .orEmpty()
    return NoteEditorConfig(mimeOptions = mimeOptions)
}

private fun decodeNoteMimeOption(obj: kotlinx.serialization.json.JsonObject): NoteMimeOption {
    val mime = obj["mime"]?.jsonPrimitive?.content ?: error("note editor config mime option missing mime")
    val label = obj["label"]?.jsonPrimitive?.content ?: error("note editor config mime option missing label")
    val description =
        obj["description"]?.jsonPrimitive?.content ?: error("note editor config mime option missing description")
    return NoteMimeOption(mime = mime, label = label, description = description)
}

private fun facetKindLabel(kind: FacetEditorKind): String = when (kind) {
    FacetEditorKind.Note -> "Note"
    FacetEditorKind.ImageMetadata -> "Image"
    FacetEditorKind.GenericJson -> "Generic"
}
