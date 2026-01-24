package org.example.daybook.ui

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.*
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlin.time.Clock
import kotlin.time.Instant
import kotlinx.coroutines.launch
import org.example.daybook.ConfigViewModel
import org.example.daybook.DrawerViewModel
import org.example.daybook.LocalContainer
import org.example.daybook.uniffi.core.DateTimePropDisplayType
import org.example.daybook.uniffi.core.PropKeyDisplayDeets
import org.example.daybook.uniffi.core.PropKeyDisplayHint
import org.example.daybook.uniffi.core.UpdateDocArgs
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocContent
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.DocPropKey
import org.example.daybook.uniffi.types.DocPropTag
import org.example.daybook.uniffi.types.WellKnownPropTag

@Composable
fun DocEditor(
    doc: Doc?,
    onContentChange: (String) -> Unit,
    modifier: Modifier = Modifier,
    blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi? = null,
    configViewModel: ConfigViewModel? = null,
    drawerRepo: org.example.daybook.uniffi.DrawerRepoFfi? = null,
    drawerViewModel: DrawerViewModel? = null
) {
    val actualConfigViewModel =
        configViewModel ?: run {
            val configRepo = LocalContainer.current.configRepo
            ConfigViewModel(configRepo)
        }
    val actualDrawerRepo = drawerRepo ?: LocalContainer.current.drawerRepo
    val actualDrawerViewModel = drawerViewModel
    val scope = rememberCoroutineScope()

    // Listen to document updates from DrawerViewModel if provided
    val drawerDocState = actualDrawerViewModel?.selectedDoc
    val drawerDoc by drawerDocState?.collectAsState() ?: remember(doc) { mutableStateOf(doc) }
    val currentDoc = drawerDoc ?: doc

    Column(modifier = modifier) {
        // Title editor at the top (if enabled)
        if (currentDoc != null) {
            val keyConfigs by actualConfigViewModel.metaTableKeyConfigs.collectAsState()
            val titleTagInfo = findTitleTag(currentDoc, keyConfigs)
            val titleConfig = keyConfigs[titleTagInfo?.key ?: "title_generic"]
            val showTitleEditor =
                when (val deets = titleConfig?.deets) {
                    is PropKeyDisplayDeets.Title -> deets.showEditor
                    else -> false
                }

            if (showTitleEditor) {
                val titleText = titleTagInfo?.value?.let { dequoteJson(it) } ?: ""
                // Allow editing if we have an editable title tag, or if we can create a title_generic tag
                val isEditable =
                    titleTagInfo?.isEditable ?: ("title_generic" in KNOWN_EDITABLE_TITLE_TAGS)

                TitleEditor(
                    title = titleText,
                    enabled = isEditable,
                    onTitleChange = { newTitle ->
                        if (currentDoc == null) return@TitleEditor

                        scope.launch {
                            try {
                                val propsSet = mutableMapOf<DocPropKey, String>()
                                val propsRemove = mutableListOf<DocPropKey>()
                                val titleKey =
                                    titleTagInfo?.propKey
                                        ?: DocPropKey.Tag(
                                            DocPropTag.WellKnown(WellKnownPropTag.TITLE_GENERIC)
                                        )

                                if (newTitle.isNotBlank()) {
                                    propsSet[titleKey] = "\"$newTitle\"" // JSON string
                                } else if (titleTagInfo != null) {
                                    propsRemove.add(titleKey)
                                } else {
                                    return@launch
                                }

                                val patch =
                                    DocPatch(
                                        id = currentDoc.id,
                                        propsSet = propsSet,
                                        propsRemove = propsRemove,
                                        userPath = null
                                    )

                                if (actualDrawerViewModel != null) {
                                    actualDrawerViewModel.updateDoc(patch)
                                } else {
                                    actualDrawerRepo.updateBatch(
                                        listOf(UpdateDocArgs("main", null, patch))
                                    )
                                }
                            } catch (e: Exception) {
                                println("Error updating title: $e")
                            }
                        }
                    },
                    modifier = Modifier.fillMaxWidth()
                )
            }

            PropertiesTable(
                doc = currentDoc,
                configViewModel = actualConfigViewModel,
                drawerRepo = actualDrawerRepo,
                drawerViewModel = actualDrawerViewModel,
                modifier = Modifier.fillMaxWidth()
            )
            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
        }

        // Content editor
        Box(modifier = Modifier.weight(1f)) {
            // If doc is null, we treat it as a new document (Text content by default)
            val contentJson = currentDoc?.props?.get(
                DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.CONTENT))
            )
            val contentText =
                if (contentJson != null) {
                    // Simplified: assume it's {"text":"..."} or similar if we were usingautosurgeon
                    // Actually, let's just try to extract string if it looks like a JSON string
                    dequoteJson(contentJson)
                } else {
                    ""
                }

            var text by remember(currentDoc?.id) {
                mutableStateOf(contentText)
            }

            // Sync external changes
            LaunchedEffect(currentDoc) {
                val externalText =
                    currentDoc?.props?.get(
                        DocPropKey.Tag(DocPropTag.WellKnown(WellKnownPropTag.CONTENT))
                    )?.let {
                        dequoteJson(it)
                    }
                        ?: ""
                if (externalText != text) {
                    text = externalText
                }
            }

            TextField(
                value = text,
                onValueChange = {
                    if (text != it) {
                        text = it
                        onContentChange(it)
                    }
                },
                modifier = Modifier.fillMaxSize(),
                placeholder = { Text("Start typing...") },
                colors =
                    TextFieldDefaults.colors(
                        focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent
                    )
            )
        }
    }
}

@Composable
fun TitleEditor(
    title: String,
    onTitleChange: (String) -> Unit,
    modifier: Modifier = Modifier,
    enabled: Boolean = true
) {
    var titleValue by remember(title) { mutableStateOf(title) }

    TextField(
        value = titleValue,
        onValueChange = { newValue ->
            titleValue = newValue
            onTitleChange(newValue)
        },
        modifier = modifier,
        placeholder = {
            Text(
                text = "Title"
                // color = MaterialTheme.colorScheme.onSurfaceVariant,
                // fontSize = MaterialTheme.typography.headlineLarge.fontSize,
            )
        },
        // label = {
        //     Text("Title")
        // },
        textStyle =
            MaterialTheme.typography.headlineLarge.copy(
                fontWeight = androidx.compose.ui.text.font.FontWeight.Bold
            ),
        colors =
            TextFieldDefaults.colors(
                focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent
            )
    )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PropertiesTable(
    doc: Doc,
    configViewModel: ConfigViewModel,
    drawerRepo: org.example.daybook.uniffi.DrawerRepoFfi,
    drawerViewModel: DrawerViewModel? = null,
    modifier: Modifier = Modifier
) {
    var isExpanded by remember { mutableStateOf(false) }
    var keyConfigs by remember { mutableStateOf<Map<String, PropKeyDisplayHint>>(emptyMap()) }
    var showConfigBottomSheet by remember { mutableStateOf<String?>(null) }
    var showAddTagBottomSheet by remember { mutableStateOf(false) }
    var showEditValueBottomSheet by remember { mutableStateOf<PropertiesRow?>(null) }
    // Observe document changes from DrawerViewModel if available
    val currentDoc by if (drawerViewModel != null) {
        drawerViewModel.selectedDoc.collectAsState()
    } else {
        remember(doc) { mutableStateOf(doc) }
    }

    val actualDoc = currentDoc ?: doc

    val scope = rememberCoroutineScope()

    // Load key configs
    LaunchedEffect(configViewModel) {
        try {
            keyConfigs = configViewModel.configRepo.listDisplayHints()
        } catch (e: Exception) {
            println("Error loading key configs: $e")
        }
    }

    // Build properties rows with formatting
    // Use actualDoc.id and actualDoc.props as keys to ensure recomposition when props change
    val propsKey =
        remember(actualDoc.props) {
            actualDoc.props.hashCode()
        }

    @OptIn(kotlin.time.ExperimentalTime::class)
    val propertiesRows =
        remember(actualDoc.id, propsKey, keyConfigs) {
            val rows = mutableListOf<PropertiesRow>()

            // Timestamps
            val createdAtConfig = keyConfigs["created_at"]
            val createdAtDisplayKey = createdAtConfig?.displayTitle ?: "created_at"
            val createdAtResult = formatValue(actualDoc.createdAt, createdAtConfig, "created_at")
            val createdAtIso =
                java.time.Instant
                    .ofEpochSecond(actualDoc.createdAt.epochSeconds)
                    .toString()
            rows.add(
                PropertiesRow(
                    "created_at",
                    createdAtDisplayKey,
                    createdAtResult.formatted,
                    createdAtResult.error,
                    actualDoc.createdAt,
                    null,
                    createdAtIso
                )
            )

            val updatedAtConfig = keyConfigs["updated_at"]
            val updatedAtDisplayKey = updatedAtConfig?.displayTitle ?: "updated_at"
            val updatedAtResult = formatValue(actualDoc.updatedAt, updatedAtConfig, "updated_at")
            val updatedAtIso =
                java.time.Instant
                    .ofEpochSecond(actualDoc.updatedAt.epochSeconds)
                    .toString()
            rows.add(
                PropertiesRow(
                    "updated_at",
                    updatedAtDisplayKey,
                    updatedAtResult.formatted,
                    updatedAtResult.error,
                    actualDoc.updatedAt,
                    null,
                    updatedAtIso
                )
            )

            // Props from the map
            actualDoc.props.forEach { (propKey, jsonValue) ->
                val key = getPropKeyString(propKey)
                val tagConfig = keyConfigs[key]
                val tagDisplayKey = tagConfig?.displayTitle ?: key
                val tagResult = formatValue(jsonValue, tagConfig, key)
                rows.add(
                    PropertiesRow(
                        key,
                        tagDisplayKey,
                        tagResult.formatted,
                        tagResult.error,
                        null,
                        jsonValue,
                        jsonValue,
                        propKey = propKey
                    )
                )
            }
            rows
        }

    val alwaysVisibleRows =
        propertiesRows.filter {
            val config = keyConfigs[it.key]
            config?.alwaysVisible ?: (it.key == "created_at" || it.key == "updated_at")
        }
    val collapsibleRows =
        propertiesRows.filter {
            val config = keyConfigs[it.key]
            !(config?.alwaysVisible ?: (it.key == "created_at" || it.key == "updated_at"))
        }

    Column(modifier = Modifier.padding(8.dp)) {
        // Header with collapse/expand button and add tag button
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Row(
                modifier =
                    Modifier
                        .weight(1f)
                        .clickable { isExpanded = !isExpanded },
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    "Properties",
                    style = MaterialTheme.typography.labelMedium
                )
                Icon(
                    imageVector = if (isExpanded) Icons.Default.ExpandLess else Icons.Default.ExpandMore,
                    contentDescription = if (isExpanded) "Collapse" else "Expand"
                )
            }
            IconButton(onClick = { showAddTagBottomSheet = true }) {
                Icon(Icons.Default.Add, contentDescription = "Add tag")
            }
        }

        // Always visible rows
        alwaysVisibleRows.forEach { row ->
            val rowConfig = keyConfigs[row.key]
            val isUnixPath = rowConfig?.deets is PropKeyDisplayDeets.UnixPath
            PropertiesTableRow(
                row = row,
                onKeyClick = { showConfigBottomSheet = row.key },
                onValueClick = { showEditValueBottomSheet = row },
                onValueChange =
                    if (isUnixPath && row.propKey != null) {
                        { newValue ->
                            val propsSet = mapOf(row.propKey to "\"$newValue\"")
                            val patch =
                                DocPatch(
                                    id = actualDoc.id,
                                    propsSet = propsSet,
                                    propsRemove = emptyList(),
                                    userPath = null
                                )
                            if (drawerViewModel != null) {
                                drawerViewModel.updateDoc(patch)
                            } else {
                                scope.launch {
                                    try {
                                        drawerRepo.updateBatch(
                                            listOf(UpdateDocArgs("main", null, patch))
                                        )
                                    } catch (e: Exception) {
                                        println("Error updating value: $e")
                                    }
                                }
                            }
                        }
                    } else {
                        null
                    },
                config = rowConfig
            )
        }

        // Collapsible rows
        AnimatedVisibility(visible = isExpanded) {
            Column {
                collapsibleRows.forEach { row ->
                    val rowConfig = keyConfigs[row.key]
                    val isUnixPath = rowConfig?.deets is PropKeyDisplayDeets.UnixPath
                    PropertiesTableRow(
                        row = row,
                        onKeyClick = { showConfigBottomSheet = row.key },
                        onValueClick = { showEditValueBottomSheet = row },
                        onValueChange =
                            if (isUnixPath && row.propKey != null) {
                                { newValue ->
                                    scope.launch {
                                        try {
                                            val propsSet = mapOf(row.propKey to "\"$newValue\"")
                                            val patch =
                                                DocPatch(
                                                    id = actualDoc.id,
                                                    propsSet = propsSet,
                                                    propsRemove = emptyList(),
                                                    userPath = null
                                                )
                                            if (drawerViewModel != null) {
                                                drawerViewModel.updateDoc(patch)
                                            } else {
                                                drawerRepo.updateBatch(
                                                    listOf(UpdateDocArgs("main", null, patch))
                                                )
                                            }
                                        } catch (e: Exception) {
                                            println("Error updating value: $e")
                                        }
                                    }
                                }
                            } else {
                                null
                            },
                        config = rowConfig
                    )
                }
            }
        }
    }
    // Configuration bottom sheet
    if (showConfigBottomSheet != null) {
        val selectedKey = showConfigBottomSheet!!
        val config = keyConfigs[selectedKey]
        ModalBottomSheet(
            onDismissRequest = { showConfigBottomSheet = null },
            sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
        ) {
            PropertiesKeyConfigBottomSheet(
                key = selectedKey,
                currentConfig = config,
                onConfigChanged = { newConfig ->
                    scope.launch {
                        try {
                            configViewModel.configRepo.setPropDisplayHint(selectedKey, newConfig)
                            keyConfigs = keyConfigs + (selectedKey to newConfig)
                        } catch (e: Exception) {
                            println("Error updating key config: $e")
                        }
                    }
                },
                onDismiss = { showConfigBottomSheet = null }
            )
        }
    }

    // Add tag bottom sheet
    if (showAddTagBottomSheet) {
        ModalBottomSheet(
            onDismissRequest = { showAddTagBottomSheet = false },
            sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
        ) {
            AddTagBottomSheet(
                configViewModel = configViewModel,
                onTagAdded = { tagKey, value ->
                    val propsSet = mapOf(tagKey to value)
                    val patch =
                        DocPatch(
                            id = actualDoc.id,
                            propsSet = propsSet,
                            propsRemove = emptyList(),
                            userPath = null
                        )
                    if (drawerViewModel != null) {
                        drawerViewModel.updateDoc(patch)
                    } else {
                        scope.launch {
                            try {
                                drawerRepo.updateBatch(listOf(UpdateDocArgs("main", null, patch)))
                            } catch (e: Exception) {
                                println("Error adding tag: $e")
                            }
                        }
                    }
                    showAddTagBottomSheet = false
                },
                onDismiss = { showAddTagBottomSheet = false }
            )
        }
    }

    // Edit value bottom sheet
    if (showEditValueBottomSheet != null) {
        val row = showEditValueBottomSheet!!
        ModalBottomSheet(
            onDismissRequest = { showEditValueBottomSheet = null },
            sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
        ) {
            EditValueBottomSheet(
                row = row,
                doc = actualDoc,
                drawerRepo = drawerRepo,
                drawerViewModel = drawerViewModel,
                onDismiss = { showEditValueBottomSheet = null }
            )
        }
    }
}

@OptIn(kotlin.time.ExperimentalTime::class)
data class PropertiesRow(
    val key: String,
    val displayKey: String,
    val value: String,
    val error: String? = null,
    val instantValue: Instant? = null,
    val tagValue: String? = null, // JSON value
    val rawValue: String? = null,
    val propKey: DocPropKey? = null // The original DocPropKey if it's a property from the map
)

data class FormatResult(val formatted: String, val error: String? = null)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PropertiesTableRow(
    row: PropertiesRow,
    onKeyClick: () -> Unit,
    onValueClick: () -> Unit,
    onValueChange: ((String) -> Unit)? = null,
    config: PropKeyDisplayHint? = null
) {
    val isUnixPath = config?.deets is PropKeyDisplayDeets.UnixPath
    val canInlineEdit = isUnixPath && row.propKey != null && onValueChange != null

    Row(
        modifier =
            Modifier
                .fillMaxWidth()
                .padding(vertical = 4.dp, horizontal = 8.dp),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            text = row.displayKey,
            style = MaterialTheme.typography.bodySmall,
            modifier =
                Modifier
                    .weight(0.25f)
                    .clickable(onClick = onKeyClick),
            fontFamily = FontFamily.Monospace
        )
        Spacer(modifier = Modifier.width(16.dp))
        Row(
            modifier =
                Modifier
                    .weight(1f),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Use inline editor for UnixPath keys
            if (canInlineEdit) {
                val currentValue = row.tagValue?.let { dequoteJson(it) } ?: row.value
                var editedValue by remember(row.tagValue) { mutableStateOf(currentValue) }
                TextField(
                    value = editedValue,
                    onValueChange = { newValue ->
                        editedValue = newValue
                        onValueChange(newValue)
                    },
                    modifier = Modifier.weight(1f),
                    colors =
                        TextFieldDefaults.colors(
                            focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent
                        ),
                    textStyle = MaterialTheme.typography.bodySmall.copy(
                        fontFamily = FontFamily.Monospace
                    )
                )
            } else {
                Row(
                    modifier =
                        Modifier
                            .clickable(onClick = onValueClick),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = row.value,
                        style = MaterialTheme.typography.bodySmall,
                        fontFamily = FontFamily.Monospace
                    )
                    if (row.error != null) {
                        Spacer(modifier = Modifier.width(4.dp))
                        TooltipBox(
                            positionProvider = TooltipDefaults.rememberPlainTooltipPositionProvider(),
                            tooltip = {
                                PlainTooltip {
                                    Text(row.error ?: "")
                                }
                            },
                            state = rememberTooltipState()
                        ) {
                            Icon(
                                Icons.Default.Warning,
                                contentDescription = "Error",
                                tint = MaterialTheme.colorScheme.error,
                                modifier = Modifier.size(16.dp)
                            )
                        }
                    }
                }
            }
        }
    }
}

/**
 * Generic accordion composable with accessibility support.
 * Selected item is shown in its own card.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun <T> RadioAccordion(
    items: List<T>,
    selectedItem: T,
    onItemSelected: (T) -> Unit,
    itemLabel: (T) -> String,
    itemContent: @Composable (T) -> Unit,
    label: String? = null,
    modifier: Modifier = Modifier
) where T : Any {
    Column(modifier = modifier) {
        // Label for the radio set
        if (label != null) {
            Text(
                text = label,
                style = MaterialTheme.typography.labelLarge,
                modifier = Modifier.padding(bottom = 8.dp)
            )
        }

        items.forEachIndexed { index, item ->
            val isSelected = item == selectedItem

            if (isSelected) {
                // Selected item in its own card
                Card(
                    modifier =
                        Modifier
                            .fillMaxWidth()
                            .padding(vertical = 4.dp),
                    colors =
                        CardDefaults.cardColors(
                            containerColor = MaterialTheme.colorScheme.primaryContainer.copy(
                                alpha = 0.3f
                            )
                        )
                ) {
                    Column(modifier = Modifier.padding(8.dp)) {
                        // Row styled like ListItem
                        Row(
                            modifier =
                                Modifier
                                    .fillMaxWidth()
                                    .height(56.dp)
                                    .clickable { onItemSelected(item) }
                                    .semantics {
                                        role = Role.RadioButton
                                        selected = true
                                    },
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            RadioButton(
                                selected = true,
                                onClick = { onItemSelected(item) }
                            )
                            Text(
                                text = itemLabel(item),
                                style = MaterialTheme.typography.bodyLarge,
                                modifier = Modifier.padding(start = 8.dp)
                            )
                        }
                        // Content for selected item
                        AnimatedVisibility(visible = true) {
                            Column(modifier = Modifier.padding(start = 48.dp, top = 4.dp)) {
                                itemContent(item)
                            }
                        }
                    }
                }
            } else {
                // Unselected items - Row styled like ListItem
                Row(
                    modifier =
                        Modifier
                            .fillMaxWidth()
                            .height(56.dp)
                            .clickable { onItemSelected(item) }
                            .padding(horizontal = 16.dp)
                            .semantics {
                                role = Role.RadioButton
                                selected = false
                            },
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    RadioButton(
                        selected = false,
                        onClick = { onItemSelected(item) }
                    )
                    Text(
                        text = itemLabel(item),
                        style = MaterialTheme.typography.bodyLarge,
                        modifier = Modifier.padding(start = 8.dp)
                    )
                }
            }

            // Separator between items
            if (index < items.size - 1) {
                HorizontalDivider(modifier = Modifier.padding(vertical = 4.dp))
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PropertiesKeyConfigBottomSheet(
    key: String,
    currentConfig: PropKeyDisplayHint?,
    onConfigChanged: (PropKeyDisplayHint) -> Unit,
    onDismiss: () -> Unit
) {
    val defaultConfig =
        PropKeyDisplayHint(
            alwaysVisible = false,
            deets = PropKeyDisplayDeets.UnixPath,
            displayTitle = null
        )
    val config = currentConfig ?: defaultConfig

    var selectedDeets by remember(currentConfig) { mutableStateOf(config.deets) }
    var alwaysVisible by remember(currentConfig) { mutableStateOf(config.alwaysVisible) }
    var showTitleEditor by remember(currentConfig) {
        val deets = config.deets
        mutableStateOf(
            if (deets is PropKeyDisplayDeets.Title) deets.showEditor else false
        )
    }
    var selectedDateTimeConfig by remember(currentConfig) {
        mutableStateOf(
            when (val dt = config.deets) {
                is PropKeyDisplayDeets.DateTime -> dt.displayType
                else -> DateTimePropDisplayType.RELATIVE
            }
        )
    }

    Column(
        modifier =
            Modifier
                .fillMaxWidth()
                .padding(16.dp)
    ) {
        val displayTitle = currentConfig?.displayTitle ?: key
        Text(
            text = "Editing property: $displayTitle",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(bottom = 16.dp)
        )

        // Always visible toggle
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text("Always visible")
            Switch(
                checked = alwaysVisible,
                onCheckedChange = { alwaysVisible = it }
            )
        }

        // Use accordion for display type selection
        val displayTypeItems =
            listOf<PropKeyDisplayDeets>(
                PropKeyDisplayDeets.DateTime(displayType = selectedDateTimeConfig),
                PropKeyDisplayDeets.UnixPath,
                PropKeyDisplayDeets.Title(showEditor = showTitleEditor)
            )

        RadioAccordion(
            items = displayTypeItems,
            selectedItem = selectedDeets,
            onItemSelected = { item ->
                selectedDeets = item
                if (item is PropKeyDisplayDeets.DateTime) {
                    selectedDateTimeConfig = item.displayType
                }
                val localItem = item
                if (localItem is PropKeyDisplayDeets.Title) {
                    showTitleEditor = localItem.showEditor
                }
            },
            itemLabel = { item ->
                when (item) {
                    is PropKeyDisplayDeets.DateTime -> "DateTime"
                    is PropKeyDisplayDeets.UnixPath -> "UnixPath"
                    is PropKeyDisplayDeets.Title -> "Title"
                    is PropKeyDisplayDeets.DebugPrint -> "Debug"
                }
            },
            label = "Display Type",
            itemContent = { item ->
                when (item) {
                    is PropKeyDisplayDeets.DateTime -> {
                        // DateTime config options
                        Column {
                            Text(
                                text = "DateTime Format",
                                style = MaterialTheme.typography.labelLarge,
                                modifier = Modifier.padding(bottom = 8.dp)
                            )

                            val dateTimeConfigs =
                                listOf(
                                    DateTimePropDisplayType.RELATIVE to "Relative",
                                    DateTimePropDisplayType.TIME_ONLY to "Time Only",
                                    DateTimePropDisplayType.DATE_ONLY to "Date Only",
                                    DateTimePropDisplayType.TIME_AND_DATE to "Time and Date"
                                )

                            dateTimeConfigs.forEachIndexed { configIndex, (config, label) ->
                                Row(
                                    modifier =
                                        Modifier
                                            .fillMaxWidth()
                                            .height(56.dp)
                                            .clickable {
                                                selectedDateTimeConfig = config
                                                selectedDeets =
                                                    PropKeyDisplayDeets.DateTime(
                                                        displayType = config
                                                    )
                                            }.semantics {
                                                role = Role.RadioButton
                                                selected = selectedDateTimeConfig == config
                                            },
                                    verticalAlignment = Alignment.CenterVertically
                                ) {
                                    RadioButton(
                                        selected = selectedDateTimeConfig == config,
                                        onClick = {
                                            selectedDateTimeConfig = config
                                            selectedDeets =
                                                PropKeyDisplayDeets.DateTime(displayType = config)
                                        }
                                    )
                                    Text(
                                        text = label,
                                        style = MaterialTheme.typography.bodyLarge,
                                        modifier = Modifier.padding(start = 8.dp)
                                    )
                                }

                                if (configIndex < dateTimeConfigs.size - 1) {
                                    HorizontalDivider(modifier = Modifier.padding(vertical = 4.dp))
                                }
                            }
                        }
                    }

                    is PropKeyDisplayDeets.UnixPath -> {
                        // No additional config for UnixPath
                        Text(
                            "No additional configuration",
                            style = MaterialTheme.typography.bodySmall
                        )
                    }

                    is PropKeyDisplayDeets.Title -> {
                        // Title config options
                        Column {
                            if (key == "title_generic") {
                                Spacer(modifier = Modifier.height(8.dp))
                                Row(
                                    modifier = Modifier.fillMaxWidth(),
                                    horizontalArrangement = Arrangement.SpaceBetween,
                                    verticalAlignment = Alignment.CenterVertically
                                ) {
                                    Text(
                                        "Show title editor",
                                        modifier = Modifier.padding(start = 32.dp)
                                    )
                                    Switch(
                                        checked = showTitleEditor,
                                        onCheckedChange = {
                                            showTitleEditor = it
                                            selectedDeets =
                                                PropKeyDisplayDeets.Title(showEditor = it)
                                        }
                                    )
                                }
                            } else {
                                Text(
                                    "No additional configuration",
                                    style = MaterialTheme.typography.bodySmall,
                                    modifier = Modifier.padding(start = 32.dp)
                                )
                            }
                        }
                    }

                    else -> {}
                }
            }
        )

        Spacer(modifier = Modifier.height(16.dp))

        Button(
            onClick = {
                val newDeets =
                    when (selectedDeets) {
                        is PropKeyDisplayDeets.DateTime -> PropKeyDisplayDeets.DateTime(
                            displayType = selectedDateTimeConfig
                        )

                        is PropKeyDisplayDeets.Title -> PropKeyDisplayDeets.Title(
                            showEditor = showTitleEditor
                        )

                        else -> selectedDeets
                    }
                onConfigChanged(
                    PropKeyDisplayHint(
                        alwaysVisible = alwaysVisible,
                        deets = newDeets,
                        displayTitle = config.displayTitle
                    )
                )
                onDismiss()
            },
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Save")
        }
    }
}

@Composable
fun AddTagBottomSheet(
    configViewModel: ConfigViewModel,
    onTagAdded: (DocPropKey, String) -> Unit,
    onDismiss: () -> Unit
) {
    var searchQuery by remember { mutableStateOf("") }
    var expandedTagType by remember { mutableStateOf<String?>(null) }
    var pathValue by remember { mutableStateOf("") }
    var keyConfigs by remember { mutableStateOf<Map<String, PropKeyDisplayHint>>(emptyMap()) }

    // Load key configs
    LaunchedEffect(configViewModel) {
        try {
            keyConfigs = configViewModel.configRepo.listDisplayHints()
        } catch (e: Exception) {
            println("Error loading key configs: $e")
        }
    }

    val availableTagTypes = listOf("title_generic", "path_generic")
    val filteredTagTypes =
        availableTagTypes.filter {
            it.contains(searchQuery, ignoreCase = true)
        }

    Column(
        modifier =
            Modifier
                .fillMaxWidth()
                .padding(16.dp)
    ) {
        Text(
            text = "Add document tag",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(bottom = 16.dp)
        )

        // Search field
        OutlinedTextField(
            value = searchQuery,
            onValueChange = { searchQuery = it },
            label = { Text("Search tag types") },
            modifier = Modifier.fillMaxWidth()
        )

        Spacer(modifier = Modifier.height(16.dp))

        // Lazy list of tag types
        LazyColumn {
            items(filteredTagTypes) { tagType ->
                val config = keyConfigs[tagType]
                val displayTitle = config?.displayTitle ?: tagType
                var isExpanded by remember(tagType) { mutableStateOf(expandedTagType == tagType) }

                Column {
                    Row(
                        modifier =
                            Modifier
                                .fillMaxWidth()
                                .clickable {
                                    expandedTagType = if (isExpanded) null else tagType
                                    isExpanded = !isExpanded
                                }.padding(vertical = 8.dp),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(displayTitle)
                        Icon(
                            imageVector = if (isExpanded) Icons.Default.ExpandLess else Icons.Default.ExpandMore,
                            contentDescription = if (isExpanded) "Collapse" else "Expand"
                        )
                    }

                    AnimatedVisibility(visible = isExpanded) {
                        Column(modifier = Modifier.padding(start = 16.dp)) {
                            when (tagType) {
                                "path_generic" -> {
                                    OutlinedTextField(
                                        value = pathValue,
                                        onValueChange = { pathValue = it },
                                        label = { Text("Path") },
                                        modifier = Modifier.fillMaxWidth()
                                    )
                                    Spacer(modifier = Modifier.height(8.dp))
                                    Button(
                                        onClick = {
                                            onTagAdded(
                                                DocPropKey.Tag(
                                                    DocPropTag.WellKnown(
                                                        WellKnownPropTag.PATH_GENERIC
                                                    )
                                                ),
                                                "\"$pathValue\""
                                            )
                                            onDismiss()
                                        },
                                        enabled = pathValue.isNotBlank(),
                                        modifier = Modifier.fillMaxWidth()
                                    ) {
                                        Text("Add Tag")
                                    }
                                }

                                "title_generic" -> {
                                    // Could add similar UI for title_generic
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/**
 * DateTimeEditor composable with text field and date picker button in a row.
 * Supports only ISO 8601 date-time format and rejects bad input.
 */
@OptIn(ExperimentalMaterial3Api::class, kotlin.time.ExperimentalTime::class)
@Composable
fun DateTimeEditor(
    value: String,
    onValueChange: (String) -> Unit,
    error: String?,
    onDatePickerClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    Column(modifier = modifier) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            OutlinedTextField(
                value = value,
                onValueChange = { newValue ->
                    // Validate ISO 8601 format
                    val isoPattern =
                        Regex(
                            "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})?$"
                        )
                    if (newValue.isBlank() || isoPattern.matches(newValue)) {
                        onValueChange(newValue)
                    }
                },
                label = { Text("ISO 8601 DateTime") },
                isError = error != null,
                supportingText =
                    if (error != null) {
                        { Text(error) }
                    } else {
                        null
                    },
                modifier = Modifier.weight(1f)
            )
            Button(onClick = onDatePickerClick) {
                Text("Pick Date")
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class, kotlin.time.ExperimentalTime::class)
@Composable
fun EditValueBottomSheet(
    row: PropertiesRow,
    doc: Doc,
    drawerRepo: org.example.daybook.uniffi.DrawerRepoFfi,
    drawerViewModel: DrawerViewModel?,
    onDismiss: () -> Unit
) {
    val scope = rememberCoroutineScope()
    // Use raw value if available, otherwise convert instant to ISO format or use tag value
    var editedValue by remember {
        mutableStateOf(
            when {
                row.propKey != null -> {
                    row.tagValue?.let { dequoteJson(it) } ?: row.value
                }

                row.rawValue != null -> {
                    row.rawValue
                }

                row.instantValue != null -> {
                    // Convert to ISO 8601 format
                    val javaInstant = java.time.Instant.ofEpochSecond(row.instantValue.epochSeconds)
                    javaInstant.toString()
                }

                else -> {
                    row.value
                }
            }
        )
    }
    var showDatePicker by remember { mutableStateOf(false) }
    var isoDateTimeError by remember { mutableStateOf<String?>(null) }

    val datePickerState =
        rememberDatePickerState(
            initialSelectedDateMillis =
                row.instantValue?.let {
                    java.time.Instant
                        .ofEpochSecond(it.epochSeconds)
                        .toEpochMilli()
                }
        )

    Column(
        modifier =
            Modifier
                .fillMaxWidth()
                .padding(16.dp)
    ) {
        Text(
            text = "Edit: ${row.displayKey}",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(bottom = 16.dp)
        )

        when {
            row.instantValue != null -> {
                // Edit date/time
                Text("Editing: ${row.displayKey}")
                Spacer(modifier = Modifier.height(8.dp))

                // Use DateTimeEditor
                DateTimeEditor(
                    value = editedValue,
                    onValueChange = { newValue ->
                        editedValue = newValue
                        // Validate ISO format
                        val isoPattern =
                            Regex(
                                "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})?$"
                            )
                        isoDateTimeError =
                            if (newValue.isNotBlank() && !isoPattern.matches(newValue)) {
                                "Invalid ISO 8601 format. Expected: YYYY-MM-DDTHH:mm:ss[.sss][Z|±HH:mm]"
                            } else {
                                null
                            }
                    },
                    error = isoDateTimeError,
                    onDatePickerClick = { showDatePicker = true },
                    modifier = Modifier.fillMaxWidth()
                )

                if (showDatePicker) {
                    DatePickerDialog(
                        onDismissRequest = { showDatePicker = false },
                        confirmButton = {
                            TextButton(
                                onClick = {
                                    datePickerState.selectedDateMillis?.let { millis ->
                                        val javaInstant = java.time.Instant.ofEpochMilli(millis)
                                        editedValue = javaInstant.toString()
                                        isoDateTimeError = null
                                        showDatePicker = false
                                    }
                                }
                            ) {
                                Text("OK")
                            }
                        },
                        dismissButton = {
                            TextButton(onClick = { showDatePicker = false }) {
                                Text("Cancel")
                            }
                        }
                    ) {
                        DatePicker(state = datePickerState)
                    }
                }
            }

            row.propKey != null -> {
                // Edit tag value
                Text("Editing: ${row.displayKey}")
                Spacer(modifier = Modifier.height(8.dp))
                OutlinedTextField(
                    value = editedValue,
                    onValueChange = { editedValue = it },
                    label = { Text("Value") },
                    modifier = Modifier.fillMaxWidth()
                )
            }

            else -> {
                Text("Unable to edit this value")
            }
        }

        Spacer(modifier = Modifier.height(16.dp))

        Button(
            onClick = {
                scope.launch {
                    try {
                        when {
                            row.instantValue != null -> {
                                // Update timestamp - parse ISO 8601 format
                                try {
                                    val javaInstant = java.time.Instant.parse(editedValue)
                                    val newInstant = Instant.fromEpochSeconds(
                                        javaInstant.epochSecond
                                    )
                                    val patch =
                                        when (row.key) {
                                            "created_at" -> {
                                                DocPatch(
                                                    id = doc.id,
                                                    propsSet = emptyMap(),
                                                    propsRemove = emptyList(),
                                                    userPath = null
                                                    // createdAt = newInstant,
                                                )
                                            }

                                            "updated_at" -> {
                                                DocPatch(
                                                    id = doc.id,
                                                    propsSet = emptyMap(),
                                                    propsRemove = emptyList(),
                                                    userPath = null
                                                    // updatedAt = newInstant,
                                                )
                                            }

                                            else -> {
                                                null
                                            }
                                        }
                                    // FIXME: DocPatch no longer has createdAt/updatedAt fields in the generated FFI?
                                    // I should check DocPatch in daybook_types.kt
                                    if (patch != null) {
                                        drawerRepo.updateBatch(
                                            listOf(UpdateDocArgs("main", null, patch))
                                        )
                                    }
                                } catch (e: Exception) {
                                    println("Error parsing ISO datetime: $e")
                                }
                            }

                            row.propKey != null -> {
                                val patch =
                                    DocPatch(
                                        id = doc.id,
                                        propsSet = mapOf(row.propKey to "\"$editedValue\""),
                                        propsRemove = emptyList(),
                                        userPath = null
                                    )
                                if (drawerViewModel != null) {
                                    drawerViewModel.updateDoc(patch)
                                } else {
                                    drawerRepo.updateBatch(
                                        listOf(UpdateDocArgs("main", null, patch))
                                    )
                                }
                            }
                        }
                        onDismiss()
                    } catch (e: Exception) {
                        println("Error updating value: $e")
                    }
                }
            },
            enabled = isoDateTimeError == null || row.instantValue == null,
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Save")
        }
    }
}

@OptIn(kotlin.time.ExperimentalTime::class, ExperimentalMaterial3Api::class)
fun formatValue(value: Any, config: PropKeyDisplayHint?, key: String): FormatResult {
    val deets =
        config?.deets ?: when {
            key == "created_at" || key == "updated_at" -> PropKeyDisplayDeets.DateTime(
                displayType = DateTimePropDisplayType.RELATIVE
            )

            else -> PropKeyDisplayDeets.UnixPath
        }

    return when (deets) {
        is PropKeyDisplayDeets.Title -> {
            // Title display type - just return the string value
            val str = if (value is String) dequoteJson(value) else value.toString()
            FormatResult(str)
        }

        is PropKeyDisplayDeets.DateTime -> {
            val instant =
                when (value) {
                    is Instant -> {
                        value
                    }

                    is String -> {
                        // Try to parse from JSON string if it's a timestamp
                        null // FIXME: implement if needed
                    }

                    else -> {
                        null
                    }
                }
            if (instant != null) {
                try {
                    val formatted =
                        when (deets.displayType) {
                            DateTimePropDisplayType.RELATIVE -> formatRelativeTime(instant)
                            DateTimePropDisplayType.TIME_ONLY -> formatTimeOnly(instant)
                            DateTimePropDisplayType.DATE_ONLY -> formatDateOnly(instant)
                            DateTimePropDisplayType.TIME_AND_DATE -> formatTimeAndDate(instant)
                        }
                    FormatResult(formatted)
                } catch (e: Exception) {
                    FormatResult(
                        instant.epochSeconds.toString(),
                        "Unable to format as DateTime: ${e.message}"
                    )
                }
            } else {
                FormatResult(value.toString(), "Value is not a DateTime")
            }
        }

        is PropKeyDisplayDeets.UnixPath -> {
            val str = if (value is String) dequoteJson(value) else value.toString()
            FormatResult(str)
        }

        is PropKeyDisplayDeets.DebugPrint -> {
            FormatResult(value.toString())
        }
    }
}

@OptIn(kotlin.time.ExperimentalTime::class)
fun formatTimeAndDate(instant: Instant): String {
    val javaInstant = java.time.Instant.ofEpochSecond(instant.epochSeconds)
    val zonedDateTime = javaInstant.atZone(java.time.ZoneId.systemDefault())
    val formatter =
        java.time.format.DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm")
    return zonedDateTime.format(formatter)
}

@OptIn(kotlin.time.ExperimentalTime::class)
fun formatRelativeTime(instant: Instant): String {
    val nowInstant = Instant.fromEpochSeconds(Clock.System.now().epochSeconds)
    val duration = nowInstant - instant

    val seconds = duration.inWholeSeconds
    val minutes = duration.inWholeMinutes
    val hours = duration.inWholeHours
    val days = duration.inWholeDays

    return when {
        seconds < 60 -> "$seconds seconds ago"
        minutes < 60 -> "$minutes minutes ago"
        hours < 24 -> "$hours hours ago"
        days < 30 -> "$days days ago"
        days < 365 -> "${days / 30} months ago"
        else -> "${days / 365} years ago"
    }
}

@OptIn(kotlin.time.ExperimentalTime::class)
fun formatTimeOnly(instant: Instant): String {
    val javaInstant = java.time.Instant.ofEpochSecond(instant.epochSeconds)
    val localTime = javaInstant.atZone(java.time.ZoneId.systemDefault()).toLocalTime()
    return String.format("%02d:%02d:%02d", localTime.hour, localTime.minute, localTime.second)
}

@OptIn(kotlin.time.ExperimentalTime::class)
fun formatDateOnly(instant: Instant): String {
    val javaInstant = java.time.Instant.ofEpochSecond(instant.epochSeconds)
    val localDate = javaInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDate()
    return String.format(
        "%04d-%02d-%02d",
        localDate.year,
        localDate.monthValue,
        localDate.dayOfMonth
    )
}

fun getPropKeyString(key: DocPropKey): String = when (key) {
    is DocPropKey.Tag -> getPropTagString(key.v1)
    is DocPropKey.TagAndId -> "${getPropTagString(key.tag)}:${key.id}"
}

fun getPropTagString(tag: DocPropTag): String = when (tag) {
    is DocPropTag.WellKnown -> tag.v1.name.lowercase()
    is DocPropTag.Any -> tag.v1
}

fun dequoteJson(json: String): String {
    if (json.startsWith("\"") && json.endsWith("\"") && json.length >= 2) {
        return json
            .substring(1, json.length - 1)
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
    }
    return json
}

// Known editable tag types (only title_generic for now)
private val KNOWN_EDITABLE_TITLE_TAGS = setOf("title_generic")

data class TitleTagInfo(
    val propKey: DocPropKey,
    val value: String,
    val key: String,
    val isEditable: Boolean
)

fun findTitleTag(doc: Doc, keyConfigs: Map<String, PropKeyDisplayHint>): TitleTagInfo? {
    // Find all props that have display type of Title
    val titleTags = mutableListOf<TitleTagInfo>()

    doc.props.forEach { (propKey, value) ->
        val key = getPropKeyString(propKey)
        val config = keyConfigs[key]
        if (config?.deets is PropKeyDisplayDeets.Title) {
            val isEditable = key in KNOWN_EDITABLE_TITLE_TAGS
            titleTags.add(TitleTagInfo(propKey, value, key, isEditable))
        }
    }

    // Prioritize known editable tags
    val editableTag = titleTags.firstOrNull { it.isEditable }
    if (editableTag != null) {
        return editableTag
    }

    // Return first title tag if any found
    return titleTags.firstOrNull()
}
