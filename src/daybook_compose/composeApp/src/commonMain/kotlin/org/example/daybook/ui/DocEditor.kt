package org.example.daybook.ui

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.*
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.ui.text.input.VisualTransformation
import kotlinx.coroutines.launch
import kotlin.time.Instant
import kotlin.time.Clock
import org.example.daybook.LocalContainer
import org.example.daybook.ConfigViewModel
import org.example.daybook.DrawerViewModel
import androidx.lifecycle.viewmodel.compose.viewModel
import org.example.daybook.uniffi.core.Doc
import org.example.daybook.uniffi.core.DocContent
import org.example.daybook.uniffi.core.DocProp
import org.example.daybook.uniffi.core.DocPatch
import org.example.daybook.uniffi.core.DateTimeDisplayType
import org.example.daybook.uniffi.core.MetaTableKeyDisplayType
import org.example.daybook.uniffi.core.MetaTableKeyConfig
import org.example.daybook.uniffi.MetaTableKeyConfigEntry

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
    val actualConfigViewModel = configViewModel ?: run {
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
            val showTitleEditor = titleConfig?.showTitleEditor ?: false
            
            if (showTitleEditor) {
                val titleText = when (val tag = titleTagInfo?.tag) {
                    is DocProp.TitleGeneric -> tag.v1
                    else -> ""
                }
                // Allow editing if we have an editable title tag, or if we can create a title_generic tag
                val isEditable = titleTagInfo?.isEditable ?: ("title_generic" in KNOWN_EDITABLE_TITLE_TAGS)
                
                TitleEditor(
                    title = titleText,
                    enabled = isEditable,
                    onTitleChange = { newTitle ->
                        if (currentDoc == null) return@TitleEditor
                        
                        scope.launch {
                            try {
                                val currentTags = currentDoc.props.toMutableList()
                                val tagIndex = titleTagInfo?.index
                                
                                when {
                                    newTitle.isNotBlank() -> {
                                        // Update existing tag or add new one
                                        if (tagIndex != null && tagIndex in currentTags.indices) {
                                            currentTags[tagIndex] = DocProp.TitleGeneric(newTitle)
                                        } else {
                                            currentTags.add(DocProp.TitleGeneric(newTitle))
                                        }
                                    }
                                    tagIndex != null && tagIndex in currentTags.indices -> {
                                        // Remove tag if text is blank
                                        currentTags.removeAt(tagIndex)
                                    }
                                    else -> return@launch // No change needed
                                }
                                
                                val patch = DocPatch(
                                    id = currentDoc.id,
                                    createdAt = null,
                                    updatedAt = null,
                                    content = null,
                                    props = currentTags
                                )
                                
                                if (actualDrawerViewModel != null) {
                                    actualDrawerViewModel.updateDoc(patch)
                                } else {
                                    actualDrawerRepo.updateBatch(listOf(patch))
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
            val content = doc?.content ?: DocContent.Text("")
    
            when (content) {
                is DocContent.Text -> {
                    var text by remember(doc?.id) { 
                        mutableStateOf(content.v1)
                    }

                    // Sync external changes
                    LaunchedEffect(doc) {
                        val externalText = (doc?.content as? DocContent.Text)?.v1 ?: ""
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
                        colors = TextFieldDefaults.colors(
                            focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                            focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                            unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                        )
                    )
                }
                is DocContent.Blob -> {
                    if (blobsRepo != null) {
                        val hashStr = content.v1.hash
                        
                        var imagePath by remember(hashStr) { mutableStateOf<String?>(null) }
                        
                        LaunchedEffect(hashStr) {
                            try {
                                imagePath = blobsRepo.getPath(hashStr)
                            } catch (e: Exception) {
                                println("Error getting blob path: $e")
                            }
                        }
                        
                        if (imagePath != null) {
                            coil3.compose.AsyncImage(
                                model = java.io.File(imagePath!!),
                                contentDescription = "Blob Image",
                                modifier = Modifier.fillMaxSize(),
                                contentScale = androidx.compose.ui.layout.ContentScale.Fit
                            )
                        } else {
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                CircularProgressIndicator()
                            }
                        }
                    } else {
                        Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                            Text("Image unavailable (Repo missing)")
                        }
                    }
                }
                else -> {
                    Column(
                        modifier = Modifier.fillMaxSize().padding(16.dp),
                        horizontalAlignment = Alignment.CenterHorizontally,
                        verticalArrangement = Arrangement.Center
                    ) {
                        Text("Unsupported document content", style = MaterialTheme.typography.headlineSmall)
                        Spacer(modifier = Modifier.height(8.dp))
                        Text("Type: ${content::class.simpleName}", style = MaterialTheme.typography.bodyMedium)
                        println("Unsupported doc content found: $content")
                    }
                }
            }
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
                text = "Title",
                // color = MaterialTheme.colorScheme.onSurfaceVariant,
                // fontSize = MaterialTheme.typography.headlineLarge.fontSize,
            ) 
        },
        // label = {
        //     Text("Title")
        // },
        textStyle = MaterialTheme.typography.headlineLarge.copy(
            fontWeight = androidx.compose.ui.text.font.FontWeight.Bold
        ),
        colors = TextFieldDefaults.colors(
            focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
            focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
            unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
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
    var keyConfigs by remember { mutableStateOf<Map<String, MetaTableKeyConfig>>(emptyMap()) }
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
            val configs = configViewModel.configRepo.getMetaTableKeyConfigs()
            keyConfigs = configs.associate { it.key to it.config }
        } catch (e: Exception) {
            println("Error loading key configs: $e")
        }
    }
    
    // Build properties rows with formatting
    // Use actualDoc.id and actualDoc.props as keys to ensure recomposition when props change
    // Note: We need to track props changes explicitly since remember uses reference equality for lists
    val tagsKey = remember(actualDoc.props) { 
        // Create a stable key from props that changes when props change
        actualDoc.props.joinToString("|") { tag ->
            when (tag) {
                is DocProp.TitleGeneric -> "title:${tag.v1}"
                is DocProp.PathGeneric -> "path:${tag.v1}"
                is DocProp.LabelGeneric -> "label:${tag.v1}"
                is DocProp.RefGeneric -> "ref:${tag.v1}"
                is DocProp.ImageMetadata -> "image:${tag.v1.mime}-${tag.v1.widthPx}-${tag.v1.heightPx}"
                is DocProp.PseudoLabel -> "pseudo:${tag.v1.joinToString(",")}"
            }
        }
    }
    
    @OptIn(kotlin.time.ExperimentalTime::class)
    val propertiesRows = remember(actualDoc.id, tagsKey, keyConfigs) {
        val rows = mutableListOf<PropertiesRow>()
        
        // Timestamps
        val createdAtConfig = keyConfigs["created_at"]
        val createdAtDisplayKey = createdAtConfig?.displayTitle ?: "created_at"
        val createdAtResult = formatValue(actualDoc.createdAt, createdAtConfig, "created_at")
        val createdAtIso = java.time.Instant.ofEpochSecond(actualDoc.createdAt.epochSeconds).toString()
        rows.add(PropertiesRow("created_at", createdAtDisplayKey, createdAtResult.formatted, createdAtResult.error, actualDoc.createdAt, null, createdAtIso))
        
        val updatedAtConfig = keyConfigs["updated_at"]
        val updatedAtDisplayKey = updatedAtConfig?.displayTitle ?: "updated_at"
        val updatedAtResult = formatValue(actualDoc.updatedAt, updatedAtConfig, "updated_at")
        val updatedAtIso = java.time.Instant.ofEpochSecond(actualDoc.updatedAt.epochSeconds).toString()
        rows.add(PropertiesRow("updated_at", updatedAtDisplayKey, updatedAtResult.formatted, updatedAtResult.error, actualDoc.updatedAt, null, updatedAtIso))
        
        // Tags
        actualDoc.props.forEachIndexed { index, tag ->
            val key = when (tag) {
                is DocProp.TitleGeneric -> "title_generic"
                is DocProp.PathGeneric -> "path_generic"
                else -> "tag_$index"
            }
            val tagConfig = keyConfigs[key]
            val tagDisplayKey = tagConfig?.displayTitle ?: getTagKind(tag)
            val tagValue = getTagValue(tag)
            val tagResult = formatValue(tagValue, tagConfig, key, tag)
            rows.add(PropertiesRow(key, tagDisplayKey, tagResult.formatted, tagResult.error, null, tag, null, index))
        }
        rows
    }
    
    val alwaysVisibleRows = propertiesRows.filter { 
        val config = keyConfigs[it.key]
        config?.alwaysVisible ?: (it.key == "created_at" || it.key == "updated_at")
    }
    val collapsibleRows = propertiesRows.filter { 
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
                modifier = Modifier
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
            val isUnixPath = rowConfig?.displayType is MetaTableKeyDisplayType.UnixPath
            PropertiesTableRow(
                row = row,
                onKeyClick = { showConfigBottomSheet = row.key },
                onValueClick = { showEditValueBottomSheet = row },
                onValueChange = if (isUnixPath && row.tagValue != null && row.tagIndex != null) { newValue ->
                    val currentTags = actualDoc.props.toMutableList()
                    val tagIndex = row.tagIndex
                    if (tagIndex >= 0 && tagIndex < currentTags.size) {
                        when (row.tagValue) {
                            is DocProp.PathGeneric -> {
                                currentTags[tagIndex] = DocProp.PathGeneric(newValue)
                            }
                            is DocProp.TitleGeneric -> {
                                currentTags[tagIndex] = DocProp.TitleGeneric(newValue)
                            }
                            is DocProp.LabelGeneric -> {
                                currentTags[tagIndex] = DocProp.LabelGeneric(newValue)
                            }
                            else -> {}
                        }
                    }
                    val patch = DocPatch(
                        id = actualDoc.id,
                        createdAt = null,
                        updatedAt = null,
                        content = null,
                        props = currentTags
                    )
                    if (drawerViewModel != null) {
                        drawerViewModel.updateDoc(patch)
                    } else {
                        scope.launch {
                            try {
                                drawerRepo.updateBatch(listOf(patch))
                            } catch (e: Exception) {
                                println("Error updating value: $e")
                            }
                        }
                    }
                } else null,
                config = rowConfig
            )
        }
        
        // Collapsible rows
        AnimatedVisibility(visible = isExpanded) {
            Column {
                collapsibleRows.forEach { row ->
                    val rowConfig = keyConfigs[row.key]
                    val isUnixPath = rowConfig?.displayType is MetaTableKeyDisplayType.UnixPath
                    PropertiesTableRow(
                        row = row,
                        onKeyClick = { showConfigBottomSheet = row.key },
                        onValueClick = { showEditValueBottomSheet = row },
                        onValueChange = if (isUnixPath && row.tagValue != null && row.tagIndex != null) { newValue ->
                            scope.launch {
                                try {
                                    val currentTags = actualDoc.props.toMutableList()
                                    val tagIndex = row.tagIndex
                                    if (tagIndex >= 0 && tagIndex < currentTags.size) {
                                        when (row.tagValue) {
                                            is DocProp.PathGeneric -> {
                                                currentTags[tagIndex] = DocProp.PathGeneric(newValue)
                                            }
                                            is DocProp.TitleGeneric -> {
                                                currentTags[tagIndex] = DocProp.TitleGeneric(newValue)
                                            }
                                            is DocProp.LabelGeneric -> {
                                                currentTags[tagIndex] = DocProp.LabelGeneric(newValue)
                                            }
                                            else -> {}
                                        }
                                    }
                                    val patch = DocPatch(
                                        id = actualDoc.id,
                                        createdAt = null,
                                        updatedAt = null,
                                        content = null,
                                        props = currentTags
                                    )
                                    if (drawerViewModel != null) {
                                        drawerViewModel.updateDoc(patch)
                                    } else {
                                        drawerRepo.updateBatch(listOf(patch))
                                    }
                                } catch (e: Exception) {
                                    println("Error updating value: $e")
                                }
                            }
                        } else null,
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
                            configViewModel.configRepo.setMetaTableKeyConfig(selectedKey, newConfig)
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
                onTagAdded = { tag ->
                    val currentTags = actualDoc.props.toMutableList()
                    currentTags.add(tag)
                    val patch = DocPatch(
                        id = actualDoc.id,
                        createdAt = null,
                        updatedAt = null,
                        content = null,
                        props = currentTags
                    )
                    if (drawerViewModel != null) {
                        drawerViewModel.updateDoc(patch)
                    } else {
                        scope.launch {
                            try {
                                drawerRepo.updateBatch(listOf(patch))
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
    val tagValue: DocProp? = null,
    val rawValue: String? = null,
    val tagIndex: Int? = null  // Index of the tag in the document's tags list
)

data class FormatResult(
    val formatted: String,
    val error: String? = null
)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PropertiesTableRow(
    row: PropertiesRow,
    onKeyClick: () -> Unit,
    onValueClick: () -> Unit,
    onValueChange: ((String) -> Unit)? = null,
    config: MetaTableKeyConfig? = null
) {
    val isUnixPath = config?.displayType is MetaTableKeyDisplayType.UnixPath
    val canInlineEdit = isUnixPath && row.tagValue != null && onValueChange != null
    
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp, horizontal = 8.dp),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            text = row.displayKey,
            style = MaterialTheme.typography.bodySmall,
            modifier = Modifier
                .weight(0.25f)
                .clickable(onClick = onKeyClick),
            fontFamily = FontFamily.Monospace
        )
        Spacer(modifier = Modifier.width(16.dp))
        Row(
            modifier = Modifier
                .weight(1f),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            // Use inline editor for UnixPath keys
            if (canInlineEdit) {
                val currentValue = when (row.tagValue) {
                    is DocProp.PathGeneric -> row.tagValue.v1
                    is DocProp.TitleGeneric -> row.tagValue.v1
                    is DocProp.LabelGeneric -> row.tagValue.v1
                    else -> row.value
                }
                var editedValue by remember(row.tagValue) { mutableStateOf(currentValue) }
                TextField(
                    value = editedValue,
                    onValueChange = { newValue ->
                        editedValue = newValue
                        onValueChange(newValue)
                    },
                    modifier = Modifier.weight(1f),
                    colors = TextFieldDefaults.colors(
                        focusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        disabledContainerColor = androidx.compose.ui.graphics.Color.Transparent,
                        focusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                        unfocusedIndicatorColor = androidx.compose.ui.graphics.Color.Transparent,
                    ),
                    textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace)
                )
            } else {
                Row(
                    modifier = Modifier
                        .clickable(onClick = onValueClick),
                    verticalAlignment = Alignment.CenterVertically,
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
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 4.dp),
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.primaryContainer.copy(alpha = 0.3f)
                    )
                ) {
                    Column(modifier = Modifier.padding(8.dp)) {
                        // Row styled like ListItem
                        Row(
                            modifier = Modifier
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
                    modifier = Modifier
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
    currentConfig: MetaTableKeyConfig?,
    onConfigChanged: (MetaTableKeyConfig) -> Unit,
    onDismiss: () -> Unit
) {
    val defaultConfig = MetaTableKeyConfig(
        alwaysVisible = false,
        displayType = MetaTableKeyDisplayType.UnixPath,
        displayTitle = null,
        showTitleEditor = null
    )
    val config = currentConfig ?: defaultConfig
    
    var selectedDisplayType by remember(currentConfig) { mutableStateOf(config.displayType) }
    var alwaysVisible by remember(currentConfig) { mutableStateOf(config.alwaysVisible) }
    var showTitleEditor by remember(currentConfig) { mutableStateOf(config.showTitleEditor ?: false) }
    var selectedDateTimeConfig by remember(currentConfig) { 
        mutableStateOf(
            when (val dt = config.displayType) {
                is MetaTableKeyDisplayType.DateTime -> dt.displayType
                else -> DateTimeDisplayType.RELATIVE
            }
        )
    }
    
    Column(
        modifier = Modifier
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
        
        Spacer(modifier = Modifier.height(16.dp))
        
        // Use accordion for display type selection
        val displayTypeItems = listOf<MetaTableKeyDisplayType>(
            MetaTableKeyDisplayType.DateTime(displayType = selectedDateTimeConfig),
            MetaTableKeyDisplayType.UnixPath,
            MetaTableKeyDisplayType.Title,
        )
        
        RadioAccordion(
            items = displayTypeItems,
            selectedItem = selectedDisplayType,
            onItemSelected = { item ->
                selectedDisplayType = item
                if (item is MetaTableKeyDisplayType.DateTime) {
                    selectedDateTimeConfig = item.displayType
                }
            },
            itemLabel = { item ->
                when (item) {
                    is MetaTableKeyDisplayType.DateTime -> "DateTime"
                    is MetaTableKeyDisplayType.UnixPath -> "UnixPath"
                    is MetaTableKeyDisplayType.Title -> "Title"
                    else -> "Unknown"
                }
            },
            label = "Display Type",
            itemContent = { item ->
                when (item) {
                    is MetaTableKeyDisplayType.DateTime -> {
                        // DateTime config options
                        Column {
                            Text(
                                text = "DateTime Format",
                                style = MaterialTheme.typography.labelLarge,
                                modifier = Modifier.padding(bottom = 8.dp)
                            )
                            
                            val dateTimeConfigs = listOf(
                                DateTimeDisplayType.RELATIVE to "Relative",
                                DateTimeDisplayType.TIME_ONLY to "Time Only",
                                DateTimeDisplayType.DATE_ONLY to "Date Only",
                                DateTimeDisplayType.TIME_AND_DATE to "Time and Date"
                            )
                            
                            dateTimeConfigs.forEachIndexed { configIndex, (config, label) ->
                                Row(
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .height(56.dp)
                                        .clickable {
                                            selectedDateTimeConfig = config
                                            selectedDisplayType = MetaTableKeyDisplayType.DateTime(displayType = config)
                                        }
                                        .semantics {
                                            role = Role.RadioButton
                                            selected = selectedDateTimeConfig == config
                                        },
                                    verticalAlignment = Alignment.CenterVertically
                                ) {
                                    RadioButton(
                                        selected = selectedDateTimeConfig == config,
                                        onClick = {
                                            selectedDateTimeConfig = config
                                            selectedDisplayType = MetaTableKeyDisplayType.DateTime(displayType = config)
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
                    is MetaTableKeyDisplayType.UnixPath -> {
                        // No additional config for UnixPath
                        Text("No additional configuration", style = MaterialTheme.typography.bodySmall)
                    }
                    is MetaTableKeyDisplayType.Title -> {
                        // Title config options
                        Column {
                            if (key == "title_generic") {
                                Spacer(modifier = Modifier.height(8.dp))
                                Row(
                                    modifier = Modifier.fillMaxWidth(),
                                    horizontalArrangement = Arrangement.SpaceBetween,
                                    verticalAlignment = Alignment.CenterVertically
                                ) {
                                    Text("Show title editor", modifier = Modifier.padding(start = 32.dp))
                                    Switch(
                                        checked = showTitleEditor,
                                        onCheckedChange = { showTitleEditor = it }
                                    )
                                }
                            } else {
                                Text("No additional configuration", style = MaterialTheme.typography.bodySmall, modifier = Modifier.padding(start = 32.dp))
                            }
                        }
                    }
                }
            }
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Button(
            onClick = {
                val newDisplayType = if (selectedDisplayType is MetaTableKeyDisplayType.DateTime) {
                    MetaTableKeyDisplayType.DateTime(displayType = selectedDateTimeConfig)
                } else {
                    selectedDisplayType
                }
                onConfigChanged(MetaTableKeyConfig(
                    alwaysVisible = alwaysVisible, 
                    displayType = newDisplayType, 
                    displayTitle = config.displayTitle,
                    showTitleEditor = if (key == "title_generic") showTitleEditor else config.showTitleEditor
                ))
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
    onTagAdded: (DocProp) -> Unit,
    onDismiss: () -> Unit
) {
    var searchQuery by remember { mutableStateOf("") }
    var expandedTagType by remember { mutableStateOf<String?>(null) }
    var pathValue by remember { mutableStateOf("") }
    var titleValue by remember { mutableStateOf("") }
    var keyConfigs by remember { mutableStateOf<Map<String, MetaTableKeyConfig>>(emptyMap()) }
    
    // Load key configs
    LaunchedEffect(configViewModel) {
        try {
            val configs = configViewModel.configRepo.getMetaTableKeyConfigs()
            keyConfigs = configs.associate { it.key to it.config }
        } catch (e: Exception) {
            println("Error loading key configs: $e")
        }
    }
    
    val availableTagTypes = listOf("title_generic", "path_generic")
    val filteredTagTypes = availableTagTypes.filter { 
        it.contains(searchQuery, ignoreCase = true) 
    }
    
    Column(
        modifier = Modifier
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
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable { 
                                expandedTagType = if (isExpanded) null else tagType
                                isExpanded = !isExpanded
                            }
                            .padding(vertical = 8.dp),
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
                                            // Note: PathGeneric doesn't exist yet in DocProp, 
                                            // this is a placeholder for when it's added
                                            // For now, we'll use LabelGeneric as a workaround
                                            onTagAdded(DocProp.PathGeneric(pathValue))
                                            onDismiss()
                                        },
                                        enabled = pathValue.isNotBlank(),
                                        modifier = Modifier.fillMaxWidth()
                                    ) {
                                        Text("Add Tag")
                                    }
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
                    val isoPattern = Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})?$")
                    if (newValue.isBlank() || isoPattern.matches(newValue)) {
                        onValueChange(newValue)
                    }
                },
                label = { Text("ISO 8601 DateTime") },
                isError = error != null,
                supportingText = if (error != null) {
                    { Text(error) }
                } else null,
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
                row.tagValue != null -> {
                    when (row.tagValue) {
                        is DocProp.TitleGeneric -> row.tagValue.v1
                        is DocProp.PathGeneric -> row.tagValue.v1
                        is DocProp.LabelGeneric -> row.tagValue.v1
                        else -> row.value
                    }
                }
                row.rawValue != null -> row.rawValue
                row.instantValue != null -> {
                    // Convert to ISO 8601 format
                    val javaInstant = java.time.Instant.ofEpochSecond(row.instantValue.epochSeconds)
                    javaInstant.toString()
                }
                else -> row.value
            }
        )
    }
    var showDatePicker by remember { mutableStateOf(false) }
    var isoDateTimeError by remember { mutableStateOf<String?>(null) }
    
    val datePickerState = rememberDatePickerState(
        initialSelectedDateMillis = row.instantValue?.let {
            java.time.Instant.ofEpochSecond(it.epochSeconds).toEpochMilli()
        }
    )
    
    Column(
        modifier = Modifier
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
                        val isoPattern = Regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})?$")
                        isoDateTimeError = if (newValue.isNotBlank() && !isoPattern.matches(newValue)) {
                            "Invalid ISO 8601 format. Expected: YYYY-MM-DDTHH:mm:ss[.sss][Z|±HH:mm]"
                        } else null
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
            row.tagValue != null -> {
                // Edit tag value
                Text("Editing: ${row.displayKey}")
                Spacer(modifier = Modifier.height(8.dp))
                when (row.tagValue) {
                    is DocProp.TitleGeneric -> {
                        OutlinedTextField(
                            value = editedValue,
                            onValueChange = { editedValue = it },
                            label = { Text("Title") },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                    is DocProp.PathGeneric -> {
                        OutlinedTextField(
                            value = editedValue,
                            onValueChange = { editedValue = it },
                            label = { Text("Path") },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                    is DocProp.LabelGeneric -> {
                        OutlinedTextField(
                            value = editedValue,
                            onValueChange = { editedValue = it },
                            label = { Text("Path") },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                    else -> {
                        Text("Editing this tag type is not yet supported")
                    }
                }
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
                                    val newInstant = Instant.fromEpochSeconds(javaInstant.epochSecond)
                                    val patch = when (row.key) {
                                        "created_at" -> DocPatch(
                                            id = doc.id,
                                            createdAt = newInstant,
                                            updatedAt = null,
                                            content = null,
                                            props = null
                                        )
                                        "updated_at" -> DocPatch(
                                            id = doc.id,
                                            createdAt = null,
                                            updatedAt = newInstant,
                                            content = null,
                                            props = null
                                        )
                                        else -> null
                                    }
                                    if (patch != null) {
                                        drawerRepo.updateBatch(listOf(patch))
                                    }
                                } catch (e: Exception) {
                                    println("Error parsing ISO datetime: $e")
                                }
                            }
                            row.tagValue != null -> {
                                // Update tag using index from row
                                val tagIndex = row.tagIndex ?: row.key.removePrefix("tag_").toIntOrNull()
                                if (tagIndex != null && tagIndex >= 0 && tagIndex < doc.props.size) {
                                    val currentTags = doc.props.toMutableList()
                                    when (row.tagValue) {
                                        is DocProp.LabelGeneric -> {
                                            currentTags[tagIndex] = DocProp.LabelGeneric(editedValue)
                                        }
                                        is DocProp.TitleGeneric -> {
                                            currentTags[tagIndex] = DocProp.TitleGeneric(editedValue)
                                        }
                                        is DocProp.PathGeneric -> {
                                            currentTags[tagIndex] = DocProp.PathGeneric(editedValue)
                                        }
                                        else -> {}
                                    }
                                    if (drawerViewModel != null) {
                                        drawerViewModel.updateDoc(DocPatch(
                                            id = doc.id,
                                            createdAt = null,
                                            updatedAt = null,
                                            content = null,
                                            props = currentTags
                                        ))
                                    } else {
                                        drawerRepo.updateBatch(listOf(DocPatch(
                                            id = doc.id,
                                            createdAt = null,
                                            updatedAt = null,
                                            content = null,
                                            props = currentTags
                                        )))
                                    }
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
fun formatValue(
    value: Any,
    config: MetaTableKeyConfig?,
    key: String,
    tag: DocProp? = null
): FormatResult {
    val displayType = config?.displayType ?: when {
        key == "created_at" || key == "updated_at" -> MetaTableKeyDisplayType.DateTime(displayType = DateTimeDisplayType.RELATIVE)
        tag != null && getTagKind(tag) == "PathGeneric" -> MetaTableKeyDisplayType.UnixPath
        else -> MetaTableKeyDisplayType.UnixPath
    }
    
    return when (displayType) {
        is MetaTableKeyDisplayType.Title -> {
            // Title display type - just return the string value
            when (value) {
                is String -> FormatResult(value)
                else -> FormatResult(value.toString(), "Title display type only supports String values")
            }
        }
        is MetaTableKeyDisplayType.DateTime -> {
            when (value) {
                is Instant -> {
                    try {
                        val formatted = when (displayType.displayType) {
                            DateTimeDisplayType.RELATIVE -> formatRelativeTime(value)
                            DateTimeDisplayType.TIME_ONLY -> formatTimeOnly(value)
                            DateTimeDisplayType.DATE_ONLY -> formatDateOnly(value)
                            DateTimeDisplayType.TIME_AND_DATE -> formatTimeAndDate(value)
                        }
                        FormatResult(formatted)
                    } catch (e: Exception) {
                        FormatResult(value.epochSeconds.toString(), "Unable to format as DateTime: ${e.message}")
                    }
                }
                else -> FormatResult(value.toString(), "Value is not a DateTime")
            }
        }
        is MetaTableKeyDisplayType.UnixPath -> {
            when (value) {
                is String -> FormatResult(value)
                else -> FormatResult(value.toString(), "Value is not a UnixPath")
            }
        }
    }
}

@OptIn(kotlin.time.ExperimentalTime::class)
fun formatTimeAndDate(instant: Instant): String {
    val javaInstant = java.time.Instant.ofEpochSecond(instant.epochSeconds)
    val zonedDateTime = javaInstant.atZone(java.time.ZoneId.systemDefault())
    val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
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
    return String.format("%04d-%02d-%02d", localDate.year, localDate.monthValue, localDate.dayOfMonth)
}

fun getTagValue(tag: DocProp): String {
    return when (tag) {
        is DocProp.RefGeneric -> tag.v1
        is DocProp.LabelGeneric -> tag.v1
        is DocProp.TitleGeneric -> tag.v1
        is DocProp.PathGeneric -> tag.v1
        is DocProp.ImageMetadata -> tag.v1.toString()
        is DocProp.PseudoLabel -> tag.v1.joinToString(", ")
    }
}

fun getTagKind(tag: DocProp): String {
    return when (tag) {
        is DocProp.RefGeneric -> "RefGeneric"
        is DocProp.LabelGeneric -> "LabelGeneric"
        is DocProp.ImageMetadata -> "ImageMetadata"
        is DocProp.PseudoLabel -> "PseudoLabel"
        is DocProp.TitleGeneric -> "TitleGeneric"
        is DocProp.PathGeneric -> "PathGeneric"
    }
}

// Known editable tag types (only title_generic for now)
private val KNOWN_EDITABLE_TITLE_TAGS = setOf("title_generic")

data class TitleTagInfo(
    val tag: DocProp,
    val key: String,
    val isEditable: Boolean,
    val index: Int
)

fun findTitleTag(doc: Doc, keyConfigs: Map<String, MetaTableKeyConfig>): TitleTagInfo? {
    // Find all props that have display type of Title
    val titleTags = mutableListOf<TitleTagInfo>()
    
    doc.props.forEachIndexed { index, tag ->
        val key = when (tag) {
            is DocProp.TitleGeneric -> "title_generic"
            is DocProp.PathGeneric -> "path_generic"
            else -> "tag_$index"
        }
        val config = keyConfigs[key]
        if (config?.displayType is MetaTableKeyDisplayType.Title) {
            val isEditable = key in KNOWN_EDITABLE_TITLE_TAGS
            titleTags.add(TitleTagInfo(tag, key, isEditable, index))
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

