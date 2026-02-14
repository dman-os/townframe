package org.example.daybook.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch
import org.example.daybook.ConfigViewModel
import org.example.daybook.DrawerViewModel
import org.example.daybook.LocalContainer
import org.example.daybook.uniffi.core.FacetKeyDisplayDeets
import org.example.daybook.uniffi.core.UpdateDocArgsV2
import org.example.daybook.uniffi.types.Doc
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.WellKnownFacetTag

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
    blobsRepo

    val actualConfigViewModel = configViewModel ?: ConfigViewModel(LocalContainer.current.configRepo)
    val actualDrawerRepo = drawerRepo ?: LocalContainer.current.drawerRepo
    val actualDrawerViewModel = drawerViewModel

    val drawerDocState = actualDrawerViewModel?.selectedDoc
    val drawerDoc by drawerDocState?.collectAsState() ?: remember(doc) { mutableStateOf(doc) }
    val currentDoc = drawerDoc ?: doc

    if (currentDoc == null) {
        Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            Text("No document selected")
        }
        return
    }

    val keyConfigs by actualConfigViewModel.metaTableKeyConfigs.collectAsState()
    val titleHint = keyConfigs["title_generic"]
    val showTitleEditor =
        when (val deets = titleHint?.deets) {
            is FacetKeyDisplayDeets.Title -> deets.showEditor
            else -> true
        }

    val scope = rememberCoroutineScope()

    Column(modifier = modifier) {
        if (showTitleEditor) {
            TitleEditor(
                title = currentDoc.facets[titleFacetKey()]?.let { dequoteJson(it) } ?: "",
                onTitleChange = { newTitle ->
                    scope.launch {
                        val facetsSet = mutableMapOf<FacetKey, String>()
                        val facetsRemove = mutableListOf<FacetKey>()
                        if (newTitle.isBlank()) {
                            facetsRemove.add(titleFacetKey())
                        } else {
                            facetsSet[titleFacetKey()] = quoteJsonString(newTitle)
                        }
                        val patch =
                            DocPatch(
                                id = currentDoc.id,
                                facetsSet = facetsSet,
                                facetsRemove = facetsRemove,
                                userPath = null
                            )
                        if (actualDrawerViewModel != null) {
                            actualDrawerViewModel.updateDoc(patch)
                        } else {
                            actualDrawerRepo.updateBatch(listOf(UpdateDocArgsV2("main", null, patch)))
                        }
                    }
                },
                modifier = Modifier.fillMaxWidth()
            )
            HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
        }

        var text by remember(currentDoc.id) {
            mutableStateOf(noteContentFromFacetJson(currentDoc.facets[noteFacetKey()]))
        }

        LaunchedEffect(currentDoc) {
            val externalText = noteContentFromFacetJson(currentDoc.facets[noteFacetKey()])
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
            modifier = Modifier.fillMaxWidth().weight(1f),
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

        HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))

        Text(
            text = "Facets",
            style = MaterialTheme.typography.titleSmall,
            modifier = Modifier.padding(bottom = 8.dp)
        )

        LazyColumn(modifier = Modifier.fillMaxWidth()) {
            val rows = currentDoc.facets.entries.sortedBy { facetKeyString(it.key) }
            items(rows) { facetEntry ->
                Row(
                    modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Text(
                        text = facetKeyString(facetEntry.key),
                        style = MaterialTheme.typography.bodySmall,
                        modifier = Modifier.weight(0.35f)
                    )
                    Text(
                        text = previewFacetValue(facetEntry.value),
                        style = MaterialTheme.typography.bodySmall,
                        modifier = Modifier.weight(0.65f)
                    )
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
        enabled = enabled,
        placeholder = { Text(text = "Title") },
        textStyle = MaterialTheme.typography.headlineLarge.copy(fontWeight = FontWeight.Bold),
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

private fun previewFacetValue(json: String): String {
    val dequoted = dequoteJson(json)
    return if (dequoted == json) json.take(120) else dequoted.take(120)
}

private fun facetKeyString(key: FacetKey): String {
    val tagString =
        when (val tag = key.tag) {
            is FacetTag.WellKnown -> tag.v1.name.lowercase()
            is FacetTag.Any -> tag.v1
        }
    return if (key.id == "main") tagString else "$tagString:${key.id}"
}

private fun titleFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.TITLE_GENERIC), "main")

private fun noteFacetKey(): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), "main")
