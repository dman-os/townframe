package org.example.daybook.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.example.daybook.uniffi.core.Doc
import org.example.daybook.uniffi.core.DocContent

@Composable
fun DocEditor(
    doc: Doc?,
    onContentChange: (String) -> Unit,
    modifier: Modifier = Modifier,
    blobsRepo: org.example.daybook.uniffi.BlobsRepoFfi? = null
) {
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
                modifier = modifier.fillMaxSize(),
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
                        modifier = modifier.fillMaxSize(),
                        contentScale = androidx.compose.ui.layout.ContentScale.Fit
                    )
                } else {
                    Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                        CircularProgressIndicator()
                    }
                }
            } else {
                Box(modifier = modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text("Image unavailable (Repo missing)")
                }
            }
        }
        else -> {
            Column(
                modifier = modifier.fillMaxSize().padding(16.dp),
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
