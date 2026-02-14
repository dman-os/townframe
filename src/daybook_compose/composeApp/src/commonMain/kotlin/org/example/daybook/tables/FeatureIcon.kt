package org.example.daybook.tables

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.CameraAlt
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.Folder
import androidx.compose.material.icons.filled.Home
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material.icons.filled.TableChart
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable

@Composable
fun FeatureIcon(feature: FeatureItem) {
    when (feature.key) {
        "nav_home" -> Icon(Icons.Default.Home, contentDescription = feature.label)
        "nav_capture" -> Icon(Icons.Default.CameraAlt, contentDescription = feature.label)
        "nav_documents" -> Icon(Icons.Default.Description, contentDescription = feature.label)
        "nav_tables" -> Icon(Icons.Default.TableChart, contentDescription = feature.label)
        "nav_settings" -> Icon(Icons.Default.Settings, contentDescription = feature.label)
        "tables_new_table" -> Icon(Icons.Default.Add, contentDescription = feature.label)
        "tables_new_tab" -> Icon(Icons.Default.Description, contentDescription = feature.label)
        else -> {
            if (feature.icon.isNotBlank()) {
                Text(feature.icon, style = MaterialTheme.typography.bodyLarge)
            }
        }
    }
}

@Composable
fun TableIcon(contentDescription: String = "Table") {
    Icon(Icons.Default.Folder, contentDescription = contentDescription)
}
