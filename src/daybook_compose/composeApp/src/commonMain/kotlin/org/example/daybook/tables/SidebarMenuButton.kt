package org.example.daybook.tables

import androidx.compose.foundation.layout.Box
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.material3.MaterialTheme
import org.example.daybook.LocalAppExitRequest

@Composable
fun SidebarMenuButton(modifier: Modifier = Modifier) {
    val exitRequest = LocalAppExitRequest.current
    var showMenu by remember { mutableStateOf(false) }

    Box(modifier = modifier) {
        IconButton(onClick = { showMenu = true }) {
            Text("🌞", style = MaterialTheme.typography.titleMedium)
        }
        DropdownMenu(
            expanded = showMenu,
            onDismissRequest = { showMenu = false }
        ) {
            DropdownMenuItem(
                text = { Text("Exit") },
                leadingIcon = { Icon(Icons.Default.Close, contentDescription = null) },
                enabled = exitRequest != null,
                onClick = {
                    showMenu = false
                    exitRequest?.invoke()
                }
            )
        }
    }
}
