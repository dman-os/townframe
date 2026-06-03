@file:OptIn(androidx.compose.material3.ExperimentalMaterial3Api::class)

package org.example.daybook

import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color as UiColor

data class ScreenChromeSpec(val topBar: TopBarSpec = TopBarSpec()) {
    data class TopBarSpec(val title: String? = null, val showBack: Boolean = false, val onBack: (() -> Unit)? = null)
}

@Composable
fun DaybookTopBar(chrome: ScreenChromeSpec.TopBarSpec) {
    TopAppBar(
        title = {
            if (chrome.title != null) {
                Text(chrome.title)
            }
        },
        navigationIcon = {
            if (chrome.showBack && chrome.onBack != null) {
                IconButton(onClick = chrome.onBack) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            }
        },
        colors =
        TopAppBarDefaults.topAppBarColors(
            containerColor = UiColor.Transparent,
            scrolledContainerColor = UiColor.Transparent,
            navigationIconContentColor = MaterialTheme.colorScheme.onSurface,
            titleContentColor = MaterialTheme.colorScheme.onSurface,
            actionIconContentColor = MaterialTheme.colorScheme.onSurface,
        ),
    )
}

@Composable
fun DaybookScreenScaffold(
    chrome: ScreenChromeSpec,
    modifier: Modifier = Modifier,
    content: @Composable (PaddingValues) -> Unit,
) {
    Scaffold(
        modifier = modifier,
        topBar = { DaybookTopBar(chrome.topBar) },
        content = content,
    )
}
