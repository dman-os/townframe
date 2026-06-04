@file:OptIn(androidx.compose.material3.ExperimentalMaterial3Api::class)
@file:Suppress("Filename", "FunctionNaming", "MatchingDeclarationName")

package org.example.daybook.layouts

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
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color as UiColor

data class ScreenChromeSpec(val topBar: TopBarSpec = TopBarSpec()) {
    data class TopBarSpec(val title: String? = null, val showBack: Boolean = false, val onBack: (() -> Unit)? = null)
}

val LocalScreenChromeSpec =
    compositionLocalOf<ScreenChromeSpec> {
        error("no ScreenChromeSpec provided")
    }

@Composable
fun ProvideScreenChromeSpec(chrome: ScreenChromeSpec, content: @Composable () -> Unit) {
    CompositionLocalProvider(LocalScreenChromeSpec provides chrome, content = content)
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
            check(!chrome.showBack || chrome.onBack != null) {
                "inconsistent top bar chrome: showBack=${chrome.showBack} " +
                    "onBack=${chrome.onBack}"
            }
            if (chrome.showBack) {
                val onBack = chrome.onBack ?: error(
                    "inconsistent top bar chrome: showBack=${chrome.showBack} " +
                        "onBack=${chrome.onBack}",
                )
                IconButton(onClick = onBack) {
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
fun DaybookScaffold(
    modifier: Modifier = Modifier,
    topBar: (@Composable () -> Unit)? = null,
    content: @Composable (PaddingValues) -> Unit,
) {
    Scaffold(
        modifier = modifier,
        topBar = {
            if (topBar != null) {
                topBar()
            } else {
                DaybookTopBar(LocalScreenChromeSpec.current.topBar)
            }
        },
        content = content,
    )
}
