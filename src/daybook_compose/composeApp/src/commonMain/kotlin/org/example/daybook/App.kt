package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

import daybook.composeapp.generated.resources.Res
import daybook.composeapp.generated.resources.compose_multiplatform

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val requestAllPermissions: () -> Unit = {},
) {
    val hasAll = hasCamera and
                hasNotifications and
                hasMicrophone and
                hasOverlay;
}


val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

@Composable
@Preview
fun App(
    extraAction: (() -> Unit)? = null,
) {
    val permCtx = LocalPermCtx.current;
    MaterialTheme {
        var showContent by remember { mutableStateOf(false) }

        Column(
            modifier = Modifier
                .safeContentPadding()
                .fillMaxSize(),
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Button(onClick = {
                showContent = !showContent
                // uniffi.daybook_core.uniffiEnsureInitialized()
                uniffi.daybook_core.init()
                extraAction?.invoke()
            }) {
                Text("Click me!")
            }
            if (permCtx != null) {
                if (permCtx.hasAll) {
                    Text("All permissions avail")
                } else {
                    Button(onClick = {
                        permCtx.requestAllPermissions()
                    }) {
                        Text("Ask for permissions")
                    }
                }
            }
            AnimatedVisibility(showContent) {
                val greeting = remember { Greeting().greet() }
                Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
                    Image(painterResource(Res.drawable.compose_multiplatform), null)
                    Text("Compose: $greeting")
                }
            }
        }
    }
}