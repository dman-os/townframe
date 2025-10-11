package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState

fun main() = application {
    val windowState = rememberWindowState(
        // FIXME: niri/xwayland doesn't like the javafx resize-logic
        // so we explicitly set it
        // size = DpSize((0.2 * 2560).dp, (1600 * 0.75).dp)
        size = DpSize((0.75 * 2560).dp, (1600 - 20).dp)
        // size = DpSize((0.75 * 1600).dp, (900 - 20).dp)
    )
    Window(
        onCloseRequest = ::exitApplication,
        title = "Daybook",
        state = windowState
    ) {
        CompositionLocalProvider(
            LocalDensity provides Density(
                density = LocalDensity.current.density,
            ),
            LocalPermCtx provides PermissionsContext(
                hasCamera = true,
                hasOverlay = true,
                hasMicrophone = true,
                hasNotifications = true,
                requestAllPermissions = {}
            ),
            LocalPlatform provides createReactiveJVMPlatform(windowState)
        ) {
            App(
                extraAction = {
                }
            )
        }
    }
}
