package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application

fun main() = application {
    Window(
        onCloseRequest = ::exitApplication,
        title = "Daybook",
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
            )
        ) {
            App(
                extraAction = {
                }
            )
        }
    }
}
