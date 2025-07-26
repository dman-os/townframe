package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application

fun main() = application {
    Window(
        onCloseRequest = ::exitApplication,
        title = "Daybook",
    ) {
        CompositionLocalProvider(
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
