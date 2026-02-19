package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import java.nio.file.Paths

private fun configureNativeImageSystemProperties() {
    val execDirPath =
        ProcessHandle.current()
            .info()
            .command()
            .orElse(null)
            ?.let { Paths.get(it).parent?.toString() }
            ?: System.getProperty("user.dir")

    if (System.getProperty("java.home") == null) {
        System.setProperty("java.home", execDirPath)
    }
    if (System.getProperty("compose.application.configure.swing.globals") == null) {
        System.setProperty("compose.application.configure.swing.globals", "true")
    }
    if (System.getProperty("sun.java2d.dpiaware") == null) {
        System.setProperty("sun.java2d.dpiaware", "true")
    }
    if (System.getProperty("skiko.library.path") == null) {
        System.setProperty("skiko.library.path", execDirPath)
    }
    if (System.getProperty("jna.boot.library.path") == null) {
        System.setProperty("jna.boot.library.path", execDirPath)
    }
    if (System.getProperty("jna.nosys") == null) {
        System.setProperty("jna.nosys", "true")
    }

    val existingLibraryPath = System.getProperty("java.library.path").orEmpty()
    if (!existingLibraryPath.split(":").contains(execDirPath)) {
        val joined = listOf(execDirPath, existingLibraryPath).filter { it.isNotBlank() }.joinToString(":")
        System.setProperty("java.library.path", joined)
    }
}

fun main() {
    configureNativeImageSystemProperties()
    application {
    val windowState =
        rememberWindowState(
            // FIXME: niri/xwayland doesn't like the javafx resize-logic
            // so we explicitly set it
            // size = DpSize((0.2 * 2560).dp, (1600 * 0.75).dp)
            // size = DpSize((0.75 * 2560).dp, (1600 - 20).dp)
            size = DpSize((0.75 * 1600).dp, (900 - 20).dp)
        )
    Window(
        onCloseRequest = ::exitApplication,
        title = "Daybook",
        state = windowState
    ) {
        CompositionLocalProvider(
            LocalDensity provides
                Density(
                    density = LocalDensity.current.density
                ),
            LocalPermCtx provides
                PermissionsContext(
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
}
