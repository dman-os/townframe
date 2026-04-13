package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import kotlinx.coroutines.delay
import java.awt.EventQueue
import sun.misc.Signal
import java.util.concurrent.atomic.AtomicBoolean

private val signalShutdownRequested = AtomicBoolean(false)

private fun installSignalHandler(signalName: String) {
    runCatching {
        Signal.handle(Signal(signalName)) {
            println("[APP_SHUTDOWN] signal received name=$signalName, requesting graceful shutdown")
            signalShutdownRequested.set(true)
        }
        println("[APP_SHUTDOWN] installed signal handler name=$signalName")
    }.onFailure { error ->
        println("[APP_SHUTDOWN] failed to install signal handler name=$signalName err=${error.message}")
        throw error
    }
}

fun main() = application {
    installSignalHandler("INT")
    installSignalHandler("TERM")

    val windowState =
        rememberWindowState(
            // FIXME: niri/xwayland doesn't like the javafx resize-logic
            // so we explicitly set it
            // size = DpSize((0.2 * 2560).dp, (1600 * 0.75).dp)
            // size = DpSize((0.75 * 2560).dp, (1600 - 20).dp)
            size = DpSize((0.75 * 1600).dp, (900 - 20).dp)
        )
    DisposableEffect(Unit) {
        val hook = Thread { println("[APP_SHUTDOWN] JVM shutdown hook triggered (signal/process exit)") }
        Runtime.getRuntime().addShutdownHook(hook)
        onDispose {
            runCatching { Runtime.getRuntime().removeShutdownHook(hook) }
        }
    }

    Window(
        onCloseRequest = {
            println("[APP_SHUTDOWN] start: close requested, beginning graceful shutdown")
            signalShutdownRequested.set(true)
        },
        title = "Daybook",
        state = windowState
    ) {
        var shutdownRequested by remember { mutableStateOf(false) }
        var shutdownDone by remember { mutableStateOf(false) }
        LaunchedEffect(Unit) {
            while (true) {
                if (signalShutdownRequested.getAndSet(false)) {
                    println("[APP_SHUTDOWN] start: signal-triggered graceful shutdown")
                    shutdownRequested = true
                }
                if (shutdownDone) break
                delay(100)
            }
        }
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
                    hasStorageRead = true,
                    hasStorageWrite = true,
                    requestPermissions = {}
                ),
            LocalPlatform provides createReactiveJVMPlatform(windowState)
        ) {
            App(
                extraAction = {
                },
                shutdownRequested = shutdownRequested,
                onShutdownCompleted = {
                    shutdownDone = true
                    EventQueue.invokeLater { exitApplication() }
                },
                autoShutdownOnDispose = false
            )
        }
    }
}
