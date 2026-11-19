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
import sun.misc.Signal
import java.awt.EventQueue
import java.util.concurrent.atomic.AtomicBoolean

private val signalShutdownRequested = AtomicBoolean(false)

private fun installSignalHandler(signalName: String) {
    try {
        Signal.handle(Signal(signalName)) {
            println("[APP_SHUTDOWN] signal received name=$signalName, requesting graceful shutdown")
            signalShutdownRequested.set(true)
        }
        println("[APP_SHUTDOWN] installed signal handler name=$signalName")
    } catch (e: IllegalArgumentException) {
        println(
            "[APP_SHUTDOWN] invalid signal name=$signalName err=${e.message}",
        )
    } catch (e: UnsupportedOperationException) {
        println(
            "[APP_SHUTDOWN] signals not supported on this platform name=$signalName err=${e.message}",
        )
    } catch (e: NoClassDefFoundError) {
        println(
            "[APP_SHUTDOWN] signal handler unavailable (jdk.unsupported missing) name=$signalName err=${e.message}",
        )
    }
}

fun main() = application {
    LaunchedEffect(Unit) {
        installSignalHandler("INT")
        installSignalHandler("TERM")
    }

    val windowState =
        rememberWindowState(
            // FIXME: niri/xwayland doesn't like the javafx resize-logic
            // so we explicitly set it
            // size = DpSize((0.2 * 2560).dp, (1600 * 0.75).dp)
            // size = DpSize((0.75 * 2560).dp, (1600 - 20).dp)
            size = DpSize((0.75 * 1600).dp, (900 - 20).dp),
        )
    DisposableEffect(Unit) {
        val hook =
            Thread {
                println("[APP_SHUTDOWN] JVM shutdown hook triggered (signal/process exit)")
                signalShutdownRequested.set(true)
            }
        Runtime.getRuntime().addShutdownHook(hook)
        onDispose {
            try {
                Runtime.getRuntime().removeShutdownHook(hook)
            } catch (e: IllegalStateException) {
                println("[APP_SHUTDOWN] shutdown already in progress, cannot remove hook: ${e.message}")
            } catch (e: SecurityException) {
                println("[APP_SHUTDOWN] denied removing shutdown hook err=${e.message}")
            }
        }
    }

    Window(
        onCloseRequest = {
            println("[APP_SHUTDOWN] start: close requested, beginning graceful shutdown")
            signalShutdownRequested.set(true)
        },
        title = "Daybook",
        state = windowState,
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
                    density = LocalDensity.current.density,
                ),
            LocalPermCtx provides
                PermissionsContext(
                    hasCamera = true,
                    hasOverlay = true,
                    hasMicrophone = true,
                    hasNotifications = true,
                    hasStorageRead = true,
                    hasStorageWrite = true,
                    requestPermissions = {},
                ),
            LocalPlatform provides createReactiveJVMPlatform(windowState),
        ) {
            App(
                extraAction = {
                },
                shutdownRequested = shutdownRequested,
                onShutdownCompleted = {
                    shutdownDone = true
                    EventQueue.invokeLater { exitApplication() }
                },
                autoShutdownOnDispose = false,
                onExitRequest = {
                    signalShutdownRequested.set(true)
                },
            )
        }
    }
}
