@file:Suppress("FunctionNaming")

package org.example.daybook

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import org.example.daybook.capture.CameraCaptureContext
import org.example.daybook.capture.ProvideCameraCaptureContext
import org.example.daybook.navigation.rememberDaybookNavigationState

@Composable
internal fun DaybookAppReadyContent(args: DaybookAppReadyContentArgs, surfaceModifier: Modifier = Modifier) {
    Box(modifier = Modifier.fillMaxSize()) {
        CompositionLocalProvider(
            LocalContainer provides args.appContainer,
            LocalDrawerViewModel provides args.drawerVm,
            LocalDocEditorStore provides args.docEditorStore,
        ) {
            val cameraCaptureContext = remember { CameraCaptureContext() }
            val chromeStateManager = remember { ChromeStateManager() }
            ProvideCameraCaptureContext(cameraCaptureContext) {
                CompositionLocalProvider(
                    LocalChromeStateManager provides chromeStateManager,
                    LocalAppExitRequest provides args.onExitRequest,
                ) {
                    val navState = rememberDaybookNavigationState()
                    AdaptiveAppLayout(
                        modifier = surfaceModifier,
                        navState = navState,
                        extraAction = args.extraAction,
                    )
                }
            }
        }
    }
}

internal data class DaybookAppReadyContentArgs(
    val appContainer: AppContainer,
    val drawerVm: DrawerViewModel,
    val docEditorStore: DocEditorStoreViewModel,
    val extraAction: (() -> Unit)? = null,
    val onExitRequest: (() -> Unit)? = null,
)
