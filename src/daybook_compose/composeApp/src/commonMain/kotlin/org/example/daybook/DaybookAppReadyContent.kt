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
internal fun DaybookAppReadyContent(
    appContainer: AppContainer,
    drawerVm: DrawerViewModel,
    docEditorStore: DocEditorStoreViewModel,
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    onExitRequest: (() -> Unit)? = null,
) {
    Box(modifier = Modifier.fillMaxSize()) {
        CompositionLocalProvider(
            LocalContainer provides appContainer,
            LocalDrawerViewModel provides drawerVm,
            LocalDocEditorStore provides docEditorStore,
        ) {
            val cameraCaptureContext = remember { CameraCaptureContext() }
            val chromeStateManager = remember { ChromeStateManager() }
            ProvideCameraCaptureContext(cameraCaptureContext) {
                CompositionLocalProvider(
                    LocalChromeStateManager provides chromeStateManager,
                    LocalAppExitRequest provides onExitRequest,
                ) {
                    val navState = rememberDaybookNavigationState()
                    AdaptiveAppLayout(
                        modifier = surfaceModifier,
                        navState = navState,
                        extraAction = extraAction,
                    )
                }
            }
        }
    }
}
