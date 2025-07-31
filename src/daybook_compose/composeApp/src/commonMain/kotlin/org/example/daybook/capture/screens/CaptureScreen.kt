package org.example.daybook.capture.screens

import androidx.compose.runtime.Composable
import androidx.compose.runtime.compositionLocalOf
import androidx.lifecycle.ViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow

enum class CaptureMode {
    Text,
    Camera,
    Mic
}

class CaptureContext(
    val initialMode: CaptureMode = CaptureMode.Text,
    val availaibleModes: Set<CaptureMode> = setOf(CaptureMode.Text)
) {
}

val LocalCaptureCtx = compositionLocalOf<CaptureContext> { CaptureContext() }

class CaptureScreenViewModel(
    ctx: CaptureContext,
): ViewModel() {
    private val _captureMode = MutableStateFlow(ctx.initialMode)
    val captureMode = _captureMode.asStateFlow()
}

fun viewModel(captureScreenViewModel: CaptureScreenViewModel) {}

@Composable
fun CaptureScreen(
) {
}
