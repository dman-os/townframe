package org.example.daybook.capture

import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * Context object that coordinates camera capture functionality between
 * the camera preview and the bottom navigation bar.
 */
class CameraCaptureContext : ViewModel() {
    private val _canCapture = MutableStateFlow(false)
    val canCapture = _canCapture.asStateFlow()

    private val _isCapturing = MutableStateFlow(false)
    val isCapturing = _isCapturing.asStateFlow()

    private var captureCallback: (() -> Unit)? = null

    /**
     * Register a callback to be invoked when capture is requested from the bottom bar.
     */
    fun setCaptureCallback(callback: (() -> Unit)?) {
        captureCallback = callback
    }

    /**
     * Called by the camera preview to indicate it's ready to capture.
     */
    fun setCanCapture(canCapture: Boolean) {
        _canCapture.value = canCapture
    }

    /**
     * Called by the camera preview to indicate capture is in progress.
     */
    fun setIsCapturing(isCapturing: Boolean) {
        _isCapturing.value = isCapturing
    }

    /**
     * Called from the bottom bar to trigger a capture.
     */
    fun requestCapture() {
        if (_canCapture.value && !_isCapturing.value) {
            captureCallback?.invoke()
        }
    }

    override fun onCleared() {
        captureCallback = null
        super.onCleared()
    }
}

val LocalCameraCaptureContext = compositionLocalOf<CameraCaptureContext?> { null }

/**
 * Provides a CameraCaptureContext to the composition tree.
 */
@Composable
fun ProvideCameraCaptureContext(context: CameraCaptureContext, content: @Composable () -> Unit) {
    CompositionLocalProvider(LocalCameraCaptureContext provides context) {
        content()
    }
}
