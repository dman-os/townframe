package org.example.daybook.capture

import androidx.camera.core.CameraSelector
import androidx.camera.extensions.ExtensionMode

/**
 * State for camera extensions and controls
 */
data class CameraExtensionsState(
    val availableExtensions: List<Int> = listOf(ExtensionMode.NONE),
    val selectedExtension: Int = ExtensionMode.NONE,
    val availableLens: List<Int> = listOf(CameraSelector.LENS_FACING_BACK),
    val selectedLens: Int = CameraSelector.LENS_FACING_BACK,
    val isInitialized: Boolean = false
)

/**
 * Extension mode display names
 */
fun getExtensionModeName(mode: Int): String = when (mode) {
    ExtensionMode.NONE -> "None"
    ExtensionMode.AUTO -> "Auto"
    ExtensionMode.BOKEH -> "Bokeh"
    ExtensionMode.HDR -> "HDR"
    ExtensionMode.NIGHT -> "Night"
    ExtensionMode.FACE_RETOUCH -> "Face Retouch"
    else -> "Unknown"
}
