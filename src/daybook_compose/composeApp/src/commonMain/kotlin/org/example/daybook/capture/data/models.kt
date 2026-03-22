package org.example.daybook.capture.data

sealed interface CameraOverlay {
    data object Grid : CameraOverlay

    data class QrBounds(
        val left: Float,
        val top: Float,
        val right: Float,
        val bottom: Float,
        val sourceWidthPx: Int,
        val sourceHeightPx: Int
    ) : CameraOverlay
}

data class CameraFrameSample(
    val widthPx: Int,
    val heightPx: Int,
    val jpegBytes: ByteArray
)

data class CameraOverlayState(
    val overlays: List<CameraOverlay> = emptyList(),
    val lastDetectedText: String? = null,
    val latestError: String? = null
)
