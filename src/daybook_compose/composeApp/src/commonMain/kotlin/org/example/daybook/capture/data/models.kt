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

class CameraFrameSample(
    val widthPx: Int,
    val heightPx: Int,
    val jpegBytes: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is CameraFrameSample) return false
        return widthPx == other.widthPx &&
            heightPx == other.heightPx &&
            jpegBytes.contentEquals(other.jpegBytes)
    }

    override fun hashCode(): Int {
        var result = widthPx
        result = 31 * result + heightPx
        result = 31 * result + jpegBytes.contentHashCode()
        return result
    }
}

data class CameraOverlayState(
    val overlays: List<CameraOverlay> = emptyList(),
    val lastDetectedText: String? = null,
    val latestError: String? = null
)
