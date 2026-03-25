// FIXME: i don't like the name of this file, surely we'll have other 
// uses for QR?

package org.example.daybook.capture.data

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlin.time.TimeSource
import org.example.daybook.uniffi.CameraOverlay as FfiCameraOverlay
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.CameraQrAnalyzerFfi
import org.example.daybook.uniffi.CameraQrEventListener

private fun createQrListener(
    state: MutableStateFlow<CameraOverlayState>,
    isSessionActive: () -> Boolean,
    onDetectedText: ((String) -> Unit)?
): CameraQrEventListener =
    object : CameraQrEventListener {
        override fun onCameraQrOverlaysUpdated(overlays: List<FfiCameraOverlay>) {
            if (!isSessionActive()) return
            state.update { prior ->
                prior.copy(
                    overlays =
                        overlays.mapNotNull { overlay ->
                            when (overlay) {
                                is FfiCameraOverlay.Grid -> CameraOverlay.Grid
                                is FfiCameraOverlay.QrBounds ->
                                    CameraOverlay.QrBounds(
                                        left = overlay.bounds.left,
                                        top = overlay.bounds.top,
                                        right = overlay.bounds.right,
                                        bottom = overlay.bounds.bottom,
                                        sourceWidthPx = overlay.frameWidthPx.toInt(),
                                        sourceHeightPx = overlay.frameHeightPx.toInt()
                                    )
                            }
                        }
                )
            }
        }

        override fun onCameraQrDetected(decodedText: String) {
            if (!isSessionActive()) return
            state.update { prior -> prior.copy(lastDetectedText = decodedText) }
            onDetectedText?.invoke(decodedText)
        }

        override fun onCameraQrError(message: String) {
            if (!isSessionActive()) return
            state.update { prior -> prior.copy(latestError = message) }
        }
    }

class CameraQrOverlayBridge(
    private val analyzer: CameraQrAnalyzerFfi,
    private val onDetectedText: ((String) -> Unit)? = null
) {
    private val _state = MutableStateFlow(CameraOverlayState())
    val state: StateFlow<CameraOverlayState> = _state

    private var lastSubmitAt: kotlin.time.TimeMark? = null
    @Volatile
    private var started = false
    @Volatile
    private var sessionEpoch = 0L
    private var listener: CameraQrEventListener? = null

    private fun nextSessionToken(): Long =
        synchronized(this) {
            sessionEpoch += 1
            sessionEpoch
        }

    fun start() {
        if (started) return
        val sessionToken = nextSessionToken()
        started = true
        val sessionListener =
            createQrListener(
                state = _state,
                isSessionActive = { started && sessionEpoch == sessionToken },
                onDetectedText = onDetectedText
            )
        listener = sessionListener
        analyzer.setListener(sessionListener)
    }

    fun stop() {
        if (!started) return
        nextSessionToken()
        started = false
        listener = null
        analyzer.clearListener()
        lastSubmitAt = null
        _state.value = CameraOverlayState()
    }

    fun submitFrame(sample: CameraFrameSample) {
        if (!started) return
        val now = TimeSource.Monotonic.markNow()
        val previous = lastSubmitAt
        if (previous != null && previous.elapsedNow().inWholeMilliseconds < 200L) return
        lastSubmitAt = now

        runCatching {
            analyzer.submitJpegFrame(
                widthPx = sample.widthPx.toUInt(),
                heightPx = sample.heightPx.toUInt(),
                frameBytes = sample.jpegBytes
            )
        }.onFailure { throwable ->
            _state.update { prior ->
                prior.copy(
                    latestError = throwable.message ?: throwable::class.simpleName
                )
            }
        }
    }
}

class CameraPreviewQrBridge(
    private val cameraPreviewFfi: CameraPreviewFfi,
    private val onDetectedText: ((String) -> Unit)? = null
) {
    private val _state = MutableStateFlow(CameraOverlayState())
    val state: StateFlow<CameraOverlayState> = _state

    @Volatile
    private var started = false
    @Volatile
    private var sessionEpoch = 0L
    private var listener: CameraQrEventListener? = null

    private fun nextSessionToken(): Long =
        synchronized(this) {
            sessionEpoch += 1
            sessionEpoch
        }

    fun start() {
        if (started) return
        val sessionToken = nextSessionToken()
        started = true
        val sessionListener =
            createQrListener(
                state = _state,
                isSessionActive = { started && sessionEpoch == sessionToken },
                onDetectedText = onDetectedText
            )
        listener = sessionListener
        cameraPreviewFfi.setQrListener(sessionListener)
        cameraPreviewFfi.setQrAnalysisEnabled(true)
    }

    fun stop() {
        if (!started) return
        nextSessionToken()
        started = false
        listener = null
        cameraPreviewFfi.setQrAnalysisEnabled(false)
        cameraPreviewFfi.clearQrListener()
        _state.value = CameraOverlayState()
    }
}
