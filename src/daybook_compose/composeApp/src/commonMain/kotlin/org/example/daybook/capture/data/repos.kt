package org.example.daybook.capture.data

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlin.time.TimeSource
import org.example.daybook.uniffi.CameraOverlay as FfiCameraOverlay
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.CameraQrAnalyzerFfi
import org.example.daybook.uniffi.CameraQrEventListener

class CameraQrOverlayBridge(
    private val analyzer: CameraQrAnalyzerFfi,
    private val onDetectedText: ((String) -> Unit)? = null
) {
    private val _state = MutableStateFlow(CameraOverlayState())
    val state: StateFlow<CameraOverlayState> = _state

    private var lastSubmitAt: kotlin.time.TimeMark? = null
    private var started = false

    private val listener =
        object : CameraQrEventListener {
            override fun onCameraQrOverlaysUpdated(overlays: List<FfiCameraOverlay>) {
                _state.update { prior ->
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
                                            bottom = overlay.bounds.bottom
                                        )
                                }
                            }
                    )
                }
            }

            override fun onCameraQrDetected(decodedText: String) {
                _state.update { prior -> prior.copy(lastDetectedText = decodedText) }
                onDetectedText?.invoke(decodedText)
            }

            override fun onCameraQrError(message: String) {
                _state.update { prior -> prior.copy(latestError = message) }
            }
        }

    fun start() {
        if (started) return
        analyzer.setListener(listener)
        started = true
    }

    fun stop() {
        if (!started) return
        analyzer.clearListener()
        started = false
    }

    fun submitFrame(sample: CameraFrameSample) {
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

    private var started = false

    private val listener =
        object : CameraQrEventListener {
            override fun onCameraQrOverlaysUpdated(overlays: List<FfiCameraOverlay>) {
                _state.update { prior ->
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
                                            bottom = overlay.bounds.bottom
                                        )
                                }
                            }
                    )
                }
            }

            override fun onCameraQrDetected(decodedText: String) {
                _state.update { prior -> prior.copy(lastDetectedText = decodedText) }
                onDetectedText?.invoke(decodedText)
            }

            override fun onCameraQrError(message: String) {
                _state.update { prior -> prior.copy(latestError = message) }
            }
        }

    fun start() {
        if (started) return
        cameraPreviewFfi.setQrListener(listener)
        cameraPreviewFfi.setQrAnalysisEnabled(true)
        started = true
    }

    fun stop() {
        if (!started) return
        cameraPreviewFfi.setQrAnalysisEnabled(false)
        cameraPreviewFfi.clearQrListener()
        started = false
    }
}
