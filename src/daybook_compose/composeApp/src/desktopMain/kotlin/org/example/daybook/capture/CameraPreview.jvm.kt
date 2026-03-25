package org.example.daybook.capture

import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.toComposeImageBitmap
import androidx.compose.ui.layout.ContentScale
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.util.logging.Logger
import javax.imageio.ImageIO
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.example.daybook.capture.data.CameraFrameSample
import org.example.daybook.uniffi.CameraDeviceInfo
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.CameraPreviewFrame
import org.example.daybook.uniffi.CameraPreviewFrameEncoding
import org.example.daybook.uniffi.CameraPreviewFrameListener
import org.example.daybook.uniffi.FfiException
import org.jetbrains.skia.Image as SkiaImage

private val previewLogger: Logger = Logger.getLogger("DaybookCameraPreviewJvm")

private fun CameraPreviewFrame.toImageBitmap(): ImageBitmap {
    return when (encoding) {
        CameraPreviewFrameEncoding.JPEG -> {
            val skiaImage = SkiaImage.makeFromEncoded(frameBytes)
            skiaImage.toComposeImageBitmap()
        }

        CameraPreviewFrameEncoding.RGB24 -> {
            val outputBuffer = ByteArrayOutputStream()
            val wroteImage = ImageIO.write(toRgbBufferedImage(), "jpg", outputBuffer)
            check(wroteImage) { "failed to encode RGB frame as JPEG for preview" }
            SkiaImage.makeFromEncoded(outputBuffer.toByteArray()).toComposeImageBitmap()
        }
    }
}

private fun CameraPreviewFrame.toRgbBufferedImage(): BufferedImage {
    check(encoding == CameraPreviewFrameEncoding.RGB24) {
        "toRgbBufferedImage requires RGB24 encoding"
    }
    val widthPixels = widthPx.toInt()
    val heightPixels = heightPx.toInt()
    val expectedSize = widthPixels * heightPixels * 3
    require(frameBytes.size >= expectedSize) {
        "invalid rgb frame buffer, expected at least $expectedSize bytes but got ${frameBytes.size}"
    }

    val pixelData = IntArray(widthPixels * heightPixels)
    var rgbOffset = 0
    for (pixelIndex in pixelData.indices) {
        val red = frameBytes[rgbOffset++].toInt() and 0xFF
        val green = frameBytes[rgbOffset++].toInt() and 0xFF
        val blue = frameBytes[rgbOffset++].toInt() and 0xFF
        pixelData[pixelIndex] = (red shl 16) or (green shl 8) or blue
    }

    return BufferedImage(widthPixels, heightPixels, BufferedImage.TYPE_INT_RGB).apply {
        setRGB(0, 0, widthPixels, heightPixels, pixelData, 0, widthPixels)
    }
}

private fun CameraPreviewFrame.toJpegBytes(): ByteArray {
    return when (encoding) {
        CameraPreviewFrameEncoding.JPEG -> frameBytes
        CameraPreviewFrameEncoding.RGB24 -> {
            val outputBuffer = ByteArrayOutputStream()
            val wroteImage = ImageIO.write(toRgbBufferedImage(), "jpg", outputBuffer)
            check(wroteImage) { "failed to encode camera frame as JPEG" }
            outputBuffer.toByteArray()
        }
    }
}

private fun jpegToImageBitmap(jpegBytes: ByteArray): ImageBitmap {
    return SkiaImage.makeFromEncoded(jpegBytes).toComposeImageBitmap()
}

private fun CameraPreviewFrame.toFrameSample(): CameraFrameSample {
    return CameraFrameSample(
        widthPx = widthPx.toInt(),
        heightPx = heightPx.toInt(),
        jpegBytes = toJpegBytes()
    )
}

@Composable
actual fun DaybookCameraPreview(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier,
    selectedDeviceId: Int?,
    onAvailableDevicesChanged: ((List<CameraDeviceInfo>, Int?) -> Unit)?,
    onImageSaved: ((ByteArray) -> Unit)?,
    onFrameAvailable: ((CameraFrameSample) -> Unit)?
) {
    val captureContext = LocalCameraCaptureContext.current
    val coroutineScope = rememberCoroutineScope()

    var devices by remember { mutableStateOf<List<CameraDeviceInfo>>(emptyList()) }
    var latestFrame by remember { mutableStateOf<CameraPreviewFrame?>(null) }
    var latestImageBitmap by remember { mutableStateOf<ImageBitmap?>(null) }
    var errorText by remember { mutableStateOf<String?>(null) }

    val noOpListener =
        remember {
            object : CameraPreviewFrameListener {
                override fun onCameraPreviewFrame(frame: CameraPreviewFrame) {
                    // Pull-based preview loop uses takeLatestFrame() to avoid callback backlog.
                }
            }
        }

    LaunchedEffect(cameraPreviewFfi) {
        try {
            val listedDevices = cameraPreviewFfi.listDevices()
            devices = listedDevices
            onAvailableDevicesChanged?.invoke(listedDevices, listedDevices.firstOrNull()?.deviceId?.toInt())
            errorText = if (listedDevices.isEmpty()) "No camera devices found." else null
        } catch (ffiError: FfiException) {
            errorText = ffiError.message()
        }
    }

    LaunchedEffect(cameraPreviewFfi, selectedDeviceId) {
        val deviceId = selectedDeviceId
        if (deviceId == null) {
            latestFrame = null
            latestImageBitmap = null
            runCatching { cameraPreviewFfi.stopStream() }.onFailure {
                previewLogger.warning("failed stopping stream on deselect: ${it.message ?: it}")
            }
            return@LaunchedEffect
        }
        try {
            cameraPreviewFfi.startStream(deviceId.toUInt(), noOpListener)
            errorText = null
            awaitCancellation()
        } catch (ffiError: FfiException) {
            errorText = ffiError.message()
        } finally {
            runCatching { cameraPreviewFfi.stopStream() }.onFailure {
                previewLogger.warning("failed stopping stream: ${it.message ?: it}")
            }
        }
    }

    LaunchedEffect(cameraPreviewFfi, selectedDeviceId, onFrameAvailable) {
        if (selectedDeviceId == null) return@LaunchedEffect
        var consecutiveFailures = 0
        while (isActive) {
            try {
                val nextFrame = cameraPreviewFfi.`takeLatestFrame`()
                if (nextFrame != null) {
                    consecutiveFailures = 0
                    errorText = null
                    latestFrame = nextFrame
                    if (nextFrame.encoding == CameraPreviewFrameEncoding.RGB24) {
                        val jpegBytes = withContext(Dispatchers.IO) { nextFrame.toJpegBytes() }
                        latestImageBitmap = withContext(Dispatchers.IO) { jpegToImageBitmap(jpegBytes) }
                        if (onFrameAvailable != null) {
                            val sample =
                                CameraFrameSample(
                                    widthPx = nextFrame.widthPx.toInt(),
                                    heightPx = nextFrame.heightPx.toInt(),
                                    jpegBytes = jpegBytes
                                )
                            onFrameAvailable.invoke(sample)
                        }
                    } else {
                        latestImageBitmap = withContext(Dispatchers.IO) { nextFrame.toImageBitmap() }
                        if (onFrameAvailable != null) {
                            val sample = withContext(Dispatchers.IO) { nextFrame.toFrameSample() }
                            onFrameAvailable.invoke(sample)
                        }
                    }
                }
            } catch (error: Throwable) {
                consecutiveFailures += 1
                previewLogger.warning("camera preview frame processing failed: ${error.message ?: error}")
                if (consecutiveFailures >= 5) {
                    errorText = "Camera preview failed repeatedly. Please restart camera stream."
                }
            }
            delay(12)
        }
    }

    LaunchedEffect(captureContext, latestFrame) {
        val context = captureContext ?: return@LaunchedEffect
        context.setCanCapture(latestFrame != null)
        context.setCaptureCallback {
            val frameToSave = latestFrame
            if (frameToSave == null) {
                context.setCanCapture(false)
                return@setCaptureCallback
            }

            coroutineScope.launch {
                context.setIsCapturing(true)
                try {
                    val jpegBytes = withContext(Dispatchers.IO) { frameToSave.toJpegBytes() }
                    onImageSaved?.invoke(jpegBytes)
                } finally {
                    context.setIsCapturing(false)
                }
            }
        }
    }

    DisposableEffect(cameraPreviewFfi, captureContext) {
        onDispose {
            captureContext?.setCaptureCallback(null)
            captureContext?.setCanCapture(false)
            cameraPreviewFfi.stopStream()
        }
    }

    Box(modifier = modifier.fillMaxSize()) {
        if (devices.isEmpty()) {
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Text(text = errorText ?: "Loading camera devices...")
            }
        } else {
            Box(
                modifier =
                    Modifier
                        .fillMaxSize()
                        .background(MaterialTheme.colorScheme.surface)
            ) {
                if (latestImageBitmap != null) {
                    Image(
                        bitmap = latestImageBitmap!!,
                        contentDescription = "Camera preview",
                        modifier = Modifier.fillMaxSize(),
                        contentScale = ContentScale.Crop,
                        alignment = Alignment.Center
                    )
                }

                if (errorText != null) {
                    val message = errorText
                    Box(
                        modifier = Modifier.fillMaxSize(),
                        contentAlignment = Alignment.Center
                    ) {
                        Text(text = message!!)
                    }
                }
            }
        }
    }
}
