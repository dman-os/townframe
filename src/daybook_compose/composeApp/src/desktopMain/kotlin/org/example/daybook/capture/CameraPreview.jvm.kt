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
import javax.imageio.ImageIO
import kotlinx.coroutines.Dispatchers
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
        val deviceId = selectedDeviceId ?: return@LaunchedEffect
        try {
            cameraPreviewFfi.startStream(deviceId.toUInt(), noOpListener)
            errorText = null
        } catch (ffiError: FfiException) {
            errorText = ffiError.message()
        }
    }

    LaunchedEffect(cameraPreviewFfi, selectedDeviceId, onFrameAvailable) {
        if (selectedDeviceId == null) return@LaunchedEffect
        while (isActive) {
            val nextFrame = cameraPreviewFfi.`takeLatestFrame`()
            if (nextFrame != null) {
                latestFrame = nextFrame
                latestImageBitmap = withContext(Dispatchers.IO) { nextFrame.toImageBitmap() }
                if (onFrameAvailable != null) {
                    val sample = withContext(Dispatchers.IO) { nextFrame.toFrameSample() }
                    onFrameAvailable.invoke(sample)
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
                    Box(
                        modifier = Modifier.fillMaxSize(),
                        contentAlignment = Alignment.Center
                    ) {
                        Text(text = errorText ?: "Camera error")
                    }
                }
            }
        }
    }
}
