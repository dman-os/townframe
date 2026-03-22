@file:OptIn(ExperimentalCamera2Interop::class)

package org.example.daybook.capture

import android.graphics.ImageFormat
import android.graphics.Rect
import android.graphics.YuvImage
import android.util.Log
import androidx.camera.camera2.interop.ExperimentalCamera2Interop
import androidx.camera.core.AspectRatio
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageCapture
import androidx.camera.core.ImageCaptureException
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
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
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.compose.LocalLifecycleOwner
import java.io.ByteArrayOutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch
import org.example.daybook.LocalPermCtx
import org.example.daybook.PermissionRequest
import org.example.daybook.capture.data.CameraFrameSample
import org.example.daybook.uniffi.CameraPreviewFfi

private fun imageProxyToJpeg(image: ImageProxy): ByteArray {
    if (image.planes.size == 1) {
        val singlePlane = image.planes[0].buffer
        val bytes = ByteArray(singlePlane.remaining())
        singlePlane.get(bytes)
        return bytes
    }

    val yPlane = image.planes[0].buffer
    val uPlane = image.planes[1].buffer
    val vPlane = image.planes[2].buffer

    val ySize = yPlane.remaining()
    val uSize = uPlane.remaining()
    val vSize = vPlane.remaining()

    val nv21 = ByteArray(ySize + uSize + vSize)
    yPlane.get(nv21, 0, ySize)
    vPlane.get(nv21, ySize, vSize)
    uPlane.get(nv21, ySize + vSize, uSize)

    val yuvImage = YuvImage(nv21, ImageFormat.NV21, image.width, image.height, null)
    val output = ByteArrayOutputStream()
    val ok = yuvImage.compressToJpeg(Rect(0, 0, image.width, image.height), 85, output)
    check(ok) { "failed to compress camera frame to jpeg" }
    return output.toByteArray()
}

@Composable
actual fun DaybookCameraPreview(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier,
    selectedDeviceId: Int?,
    onAvailableDevicesChanged: ((List<org.example.daybook.uniffi.CameraDeviceInfo>, Int?) -> Unit)?,
    onImageSaved: ((ByteArray) -> Unit)?,
    onFrameAvailable: ((CameraFrameSample) -> Unit)?
) {
    val permCtx = LocalPermCtx.current
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val scope = rememberCoroutineScope()

    if (permCtx != null && !permCtx.hasCamera) {
        LaunchedEffect(Unit) {
            permCtx.requestPermissions(PermissionRequest(camera = true))
        }
        Box(
            modifier = modifier.fillMaxSize(),
            contentAlignment = Alignment.Center
        ) {
            Text("Camera permission required")
        }
        return
    }

    var imageCapture: ImageCapture? by remember { mutableStateOf(null) }
    var cameraProvider: ProcessCameraProvider? by remember { mutableStateOf(null) }
    val cameraExecutor: ExecutorService = remember { Executors.newSingleThreadExecutor() }
    val captureContext = LocalCameraCaptureContext.current
    var previewView: PreviewView? by remember { mutableStateOf(null) }

    LaunchedEffect(Unit) {
        cameraProvider = ProcessCameraProvider.getInstance(context).await()
    }

    LaunchedEffect(captureContext, imageCapture) {
        if (captureContext != null && imageCapture != null) {
            val capture = imageCapture!!
            captureContext.setCanCapture(true)
            captureContext.setCaptureCallback {
                scope.launch {
                    captureContext.setIsCapturing(true)
                    capture.takePicture(
                        cameraExecutor,
                        object : ImageCapture.OnImageCapturedCallback() {
                            override fun onError(exception: ImageCaptureException) {
                                Log.e(
                                    "DaybookCamera",
                                    "Image capture failed: ${exception.message}",
                                    exception
                                )
                                captureContext.setIsCapturing(false)
                            }

                            override fun onCaptureSuccess(image: ImageProxy) {
                                try {
                                    val bytes = imageProxyToJpeg(image)
                                    onImageSaved?.invoke(bytes)
                                } finally {
                                    image.close()
                                    captureContext.setIsCapturing(false)
                                }
                            }
                        }
                    )
                }
            }
        } else {
            captureContext?.setCanCapture(false)
            captureContext?.setCaptureCallback(null)
        }
    }

    LaunchedEffect(cameraProvider, previewView, onFrameAvailable) {
        val provider = cameraProvider ?: return@LaunchedEffect
        val pv = previewView ?: return@LaunchedEffect

        val preview =
            Preview
                .Builder()
                .setTargetAspectRatio(AspectRatio.RATIO_16_9)
                .build().also {
                    it.setSurfaceProvider(pv.surfaceProvider)
                }

        val imageCaptureUseCase =
            ImageCapture
                .Builder()
                .setCaptureMode(ImageCapture.CAPTURE_MODE_MINIMIZE_LATENCY)
                .setTargetAspectRatio(AspectRatio.RATIO_16_9)
                .build()
        imageCapture = imageCaptureUseCase

        val imageAnalysisUseCase =
            if (onFrameAvailable != null) {
                ImageAnalysis
                    .Builder()
                    .setTargetAspectRatio(AspectRatio.RATIO_16_9)
                    .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                    .build().also { analysis ->
                        analysis.setAnalyzer(cameraExecutor) { image ->
                            try {
                                val jpeg = imageProxyToJpeg(image)
                                onFrameAvailable.invoke(
                                    CameraFrameSample(
                                        widthPx = image.width,
                                        heightPx = image.height,
                                        jpegBytes = jpeg
                                    )
                                )
                            } catch (error: Throwable) {
                                Log.e("DaybookCamera", "Frame analysis failed", error)
                            } finally {
                                image.close()
                            }
                        }
                    }
            } else {
                null
            }

        val cameraSelector = CameraSelector.DEFAULT_BACK_CAMERA
        try {
            provider.unbindAll()
            if (imageAnalysisUseCase != null) {
                provider.bindToLifecycle(
                    lifecycleOwner,
                    cameraSelector,
                    preview,
                    imageCaptureUseCase,
                    imageAnalysisUseCase
                )
            } else {
                provider.bindToLifecycle(
                    lifecycleOwner,
                    cameraSelector,
                    preview,
                    imageCaptureUseCase
                )
            }
        } catch (exc: Exception) {
            println("Use case binding failed: ${exc.message}")
        }
    }

    DisposableEffect(Unit) {
        onDispose {
            cameraProvider?.unbindAll()
            cameraExecutor.shutdown()
            captureContext?.setCaptureCallback(null)
            captureContext?.setCanCapture(false)
        }
    }

    Box(modifier = modifier.fillMaxSize()) {
        AndroidView(
            factory = { ctx ->
                PreviewView(ctx).also { previewView = it }
            },
            modifier = Modifier.fillMaxSize()
        )
    }

    val _unused = cameraPreviewFfi
    val _unusedDevice = selectedDeviceId
    val _unusedDevicesCb = onAvailableDevicesChanged
}
