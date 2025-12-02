@file:OptIn(ExperimentalCamera2Interop::class)

package org.example.daybook.capture

import android.content.ContentValues
import android.content.Context
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CaptureRequest
import android.os.Build
import android.provider.MediaStore
import android.util.Range
import androidx.camera.camera2.interop.Camera2Interop
import androidx.camera.camera2.interop.ExperimentalCamera2Interop
import androidx.camera.core.AspectRatio
import androidx.camera.core.Camera
import androidx.camera.core.CameraSelector
import androidx.camera.core.ExtendableBuilder
import androidx.camera.core.ImageCapture
import androidx.camera.core.ImageCaptureException
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.Slider
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import androidx.lifecycle.compose.LocalLifecycleOwner
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch
import org.example.daybook.LocalPermCtx
import org.example.daybook.capture.LocalCameraCaptureContext
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Composable
actual fun DaybookCameraPreview(
    modifier: Modifier,
    onImageSaved: ((ByteArray) -> Unit)?,
    onCaptureRequested: (() -> Unit)?,
) {
    val permCtx = LocalPermCtx.current
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val scope = rememberCoroutineScope()
    
    // Request camera permission if not granted
    if (permCtx != null && !permCtx.hasCamera) {
        LaunchedEffect(Unit) {
            permCtx.requestAllPermissions()
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
    var camera: Camera? by remember { mutableStateOf(null) }
    var cameraProvider: ProcessCameraProvider? by remember { mutableStateOf(null) }
    val cameraExecutor: ExecutorService = remember { Executors.newSingleThreadExecutor() }
    val captureContext = LocalCameraCaptureContext.current
    
    // ISO and frame rate state
    var isoRange: Range<Int>? by remember { mutableStateOf(null) }
    var frameRateRanges: Array<Range<Int>>? by remember { mutableStateOf(null) }
    var currentIso by remember { mutableIntStateOf(100) }
    var currentFrameRate by remember { mutableIntStateOf(30) }
    var showControls by remember { mutableStateOf(false) }
    
    // Initialize camera provider and query characteristics
    LaunchedEffect(Unit) {
        val provider = ProcessCameraProvider.getInstance(context).await()
        cameraProvider = provider
        
        // Query camera characteristics using CameraManager
        val cameraManager = context.getSystemService(Context.CAMERA_SERVICE) as CameraManager
        val cameraId = cameraManager.cameraIdList.firstOrNull { id ->
            val characteristics = cameraManager.getCameraCharacteristics(id)
            characteristics.get(CameraCharacteristics.LENS_FACING) == CameraSelector.LENS_FACING_BACK
        } ?: cameraManager.cameraIdList.firstOrNull()
        
        cameraId?.let { id ->
            val characteristics = cameraManager.getCameraCharacteristics(id)
            
            // Get ISO range
            characteristics.get(CameraCharacteristics.SENSOR_INFO_SENSITIVITY_RANGE)?.let { range ->
                isoRange = range
                currentIso = range.lower
            }
            
            // Get frame rate ranges
            characteristics.get(CameraCharacteristics.CONTROL_AE_AVAILABLE_TARGET_FPS_RANGES)?.let { ranges ->
                frameRateRanges = ranges
                ranges.firstOrNull()?.let { range ->
                    currentFrameRate = range.upper
                }
            }
        }
    }
    
    // Set up capture callback
    LaunchedEffect(captureContext, imageCapture) {
        if (captureContext != null && imageCapture != null) {
            val capture = imageCapture!!
            captureContext.setCanCapture(true)
            captureContext.setCaptureCallback {
                scope.launch {
                    // Create time stamped name
                    val name = SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS", Locale.US)
                        .format(System.currentTimeMillis())
                    val contentValues = ContentValues().apply {
                        put(MediaStore.MediaColumns.DISPLAY_NAME, name)
                        put(MediaStore.MediaColumns.MIME_TYPE, "image/jpeg")
                        if (Build.VERSION.SDK_INT > Build.VERSION_CODES.P) {
                            put(MediaStore.Images.Media.RELATIVE_PATH, "Pictures/Daybook")
                        }
                    }
                    
                    // Create output options
                    val outputOptions = ImageCapture.OutputFileOptions
                        .Builder(
                            context.contentResolver,
                            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
                            contentValues
                        )
                        .build()
                    
                    captureContext.setIsCapturing(true)
                    
                    // Take picture
                    capture.takePicture(
                        outputOptions,
                        cameraExecutor,
                        object : ImageCapture.OnImageSavedCallback {
                            override fun onError(exception: ImageCaptureException) {
                                println("Image capture failed: ${exception.message}")
                                captureContext.setIsCapturing(false)
                            }
                            
                            override fun onImageSaved(output: ImageCapture.OutputFileResults) {
                                // Read the saved image as ByteArray
                                output.savedUri?.let { uri ->
                                    context.contentResolver.openInputStream(uri)?.use { inputStream ->
                                        val byteArray = inputStream.readBytes()
                                        onImageSaved?.invoke(byteArray)
                                    }
                                }
                                captureContext.setIsCapturing(false)
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
    
    var previewView: PreviewView? by remember { mutableStateOf(null) }
    
    // Helper function to attach settings to use case builder
    @OptIn(ExperimentalCamera2Interop::class)
    fun attachSettingsTo(useCaseBuilder: ExtendableBuilder<*>) {
        Camera2Interop.Extender(useCaseBuilder).apply {
            // Disable auto exposure to enable manual ISO
            setCaptureRequestOption(
                CaptureRequest.CONTROL_AE_MODE,
                CaptureRequest.CONTROL_AE_MODE_OFF
            )
            
            // Set ISO
            isoRange?.let { range ->
                val clampedIso = currentIso.coerceIn(range.lower, range.upper)
                setCaptureRequestOption(
                    CaptureRequest.SENSOR_SENSITIVITY,
                    clampedIso
                )
            }
            
            // Set frame rate using target FPS range
            frameRateRanges?.let { ranges ->
                val fpsRange = Range(currentFrameRate, currentFrameRate)
                setCaptureRequestOption(
                    CaptureRequest.CONTROL_AE_TARGET_FPS_RANGE,
                    fpsRange
                )
            }
        }
    }
    
    // Rebind camera when ISO or frame rate changes
    LaunchedEffect(currentIso, currentFrameRate, cameraProvider, previewView) {
        val provider = cameraProvider ?: return@LaunchedEffect
        val pv = previewView ?: return@LaunchedEffect
        
        // Preview use case with Camera2Interop
        val previewBuilder = Preview.Builder()
            .setTargetAspectRatio(AspectRatio.RATIO_16_9)
        
        attachSettingsTo(previewBuilder)
        
        val preview = previewBuilder.build().also {
            it.setSurfaceProvider(pv.surfaceProvider)
        }
        
        // Image capture use case
        val imageCaptureBuilder = ImageCapture.Builder()
            .setCaptureMode(ImageCapture.CAPTURE_MODE_MINIMIZE_LATENCY)
            .setTargetAspectRatio(AspectRatio.RATIO_16_9)
        
        attachSettingsTo(imageCaptureBuilder)
        
        val imageCaptureUseCase = imageCaptureBuilder.build()
        imageCapture = imageCaptureUseCase
        
        val cameraSelector = CameraSelector.DEFAULT_BACK_CAMERA
        
        try {
            provider.unbindAll()
            camera = provider.bindToLifecycle(
                lifecycleOwner,
                cameraSelector,
                preview,
                imageCaptureUseCase
            )
        } catch (exc: Exception) {
            println("Use case binding failed: ${exc.message}")
        }
    }
    
    // Cleanup executor on dispose
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
        
        // Controls overlay
        Column(
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .fillMaxWidth()
                .padding(16.dp)
        ) {
            // Toggle controls button
            Button(
                onClick = { showControls = !showControls }
            ) {
                Text(if (showControls) "Hide Controls" else "Show Controls")
            }
            
            // ISO and Frame Rate sliders
            if (showControls) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 8.dp)
                ) {
                    // ISO Slider
                    isoRange?.let { range ->
                        Text("ISO: $currentIso (Range: ${range.lower}-${range.upper})")
                        Slider(
                            value = currentIso.toFloat(),
                            onValueChange = { currentIso = it.toInt() },
                            valueRange = range.lower.toFloat()..range.upper.toFloat(),
                            steps = ((range.upper - range.lower) / 100).coerceAtMost(100)
                        )
                    }
                    
                    // Frame Rate Slider
                    frameRateRanges?.let { ranges ->
                        val minFps = ranges.minOfOrNull { it.lower } ?: 15
                        val maxFps = ranges.maxOfOrNull { it.upper } ?: 30
                        Text("Frame Rate: ${currentFrameRate}fps (Range: ${minFps}-${maxFps})")
                        Slider(
                            value = currentFrameRate.toFloat(),
                            onValueChange = { currentFrameRate = it.toInt() },
                            valueRange = minFps.toFloat()..maxFps.toFloat(),
                            steps = (maxFps - minFps).coerceAtMost(30)
                        )
                    }
                }
            }
        }
    }
}
