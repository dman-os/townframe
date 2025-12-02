package org.example.daybook.capture

import android.content.ContentValues
import android.graphics.Bitmap
import android.os.Build
import android.provider.MediaStore
import androidx.camera.core.AspectRatio
import androidx.camera.core.Camera
import androidx.camera.core.CameraSelector
import androidx.camera.core.FocusMeteringAction
import androidx.camera.core.ImageCapture
import androidx.camera.core.ImageCaptureException
import androidx.camera.core.MeteringPoint
import androidx.camera.core.Preview
import androidx.camera.core.UseCaseGroup
import androidx.camera.extensions.ExtensionMode
import androidx.camera.extensions.ExtensionsManager
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.gestures.detectTransformGestures
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
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

/**
 * Enhanced camera preview with CameraX Extensions support
 */
@Composable
fun DaybookCameraPreviewEnhanced(
    modifier: Modifier = Modifier,
    onImageSaved: ((ByteArray) -> Unit)? = null,
    onCaptureRequested: (() -> Unit)? = null,
) {
    val permCtx = LocalPermCtx.current
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val scope = rememberCoroutineScope()
    val density = LocalDensity.current
    
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
    
    // Camera state
    var cameraProvider: ProcessCameraProvider? by remember { mutableStateOf(null) }
    var extensionsManager: ExtensionsManager? by remember { mutableStateOf(null) }
    var camera: Camera? by remember { mutableStateOf(null) }
    var imageCapture: ImageCapture? by remember { mutableStateOf(null) }
    var preview: Preview? by remember { mutableStateOf(null) }
    var previewView: PreviewView? by remember { mutableStateOf(null) }
    val cameraExecutor: ExecutorService = remember { Executors.newSingleThreadExecutor() }
    val captureContext = LocalCameraCaptureContext.current
    
    // Extension state
    var extensionsState by remember {
        mutableStateOf(CameraExtensionsState())
    }
    
    // UI state
    var showExtensionSelector by remember { mutableStateOf(false) }
    var postviewBitmap by remember { mutableStateOf<Bitmap?>(null) }
    var captureProgress by remember { mutableIntStateOf(0) }
    var isCapturing by remember { mutableStateOf(false) }
    var currentZoomRatio by remember { mutableFloatStateOf(1f) }
    
    // Initialize camera and extensions manager
    LaunchedEffect(Unit) {
        val provider = ProcessCameraProvider.getInstance(context).await()
        val extensions = ExtensionsManager.getInstanceAsync(context, provider).await()
        cameraProvider = provider
        extensionsManager = extensions
        
        // Query available extensions and lenses
        val availableLens = listOf(
            CameraSelector.LENS_FACING_BACK,
            CameraSelector.LENS_FACING_FRONT
        ).filter { lens ->
            provider.hasCamera(
                when (lens) {
                    CameraSelector.LENS_FACING_BACK -> CameraSelector.DEFAULT_BACK_CAMERA
                    CameraSelector.LENS_FACING_FRONT -> CameraSelector.DEFAULT_FRONT_CAMERA
                    else -> CameraSelector.DEFAULT_BACK_CAMERA
                }
            )
        }
        
        val defaultLens = availableLens.firstOrNull() ?: CameraSelector.LENS_FACING_BACK
        val defaultSelector = when (defaultLens) {
            CameraSelector.LENS_FACING_BACK -> CameraSelector.DEFAULT_BACK_CAMERA
            CameraSelector.LENS_FACING_FRONT -> CameraSelector.DEFAULT_FRONT_CAMERA
            else -> CameraSelector.DEFAULT_BACK_CAMERA
        }
        
        val availableExtensions = listOf(
            ExtensionMode.AUTO,
            ExtensionMode.BOKEH,
            ExtensionMode.HDR,
            ExtensionMode.NIGHT,
            ExtensionMode.FACE_RETOUCH
        ).filter { mode ->
            val isAvailable = extensions.isExtensionAvailable(defaultSelector, mode)
            println("CameraX Extension ${getExtensionModeName(mode)}: ${if (isAvailable) "AVAILABLE" else "NOT AVAILABLE"}")
            isAvailable
        }
        
        println("CameraX Extensions: Found ${availableExtensions.size} available extensions out of 5 checked")
        
        extensionsState = CameraExtensionsState(
            availableExtensions = listOf(ExtensionMode.NONE) + availableExtensions,
            selectedExtension = ExtensionMode.NONE,
            availableLens = availableLens,
            selectedLens = defaultLens,
            isInitialized = true
        )
    }
    
    // Bind camera when state changes
    LaunchedEffect(cameraProvider, extensionsManager, extensionsState, previewView, lifecycleOwner) {
        val provider = cameraProvider ?: return@LaunchedEffect
        val extensions = extensionsManager ?: return@LaunchedEffect
        val pv = previewView ?: return@LaunchedEffect
        if (!extensionsState.isInitialized) return@LaunchedEffect
        
        // Wait for viewPort to be available
        while (pv.viewPort == null) {
            kotlinx.coroutines.delay(16) // Wait one frame
        }
        
        // Determine camera selector
        val baseSelector = when (extensionsState.selectedLens) {
            CameraSelector.LENS_FACING_BACK -> CameraSelector.DEFAULT_BACK_CAMERA
            CameraSelector.LENS_FACING_FRONT -> CameraSelector.DEFAULT_FRONT_CAMERA
            else -> CameraSelector.DEFAULT_BACK_CAMERA
        }
        
        val cameraSelector = if (extensionsState.selectedExtension == ExtensionMode.NONE) {
            baseSelector
        } else {
            extensions.getExtensionEnabledCameraSelector(
                baseSelector,
                extensionsState.selectedExtension
            )
        }
        
        // Create preview
        val previewUseCase = Preview.Builder()
            .setTargetAspectRatio(AspectRatio.RATIO_16_9)
            .build()
            .also {
                it.setSurfaceProvider(pv.surfaceProvider)
            }
        preview = previewUseCase
        
        // Create image capture with postview support
        provider.unbindAll()
        
        // First bind to get camera info
        val tempCamera = provider.bindToLifecycle(lifecycleOwner, cameraSelector)
        val isPostviewSupported = ImageCapture.getImageCaptureCapabilities(tempCamera.cameraInfo).isPostviewSupported
        
        provider.unbindAll()
        
        val captureUseCase = ImageCapture.Builder()
            .setTargetAspectRatio(AspectRatio.RATIO_16_9)
            .setCaptureMode(ImageCapture.CAPTURE_MODE_MINIMIZE_LATENCY)
            .setPostviewEnabled(isPostviewSupported)
            .build()
        
        imageCapture = captureUseCase
        
        // Bind use cases with viewPort
        val useCaseGroup = UseCaseGroup.Builder()
            .setViewPort(pv.viewPort!!)
            .addUseCase(previewUseCase)
            .addUseCase(captureUseCase)
            .build()
        
        camera = provider.bindToLifecycle(
            lifecycleOwner,
            cameraSelector,
            useCaseGroup
        )
        
        // Update zoom ratio
        camera?.cameraInfo?.zoomState?.value?.let { zoomState ->
            currentZoomRatio = zoomState.zoomRatio
        }
    }
    
    // Set up capture callback
    LaunchedEffect(captureContext, imageCapture) {
        if (captureContext != null && imageCapture != null) {
            val capture = imageCapture!!
            captureContext.setCanCapture(true)
            captureContext.setCaptureCallback {
                scope.launch {
                    capturePhoto(
                        capture = capture,
                        context = context,
                        cameraExecutor = cameraExecutor,
                        captureContext = captureContext,
                        onImageSaved = onImageSaved,
                        onProgress = { captureProgress = it },
                        onPostview = { postviewBitmap = it },
                        onCapturing = { isCapturing = it }
                    )
                }
            }
        } else {
            captureContext?.setCanCapture(false)
            captureContext?.setCaptureCallback(null)
        }
    }
    
    // Cleanup
    DisposableEffect(Unit) {
        onDispose {
            cameraProvider?.unbindAll()
            cameraExecutor.shutdown()
            captureContext?.setCaptureCallback(null)
            captureContext?.setCanCapture(false)
        }
    }
    
    Box(modifier = modifier.fillMaxSize()) {
        // Camera preview
        AndroidView(
            factory = { ctx ->
                PreviewView(ctx).also { previewView = it }
            },
            modifier = Modifier
                .fillMaxSize()
                .pointerInput(camera, previewView) {
                    val cam = camera
                    val pv = previewView
                    
                    // Tap to focus
                    detectTapGestures { tapOffset ->
                        cam?.let { c ->
                            pv?.let { view ->
                                val meteringPointFactory = view.meteringPointFactory
                                val focusPoint = meteringPointFactory.createPoint(tapOffset.x, tapOffset.y)
                                val meteringAction = FocusMeteringAction.Builder(focusPoint).build()
                                c.cameraControl.startFocusAndMetering(meteringAction)
                            }
                        }
                    }
                    
                    // Pinch to zoom
                    detectTransformGestures { _, _, zoom, _ ->
                        cam?.let { c ->
                            val zoomState = c.cameraInfo.zoomState.value
                            val currentZoom = zoomState?.zoomRatio ?: 1f
                            val newZoom = (currentZoom * zoom).coerceIn(
                                zoomState?.minZoomRatio ?: 1f,
                                zoomState?.maxZoomRatio ?: 1f
                            )
                            c.cameraControl.setZoomRatio(newZoom)
                            currentZoomRatio = newZoom
                        }
                    }
                }
        )
        
        // Extension selector UI
        if (showExtensionSelector && extensionsState.isInitialized) {
            Card(
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .padding(16.dp)
                    .width(300.dp)
                    .height(200.dp),
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.9f)
                )
            ) {
                Column(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(8.dp)
                        .verticalScroll(rememberScrollState())
                ) {
                    Text(
                        "Extensions",
                        style = MaterialTheme.typography.titleMedium,
                        modifier = Modifier.padding(bottom = 8.dp)
                    )
                    extensionsState.availableExtensions.forEach { mode ->
                        TextButton(
                            onClick = {
                                extensionsState = extensionsState.copy(selectedExtension = mode)
                                showExtensionSelector = false
                            },
                            modifier = Modifier.fillMaxWidth()
                        ) {
                            Text(
                                getExtensionModeName(mode),
                                color = if (extensionsState.selectedExtension == mode) {
                                    MaterialTheme.colorScheme.primary
                                } else {
                                    MaterialTheme.colorScheme.onSurface
                                }
                            )
                        }
                    }
                }
            }
        }
        
        // Controls overlay
        Column(
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .fillMaxWidth()
                .padding(16.dp)
        ) {
            // Extension selector button
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = androidx.compose.foundation.layout.Arrangement.SpaceBetween
            ) {
                Button(
                    onClick = { showExtensionSelector = !showExtensionSelector },
                    modifier = Modifier.weight(1f)
                ) {
                    Text(getExtensionModeName(extensionsState.selectedExtension))
                }
                
                Spacer(modifier = Modifier.width(8.dp))
                
                // Camera switch button
                if (extensionsState.availableLens.size > 1) {
                    IconButton(
                        onClick = {
                            val newLens = if (extensionsState.selectedLens == CameraSelector.LENS_FACING_BACK) {
                                CameraSelector.LENS_FACING_FRONT
                            } else {
                                CameraSelector.LENS_FACING_BACK
                            }
                            extensionsState = extensionsState.copy(selectedLens = newLens)
                        }
                    ) {
                        Text("🔄")
                    }
                }
            }
            
            // Postview display
            postviewBitmap?.let { bitmap ->
                Card(
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(200.dp)
                        .padding(vertical = 8.dp),
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.surface
                    )
                ) {
                    // Display postview bitmap using Android ImageView
                    AndroidView(
                        factory = { ctx ->
                            android.widget.ImageView(ctx).apply {
                                scaleType = android.widget.ImageView.ScaleType.CENTER_CROP
                                setImageBitmap(bitmap)
                            }
                        },
                        modifier = Modifier
                            .fillMaxSize()
                            .clip(RoundedCornerShape(8.dp))
                    )
                }
            }
            
            // Capture progress
            if (isCapturing && captureProgress > 0) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(vertical = 8.dp),
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    CircularProgressIndicator(progress = { captureProgress / 100f })
                    Spacer(modifier = Modifier.height(8.dp))
                    Text("Processing: $captureProgress%")
                }
            }
        }
    }
}

/**
 * Capture photo with extensions support
 */
private suspend fun capturePhoto(
    capture: ImageCapture,
    context: android.content.Context,
    cameraExecutor: ExecutorService,
    captureContext: CameraCaptureContext?,
    onImageSaved: ((ByteArray) -> Unit)?,
    onProgress: (Int) -> Unit,
    onPostview: (Bitmap) -> Unit,
    onCapturing: (Boolean) -> Unit
) {
    val name = SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS", Locale.US)
        .format(System.currentTimeMillis())
    val contentValues = ContentValues().apply {
        put(MediaStore.MediaColumns.DISPLAY_NAME, name)
        put(MediaStore.MediaColumns.MIME_TYPE, "image/jpeg")
        if (Build.VERSION.SDK_INT > Build.VERSION_CODES.P) {
            put(MediaStore.Images.Media.RELATIVE_PATH, "Pictures/Daybook")
        }
    }
    
    val outputOptions = ImageCapture.OutputFileOptions
        .Builder(
            context.contentResolver,
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            contentValues
        )
        .build()
    
    onCapturing(true)
    captureContext?.setIsCapturing(true)
    
    capture.takePicture(
        outputOptions,
        cameraExecutor,
        object : ImageCapture.OnImageSavedCallback {
            override fun onError(exception: ImageCaptureException) {
                println("Image capture failed: ${exception.message}")
                onCapturing(false)
                captureContext?.setIsCapturing(false)
                onProgress(0)
            }
            
            override fun onImageSaved(output: ImageCapture.OutputFileResults) {
                output.savedUri?.let { uri ->
                    context.contentResolver.openInputStream(uri)?.use { inputStream ->
                        val byteArray = inputStream.readBytes()
                        onImageSaved?.invoke(byteArray)
                    }
                }
                onCapturing(false)
                captureContext?.setIsCapturing(false)
                onProgress(0)
            }
            
            override fun onCaptureProcessProgressed(progress: Int) {
                onProgress(progress)
            }
            
            override fun onPostviewBitmapAvailable(bitmap: Bitmap) {
                onPostview(bitmap)
            }
        }
    )
}
