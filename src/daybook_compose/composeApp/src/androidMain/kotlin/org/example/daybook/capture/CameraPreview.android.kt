package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.kashif.cameraK.controller.CameraController
import com.kashif.cameraK.enums.CameraLens
import com.kashif.cameraK.ui.CameraPreview
import com.kashif.cameraK.enums.FlashMode
import com.kashif.cameraK.result.ImageCaptureResult
import com.kashif.cameraK.enums.ImageFormat
import com.kashif.imagesaverplugin.ImageSaverConfig
import com.kashif.cameraK.enums.Directory
import com.kashif.imagesaverplugin.rememberImageSaverPlugin
import kotlinx.coroutines.launch
import org.example.daybook.LocalPermCtx

@Composable
actual fun DaybookCameraPreview(
    modifier: Modifier,
    onImageSaved: ((ByteArray) -> Unit)?,
) {
    val permCtx = LocalPermCtx.current
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
    
    val cameraController = remember { mutableStateOf<CameraController?>(null) }
    
    // Configure image saver plugin
    val imageSaverPlugin = rememberImageSaverPlugin(
        config = ImageSaverConfig(
            isAutoSave = false,
            prefix = "Daybook",
            directory = Directory.PICTURES,
            customFolderName = "DaybookPhotos"
        )
    )
    
    Box(modifier = modifier.fillMaxSize()) {
        CameraPreview(
            modifier = Modifier.fillMaxSize(),
            cameraConfiguration = {
                setCameraLens(CameraLens.BACK)
                setFlashMode(FlashMode.OFF)
                setImageFormat(ImageFormat.JPEG)
                setDirectory(Directory.PICTURES)
                addPlugin(imageSaverPlugin)
            },
            onCameraControllerReady = {
                cameraController.value = it
            }
        )
        
        // Save button
        Button(
            onClick = {
                cameraController.value?.let { controller ->
                    scope.launch {
                        when (val result = controller.takePicture()) {
                            is ImageCaptureResult.Success -> {
                                // Save the image using the plugin
                                imageSaverPlugin.saveImage(
                                    byteArray = result.byteArray,
                                    imageName = "Photo_${System.currentTimeMillis()}"
                                )
                                // Callback if provided
                                onImageSaved?.invoke(result.byteArray)
                            }
                            is ImageCaptureResult.Error -> {
                                // Handle error - could show a snackbar or toast
                                println("Image Capture Error: ${result.exception.message}")
                            }
                        }
                    }
                }
            },
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .padding(16.dp)
        ) {
            Text("Save Photo")
        }
    }
}


