package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import org.example.daybook.uniffi.CameraPreviewFfi

@Composable
actual fun DaybookCameraPreview(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier,
    onImageSaved: ((ByteArray) -> Unit)?,
    onCaptureRequested: (() -> Unit)?
) {
    // Camera not available on WebAssembly
    Box(
        modifier = modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Text("Camera preview not available on WebAssembly")
    }
}
