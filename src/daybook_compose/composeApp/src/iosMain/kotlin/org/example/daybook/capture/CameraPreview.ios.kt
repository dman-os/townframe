package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import org.example.daybook.capture.data.CameraFrameSample
import org.example.daybook.uniffi.CameraDeviceInfo
import org.example.daybook.uniffi.CameraPreviewFfi

@Composable
actual fun DaybookCameraPreview(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier,
    selectedDeviceId: Int?,
    onAvailableDevicesChanged: ((List<CameraDeviceInfo>, Int?) -> Unit)?,
    onImageSaved: ((ByteArray) -> Unit)?,
    onFrameAvailable: ((CameraFrameSample) -> Unit)?
) {
    // TODO: Implement iOS camera preview using CameraK when iOS support is added
    Box(
        modifier = modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Text("Camera preview not yet implemented on iOS")
    }
}
