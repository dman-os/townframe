package org.example.daybook.capture

import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import org.example.daybook.capture.data.CameraFrameSample
import org.example.daybook.uniffi.CameraDeviceInfo
import org.example.daybook.uniffi.CameraPreviewFfi

@Composable
expect fun DaybookCameraPreview(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier = Modifier,
    selectedDeviceId: Int? = null,
    onAvailableDevicesChanged: ((List<CameraDeviceInfo>, Int?) -> Unit)? = null,
    onImageSaved: ((ByteArray) -> Unit)? = null,
    onFrameAvailable: ((CameraFrameSample) -> Unit)? = null
)
