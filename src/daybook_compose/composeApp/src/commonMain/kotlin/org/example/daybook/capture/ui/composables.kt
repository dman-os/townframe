package org.example.daybook.capture.ui

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.Alignment
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.dp
import org.example.daybook.capture.DaybookCameraPreview
import org.example.daybook.capture.data.CameraFrameSample
import org.example.daybook.capture.data.CameraOverlay
import org.example.daybook.uniffi.CameraDeviceInfo
import org.example.daybook.uniffi.CameraPreviewFfi

@Composable
fun DaybookCameraViewport(
    cameraPreviewFfi: CameraPreviewFfi,
    modifier: Modifier = Modifier,
    overlays: List<CameraOverlay> = emptyList(),
    onImageSaved: ((ByteArray) -> Unit)? = null,
    onFrameAvailable: ((CameraFrameSample) -> Unit)? = null,
    showCameraSelector: Boolean = true
) {
    var availableDevices by remember { mutableStateOf<List<CameraDeviceInfo>>(emptyList()) }
    var selectedDeviceId by remember { mutableStateOf<Int?>(null) }
    var selectorExpanded by remember { mutableStateOf(false) }

    Box(modifier = modifier) {
        DaybookCameraPreview(
            cameraPreviewFfi = cameraPreviewFfi,
            modifier = Modifier.fillMaxSize(),
            selectedDeviceId = selectedDeviceId,
            onAvailableDevicesChanged = { devices, preferredId ->
                availableDevices = devices
                if (devices.isEmpty()) {
                    selectedDeviceId = null
                } else if (selectedDeviceId == null || devices.none { it.deviceId.toInt() == selectedDeviceId }) {
                    selectedDeviceId = preferredId ?: devices.first().deviceId.toInt()
                }
            },
            onImageSaved = onImageSaved,
            onFrameAvailable = onFrameAvailable
        )
        CameraOverlayLayer(
            overlays = overlays,
            modifier = Modifier.fillMaxSize()
        )
        if (showCameraSelector && availableDevices.isNotEmpty()) {
            Box(
                modifier =
                    Modifier
                        .align(Alignment.TopEnd)
                        .padding(8.dp)
            ) {
                val selectedLabel =
                    availableDevices
                        .firstOrNull { it.deviceId.toInt() == selectedDeviceId }
                        ?.label ?: "Select camera"
                Button(onClick = { selectorExpanded = !selectorExpanded }) {
                    Text(selectedLabel, style = MaterialTheme.typography.bodyMedium)
                }
                DropdownMenu(
                    expanded = selectorExpanded,
                    onDismissRequest = { selectorExpanded = false }
                ) {
                    availableDevices.forEach { device ->
                        DropdownMenuItem(
                            text = { Text(device.label) },
                            onClick = {
                                selectedDeviceId = device.deviceId.toInt()
                                selectorExpanded = false
                            }
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun CameraOverlayLayer(
    overlays: List<CameraOverlay>,
    modifier: Modifier = Modifier
) {
    if (overlays.isEmpty()) return
    Canvas(modifier = modifier) {
        overlays.forEach { overlay ->
            when (overlay) {
                is CameraOverlay.Grid -> {
                    val stroke = Stroke(width = 1.5f)
                    val color = Color.White.copy(alpha = 0.35f)
                    for (step in 1..2) {
                        val x = size.width * (step / 3f)
                        val y = size.height * (step / 3f)
                        drawLine(
                            color = color,
                            start = Offset(x, 0f),
                            end = Offset(x, size.height),
                            strokeWidth = stroke.width
                        )
                        drawLine(
                            color = color,
                            start = Offset(0f, y),
                            end = Offset(size.width, y),
                            strokeWidth = stroke.width
                        )
                    }
                }

                is CameraOverlay.QrBounds -> {
                    val left = overlay.left.coerceIn(0f, 1f) * size.width
                    val top = overlay.top.coerceIn(0f, 1f) * size.height
                    val right = overlay.right.coerceIn(0f, 1f) * size.width
                    val bottom = overlay.bottom.coerceIn(0f, 1f) * size.height
                    if (right > left && bottom > top) {
                        drawRect(
                            color = Color(0xFF4DD0E1),
                            topLeft = Offset(left, top),
                            size = androidx.compose.ui.geometry.Size(right - left, bottom - top),
                            style = Stroke(width = 3f)
                        )
                    }
                }
            }
        }
    }
}
