package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier

@Composable
actual fun DaybookCameraPreview(
    modifier: Modifier,
    onImageSaved: ((ByteArray) -> Unit)?,
) {
    // TODO: Implement iOS camera preview using CameraK when iOS support is added
    Box(
        modifier = modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Text("Camera preview not yet implemented on iOS")
    }
}






