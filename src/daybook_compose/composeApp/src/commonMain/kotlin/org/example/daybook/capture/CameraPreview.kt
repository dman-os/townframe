package org.example.daybook.capture

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier

@Composable
expect fun DaybookCameraPreview(
    modifier: Modifier = Modifier,
    onImageSaved: ((ByteArray) -> Unit)? = null,
    onCaptureRequested: (() -> Unit)? = null,
)
