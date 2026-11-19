package org.example.daybook

import androidx.compose.ui.graphics.ImageBitmap

expect fun decodePngImageBitmap(pngBytes: ByteArray): ImageBitmap?
