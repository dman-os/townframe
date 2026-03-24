package org.example.daybook

import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.toComposeImageBitmap
import org.jetbrains.skia.Image as SkiaImage

actual fun decodePngImageBitmap(pngBytes: ByteArray): ImageBitmap? =
    try {
        SkiaImage.makeFromEncoded(pngBytes).toComposeImageBitmap()
    } catch (error: IllegalArgumentException) {
        null
    }
