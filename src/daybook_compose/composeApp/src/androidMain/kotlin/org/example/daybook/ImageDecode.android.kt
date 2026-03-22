package org.example.daybook

import android.graphics.BitmapFactory
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.asImageBitmap

actual fun decodePngImageBitmap(pngBytes: ByteArray): ImageBitmap? {
    val bitmap = BitmapFactory.decodeByteArray(pngBytes, 0, pngBytes.size) ?: return null
    return bitmap.asImageBitmap()
}
