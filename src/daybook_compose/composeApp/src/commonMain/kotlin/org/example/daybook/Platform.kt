package org.example.daybook

import androidx.compose.runtime.Composable
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.ui.unit.Dp

interface Platform {
    val name: String
    fun getScreenWidthDp(): Dp
}

// CompositionLocal for accessing the platform
val LocalPlatform = compositionLocalOf<Platform> {
    error("No Platform provided")
}

@Composable
fun getPlatform(): Platform {
    return LocalPlatform.current
}
