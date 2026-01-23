package org.example.daybook

import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.WindowState
import java.awt.Toolkit

class JVMPlatform : Platform {
    override val name: String = "Java ${System.getProperty("java.version")}"

    override fun getScreenWidthDp(): Dp {
        // Fallback to screen size
        val screenSize = Toolkit.getDefaultToolkit().screenSize
        val screenWidth = screenSize.width
        // Convert pixels to dp (assuming 96 DPI for desktop)
        val dpi = 96f
        val dp = (screenWidth / dpi) * 160f
        return dp.dp
    }
}

@Composable
fun createReactiveJVMPlatform(windowState: WindowState): Platform = object : Platform {
    override val name: String = "Java ${System.getProperty("java.version")}"

    override fun getScreenWidthDp(): Dp = windowState.size.width
}
