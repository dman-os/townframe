package org.example.daybook.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.ColorScheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable

sealed class ThemeConfig(val colorsScheme: ColorScheme) {
    object Dark: ThemeConfig(darkColorScheme())
    object Light: ThemeConfig(lightColorScheme())
    class Custom(colorsScheme: ColorScheme): ThemeConfig(colorsScheme)
}

@Composable
fun DaybookTheme(
    themeConfig: ThemeConfig,
    content: @Composable () -> Unit,
) {
    MaterialTheme(
        colorScheme = themeConfig.colorsScheme,
        content = content,
    )
}