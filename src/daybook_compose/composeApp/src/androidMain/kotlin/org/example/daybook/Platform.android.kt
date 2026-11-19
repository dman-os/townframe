package org.example.daybook

import android.content.Context
import android.os.Build
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

class AndroidPlatform(private val context: Context) : Platform {
    override val name: String = "Android ${Build.VERSION.SDK_INT}"

    override fun getScreenWidthDp(): Dp {
        val configuration = context.resources.configuration
        return configuration.screenWidthDp.dp
    }
}
