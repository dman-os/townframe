package org.example.daybook

import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.asPaddingValues
import androidx.compose.foundation.layout.calculateEndPadding
import androidx.compose.foundation.layout.calculateStartPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLayoutDirection
import androidx.compose.ui.tooling.preview.Preview
import androidx.core.content.ContextCompat
import com.mohamedrejeb.calf.permissions.ExperimentalPermissionsApi
import com.mohamedrejeb.calf.permissions.Permission
import com.mohamedrejeb.calf.permissions.isGranted

import com.mohamedrejeb.calf.permissions.rememberMultiplePermissionsState
import androidx.core.net.toUri
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import org.example.daybook.theme.ThemeConfig

class MainActivity : ComponentActivity() {
    private val drawOverlayPermissionLauncher =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) {
            // Check permission again after returning from settings
            if (Settings.canDrawOverlays(this)) {
                // Permission granted, now you can start your service
                // startOverlayService()
            } else {
                // Permission still not granted, handle accordingly
            }
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        setContent {
            AndroidApp()
        }
    }
}

@OptIn(ExperimentalPermissionsApi::class)
@Composable
fun AndroidApp() {
    val context = LocalContext.current

    var hasOverlayPermission by remember { mutableStateOf(Settings.canDrawOverlays(context)) }

    val permCtx = run {
        val notificationPermissionState = rememberMultiplePermissionsState(
            listOf(
                Permission.Notification,
                Permission.Camera,
                Permission.RecordAudio,
            )
        )
        val lifecycleOwner = LocalLifecycleOwner.current
        // Observe lifecycle events to refresh permission status when app resumes
        DisposableEffect(lifecycleOwner) {
            val observer = LifecycleEventObserver { _, event ->
                if (event == Lifecycle.Event.ON_RESUME) {
                    hasOverlayPermission = Settings.canDrawOverlays(context)
                }
            }
            lifecycleOwner.lifecycle.addObserver(observer)

            onDispose {
                lifecycleOwner.lifecycle.removeObserver(observer)
            }
        }
        PermissionsContext(
            hasOverlay = hasOverlayPermission,
            hasNotifications =
                notificationPermissionState.permissions[0].status.isGranted,
            hasCamera =
                notificationPermissionState.permissions[1].status.isGranted,
            hasMicrophone =
                notificationPermissionState.permissions[2].status.isGranted,
            requestAllPermissions = {
                notificationPermissionState.launchMultiplePermissionRequest()
                if (!hasOverlayPermission) {
                    // Request permission
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                        val intent = Intent(
                            Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                            "package:${context.packageName}".toUri()
                        )
                        context.startActivity(intent)
                    } else {
                        // Pre-M, permission is granted if declared, though this case is rare now
                        // and Settings.canDrawOverlays should already be true if manifest perm is there.
                        // However, for safety, you might re-check or assume it's okay if declared.
                        hasOverlayPermission =
                            Settings.canDrawOverlays(context) // Re-check for safety
                        if (!hasOverlayPermission) {
                            throw Exception("impossible");
                        }
                    }
                }
            }
        )
    }

    val theme = run {
        val isDarkMode = isSystemInDarkTheme()
        when {
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> {
                val context = LocalContext.current
                ThemeConfig.Custom(

                    if (isDarkMode) dynamicDarkColorScheme(context) else dynamicLightColorScheme(
                        context
                    )
                )
            }

            isDarkMode -> ThemeConfig.Light
            else -> ThemeConfig.Light
        }
    }
    val config = run {
        AppConfig(
            theme = theme
        )
    }

    CompositionLocalProvider(
        LocalPermCtx provides permCtx,
        LocalPlatform provides AndroidPlatform(context)
    ) {
        val layoutDirection = LocalLayoutDirection.current
        App(
            surfaceModifier = Modifier
                .padding(
                    start = WindowInsets.safeDrawing.asPaddingValues()
                        .calculateStartPadding(layoutDirection),
                    end = WindowInsets.safeDrawing.asPaddingValues()
                        .calculateEndPadding(layoutDirection)
                ),
            config = config,
            extraAction = {
                if (hasOverlayPermission) {
                    startMagicWandService(context)
                }
            }
        )
    }
}


fun startMagicWandService(context: Context) {
    val serviceIntent = Intent(context, MagicWandService::class.java)
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
        ContextCompat.startForegroundService(context, serviceIntent)
    } else {
        context.startService(serviceIntent)
    }
}
