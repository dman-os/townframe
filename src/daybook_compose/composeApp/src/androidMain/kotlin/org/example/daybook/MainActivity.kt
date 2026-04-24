package org.example.daybook

import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.compose.rememberLauncherForActivityResult
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
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLayoutDirection
import androidx.compose.ui.tooling.preview.Preview
import androidx.core.content.ContextCompat
import androidx.core.net.toUri
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.compose.LocalLifecycleOwner
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
                Toast.makeText(this, "Overlay permission denied", Toast.LENGTH_SHORT).show()
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

@Composable
fun AndroidApp() {
    val context = LocalContext.current
    val activity = context as? ComponentActivity

    var hasOverlayPermission by remember { mutableStateOf(Settings.canDrawOverlays(context)) }
    var permissionRefreshTick by remember { mutableIntStateOf(0) }
    var shutdownRequested by remember { mutableStateOf(false) }

    val permissionLauncher =
        rememberLauncherForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) {
            permissionRefreshTick += 1
        }

    val permCtx =
        run {
            fun hasRuntimePermission(name: String): Boolean =
                ContextCompat.checkSelfPermission(context, name) == PackageManager.PERMISSION_GRANTED

            fun hasNotificationsPermission(): Boolean =
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    hasRuntimePermission(Manifest.permission.POST_NOTIFICATIONS)
                } else {
                    true
                }

            fun hasStorageReadPermission(): Boolean =
                if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.S_V2) {
                    hasRuntimePermission(Manifest.permission.READ_EXTERNAL_STORAGE)
                } else {
                    true
                }

            fun hasStorageWritePermission(): Boolean =
                if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.P) {
                    hasRuntimePermission(Manifest.permission.WRITE_EXTERNAL_STORAGE)
                } else {
                    true
                }

            val lifecycleOwner = LocalLifecycleOwner.current
            // Observe lifecycle events to refresh permission status when app resumes
            DisposableEffect(lifecycleOwner) {
                val observer =
                    LifecycleEventObserver { _, event ->
                        if (event == Lifecycle.Event.ON_RESUME) {
                            hasOverlayPermission = Settings.canDrawOverlays(context)
                            permissionRefreshTick += 1
                        } else if (event == Lifecycle.Event.ON_DESTROY) {
                            // Ignore config-change destroys; shut down only when activity is actually finishing.
                            if (activity?.isFinishing != false) {
                                shutdownRequested = true
                            }
                        }
                    }
                lifecycleOwner.lifecycle.addObserver(observer)

                onDispose {
                    lifecycleOwner.lifecycle.removeObserver(observer)
                }
            }
            permissionRefreshTick
            PermissionsContext(
                hasOverlay = hasOverlayPermission,
                hasNotifications = hasNotificationsPermission(),
                hasCamera = hasRuntimePermission(Manifest.permission.CAMERA),
                hasMicrophone = hasRuntimePermission(Manifest.permission.RECORD_AUDIO),
                hasStorageRead = hasStorageReadPermission(),
                hasStorageWrite = hasStorageWritePermission(),
                requestPermissions = { request ->
                    val requestedPermissions = buildList {
                        if (request.camera && !hasRuntimePermission(Manifest.permission.CAMERA)) {
                            add(Manifest.permission.CAMERA)
                        }
                        if (request.microphone && !hasRuntimePermission(Manifest.permission.RECORD_AUDIO)) {
                            add(Manifest.permission.RECORD_AUDIO)
                        }
                        if (request.notifications &&
                            Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                            !hasRuntimePermission(Manifest.permission.POST_NOTIFICATIONS)
                        ) {
                            add(Manifest.permission.POST_NOTIFICATIONS)
                        }
                        if (request.storageRead &&
                            Build.VERSION.SDK_INT <= Build.VERSION_CODES.S_V2 &&
                            !hasRuntimePermission(Manifest.permission.READ_EXTERNAL_STORAGE)
                        ) {
                            add(Manifest.permission.READ_EXTERNAL_STORAGE)
                        }
                        if (request.storageWrite &&
                            Build.VERSION.SDK_INT <= Build.VERSION_CODES.P &&
                            !hasRuntimePermission(Manifest.permission.WRITE_EXTERNAL_STORAGE)
                        ) {
                            add(Manifest.permission.WRITE_EXTERNAL_STORAGE)
                        }
                    }

                    if (requestedPermissions.isNotEmpty()) {
                        permissionLauncher.launch(requestedPermissions.toTypedArray())
                    }

                    if (request.overlay && !hasOverlayPermission) {
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                            val intent =
                                Intent(
                                    Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                                    "package:${context.packageName}".toUri()
                                )
                            context.startActivity(intent)
                        } else {
                            hasOverlayPermission = Settings.canDrawOverlays(context)
                        }
                    }
                }
            )
        }

    val theme =
        run {
            val isDarkMode = isSystemInDarkTheme()
            when {
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> {
                    val context = LocalContext.current
                    ThemeConfig.Custom(
                        if (isDarkMode) {
                            dynamicDarkColorScheme(context)
                        } else {
                            dynamicLightColorScheme(
                                context
                            )
                        }
                    )
                }

                isDarkMode -> {
                    ThemeConfig.Dark
                }

                else -> {
                    ThemeConfig.Light
                }
            }
        }
    val config =
        run {
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
            surfaceModifier =
                Modifier
                    .padding(
                        start =
                            WindowInsets.safeDrawing
                                .asPaddingValues()
                                .calculateStartPadding(layoutDirection),
                        end =
                            WindowInsets.safeDrawing
                                .asPaddingValues()
                                .calculateEndPadding(layoutDirection)
                    ),
            config = config,
            extraAction = {
                if (hasOverlayPermission) {
                    startMagicWandService(context)
                }
            },
            shutdownRequested = shutdownRequested,
            onShutdownCompleted = {},
            autoShutdownOnDispose = true
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
