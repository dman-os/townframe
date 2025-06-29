package org.example.daybook

import MagicWandLifecycleOwner
import android.app.Service
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.PixelFormat
import android.os.Build
import android.os.IBinder
import android.view.Gravity
import android.view.View
import android.view.WindowManager
import android.widget.Toast
import android.util.Log;
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.Recomposer
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.AndroidUiDispatcher
import androidx.compose.ui.platform.ComposeView
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ViewModelStore
import androidx.lifecycle.ViewModelStoreOwner
import kotlin.math.roundToInt
import androidx.lifecycle.setViewTreeLifecycleOwner
import androidx.lifecycle.setViewTreeViewModelStoreOwner
import androidx.savedstate.setViewTreeSavedStateRegistryOwner
import kotlinx.coroutines.CoroutineScope

class MagicWandService : Service() {

    private val notificationChannelId = "MagicWandServiceChannel"
    private val notificationId = 1 // Must be > 0

    private lateinit var windowManager: WindowManager
    private var overlayView: View? = null // Will host the ComposeView
    private lateinit var viewParams: WindowManager.LayoutParams

    // For draggable button
    private var initialX: Int = 0
    private var initialY: Int = 0
    private var initialTouchX: Float = 0f
    private var initialTouchY: Float = 0f


    override fun onBind(intent: Intent): IBinder? {
        // TODO("Return the communication channel to the service.")
        return null
    }
    override fun onCreate() {
        super.onCreate()
        windowManager = getSystemService(WINDOW_SERVICE) as WindowManager
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Toast.makeText(this, "service starting", Toast.LENGTH_SHORT).show()
        if (overlayView == null) {
            initializeOverlay()
            showOverlayButton()
        }
        val notification = this.createOngoingNotification("magic wand")
        ServiceCompat.startForeground(
            this,
            notificationId,
            notification,
            foregroundServiceType()
        )

        return START_STICKY;
    }

    private fun initializeOverlay() {
        viewParams = WindowManager.LayoutParams(
            WindowManager.LayoutParams.WRAP_CONTENT,
            WindowManager.LayoutParams.WRAP_CONTENT,
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
            } else {
                WindowManager.LayoutParams.TYPE_PHONE // Or TYPE_SYSTEM_ALERT for older versions
            },
            WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or // Prevents keyboard focus
                    WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or // Allows touches outside
                    WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN or
                    WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS, // Allows moving outside screen edges slightly
            PixelFormat.TRANSLUCENT
        ).apply {
            gravity = Gravity.TOP or Gravity.START
            x = 50 // Initial X position
            y = 100 // Initial Y position
        }

        overlayView = ComposeView(this).apply {
            setContent {
                DraggableOverlayButton(
                    onClick = {
                        // Action when the button is clicked
                        println("Overlay button clicked!")
                        // Example: Open your app
//                        val appIntent = Intent(this@MagicWandService, MainActivity::class.java).apply {
//                            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
//                        }
//                        startActivity(appIntent)
                    },
                    onDrag = { dx, dy ->
                        // Update window position during drag
                        if (overlayView != null && overlayView!!.isAttachedToWindow) {
                            viewParams.x += dx.roundToInt()
                            viewParams.y += dy.roundToInt()
                            windowManager.updateViewLayout(overlayView, viewParams)
                        }
                    }
                )
            }
        }.apply {

            // Trick The ComposeView into thinking we are tracking lifecycle
            val viewModelStoreOwner = object : ViewModelStoreOwner {
                override val viewModelStore: ViewModelStore
                    get() = ViewModelStore()
            }
            val lifecycleOwner = MagicWandLifecycleOwner().apply {
                performRestore(null)
                // This is required or otherwise the UI will not recompose
                handleLifecycleEvent(Lifecycle.Event.ON_CREATE)
                handleLifecycleEvent(Lifecycle.Event.ON_START)
                handleLifecycleEvent(Lifecycle.Event.ON_RESUME)
            }
            setViewTreeLifecycleOwner(lifecycleOwner)
            setViewTreeViewModelStoreOwner(viewModelStoreOwner)
            setViewTreeSavedStateRegistryOwner(lifecycleOwner)
        }

    }


    private fun showOverlayButton() {
        try {
            if (overlayView != null && !overlayView!!.isAttachedToWindow) {
                windowManager.addView(overlayView, viewParams)
            }
        } catch (e: Exception) {
            e.printStackTrace() // Log error
        }
    }

    @Composable
    fun DraggableOverlayButton(onClick: () -> Unit, onDrag: (Float, Float) -> Unit) {
        var offsetX by remember { mutableStateOf(0f) }
        var offsetY by remember { mutableStateOf(0f) }

        Box(
            modifier = Modifier
                .offset { IntOffset(offsetX.roundToInt(), offsetY.roundToInt()) } // Visual offset while dragging
                .pointerInput(Unit) {
                    detectDragGestures(
                        onDragStart = {
                            // Can store initial position if needed for snapping back etc.
                        },
                        onDrag = { change, dragAmount ->
                            change.consume()
                            // Pass the raw dragAmount for window position update
                            onDrag(dragAmount.x, dragAmount.y)
                        },
                        onDragEnd = {
                            // Optional: Snap to edges or finalize position
                        }
                    )
                }
        ) {
            Button(
                onClick = onClick,
                modifier = Modifier
                    .padding(8.dp)
                    .background(Color.Blue.copy(alpha = 0.7f)) // Semi-transparent
            ) {
                Text("App", color = Color.White)
            }
        }
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val serviceChannel = NotificationChannel(
                notificationChannelId,
                "Magic Wand Service Notification Channel",
                NotificationManager.IMPORTANCE_DEFAULT // Or IMPORTANCE_LOW etc.
            )
            val manager = getSystemService(NotificationManager::class.java)
            manager?.createNotificationChannel(serviceChannel)
        }
    }

    private fun createOngoingNotification(contentText: String): Notification {
        // (This is a simplified version. See previous examples for full notification setup
        // including click intent and channel creation for Android 8.0+)

        val notificationIntent = Intent(this, MainActivity::class.java)
        val pendingIntentFlags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        } else {
            PendingIntent.FLAG_UPDATE_CURRENT
        }
        val pendingIntent = PendingIntent.getActivity(this, 0, notificationIntent, pendingIntentFlags)

        return NotificationCompat.Builder(this, notificationChannelId)
//            .setContentTitle("My Foreground Service")
            .setContentText(contentText)
            .setSmallIcon(R.drawable.ic_launcher_foreground) // Replace with your icon
            //.setContentIntent(pendingIntent)
            .setOngoing(true) // Makes the notification persistent
            .build()
    }
    private fun foregroundServiceType(): Int {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) { // API 34+
            return ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA or ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE or ServiceInfo.FOREGROUND_SERVICE_TYPE_REMOTE_MESSAGING or ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE // Or provide the relevant type(s)
            // If your service has NO foregroundServiceType declared in the manifest AT ALL:
            // return ServiceInfo.FOREGROUND_SERVICE_TYPE_NONE (Though this means it's not a special FGS)
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) { // API 29-33
            return ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA or ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE; // Or the relevant type.
        }
        return 0 // For older versions or if no specific type is applicable for this start.
    }


    override fun onDestroy() {
        super.onDestroy()
        if (overlayView != null && overlayView!!.isAttachedToWindow) {
            windowManager.removeView(overlayView)
        }
        overlayView = null
        stopForeground(STOP_FOREGROUND_REMOVE)
    }

}