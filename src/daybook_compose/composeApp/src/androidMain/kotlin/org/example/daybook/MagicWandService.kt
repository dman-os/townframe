package org.example.daybook

import MagicWandLifecycleOwner
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.PixelFormat
import android.os.Build
import android.os.IBinder
import android.view.Gravity
import android.view.WindowManager
import android.widget.Toast
import androidx.compose.animation.core.FastOutSlowInEasing
import androidx.compose.animation.core.Spring
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.animateValueAsState
import androidx.compose.animation.core.snap
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsPressedAsState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.positionInWindow
import androidx.compose.ui.platform.ComposeView
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ViewModelStore
import androidx.lifecycle.ViewModelStoreOwner
import androidx.lifecycle.setViewTreeLifecycleOwner
import androidx.lifecycle.setViewTreeViewModelStoreOwner
import androidx.savedstate.setViewTreeSavedStateRegistryOwner
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.round
import kotlin.math.roundToInt

// Utility function for rounding floats to specific decimal places
fun Float.roundToString(decimals: Int): String {
    val factor = 10f.pow(decimals)
    return (round(this * factor) / factor).toString()
}

// Function to get animation config from system settings
@Composable
fun getAnimationConfig(): AnimationConfig {
    val context = androidx.compose.ui.platform.LocalContext.current

    // Check if animations are disabled globally
    val animationsEnabled =
        android.provider.Settings.Global.getFloat(
            context.contentResolver,
            android.provider.Settings.Global.ANIMATOR_DURATION_SCALE,
            1f
        ) > 0f

    // Get animation speed scale
    val animationScale =
        android.provider.Settings.Global.getFloat(
            context.contentResolver,
            android.provider.Settings.Global.ANIMATOR_DURATION_SCALE,
            1f
        )

    // Convert duration scale to speed factor (inverse relationship)
    val speedFactor = if (animationScale > 0f) 1f / animationScale else 1f

    return AnimationConfig(
        speedFactor = speedFactor.coerceIn(0.1f, 10f),
        isEnabled = animationsEnabled
    )
}

// Main service - only handles system concerns
class MagicWandService : Service() {
    private val notificationChannelId = "MagicWandServiceChannel"
    private val notificationId = 1

    private lateinit var windowManager: WindowManager
    private var overlayView: ComposeView? = null
    private var lifecycleOwner: MagicWandLifecycleOwner? = null
    private var layoutParams: WindowManager.LayoutParams? = null

    override fun onBind(intent: Intent): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        windowManager = getSystemService(WINDOW_SERVICE) as WindowManager
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.getStringExtra("ACTION_FROM_NOTIFICATION") == "STOP") {
            Toast.makeText(this, "Stopping service from notification", Toast.LENGTH_SHORT).show()
            stopSelf()
            return START_NOT_STICKY
        }

        Toast.makeText(this, "Magic Wand starting", Toast.LENGTH_SHORT).show()
        if (overlayView == null) {
            showOverlay()
        }

        val notification = createOngoingNotification("Magic Wand Active")
        ServiceCompat.startForeground(this, notificationId, notification, foregroundServiceType())
        return START_STICKY
    }

    private fun showOverlay() {
        overlayView =
            ComposeView(this).apply {
                setContent {
                    MagicWandOverlay(
                        onStopService = { stopSelf() },
                        onOverlayPosChanged = { mode, puckPosition, puckSize ->
                            updateWindowLayout(mode, puckPosition, puckSize)
                        }
                    )
                }

                // Lifecycle setup
                val fixedViewModelStore = ViewModelStore()
                val viewModelStoreOwner =
                    object : ViewModelStoreOwner {
                        override val viewModelStore: ViewModelStore
                            get() = fixedViewModelStore
                    }
                lifecycleOwner =
                    MagicWandLifecycleOwner().apply {
                        performRestore(null)
                        handleLifecycleEvent(Lifecycle.Event.ON_CREATE)
                        handleLifecycleEvent(Lifecycle.Event.ON_START)
                        handleLifecycleEvent(Lifecycle.Event.ON_RESUME)
                    }
                setViewTreeLifecycleOwner(lifecycleOwner)
                setViewTreeViewModelStoreOwner(viewModelStoreOwner)
                setViewTreeSavedStateRegistryOwner(lifecycleOwner)
            }

        layoutParams =
            WindowManager.LayoutParams(
                WindowManager.LayoutParams.MATCH_PARENT,
                WindowManager.LayoutParams.MATCH_PARENT,
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
                } else {
                    WindowManager.LayoutParams.TYPE_PHONE
                },
                WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or
                        WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or
                        WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN or
                        WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS or
                        WindowManager.LayoutParams.FLAG_FULLSCREEN or
                        WindowManager.LayoutParams.FLAG_LAYOUT_INSET_DECOR or
                        WindowManager.LayoutParams
                            .FLAG_DRAWS_SYSTEM_BAR_BACKGROUNDS,
                PixelFormat.TRANSLUCENT
            )
                .apply {
                    gravity = Gravity.TOP or Gravity.START
                    x = 0
                    y = 0
                }

        try {
            windowManager.addView(overlayView, layoutParams)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    // Throttle window updates to improve performance
    private var lastWindowUpdateTime = 0L
    private var lastMode = OverlayMode.HIDDEN
    private var lastPosition = PuckPosition(0.dp, 0.dp)
    private val windowUpdateThrottleMs = 16L // ~60fps max update rate

    fun updateWindowLayout(mode: OverlayMode, puckPosition: PuckPosition, puckSize: Dp) {
        layoutParams?.let { params ->
            overlayView?.let { view ->
                try {
                    val currentTime = System.currentTimeMillis()

                    // Skip update if too soon and values haven't changed significantly
                    if (currentTime - lastWindowUpdateTime < windowUpdateThrottleMs &&
                        mode == lastMode &&
                        kotlin.math.abs(puckPosition.x.value - lastPosition.x.value) < 2f &&
                        kotlin.math.abs(puckPosition.y.value - lastPosition.y.value) < 2f
                    ) {
                        return
                    }

                    lastWindowUpdateTime = currentTime
                    lastMode = mode
                    lastPosition = puckPosition

                    val density = resources.displayMetrics.density
                    val puckSizePx = (puckSize.value * density).toInt()
                    val puckXPx = (puckPosition.x.value * density).toInt()
                    val puckYPx = (puckPosition.y.value * density).toInt()

                    if (mode == OverlayMode.HIDDEN) {
                        // Resize window to hug the puck
                        params.width = puckSizePx
                        params.height = puckSizePx
                        params.x = puckXPx
                        params.y = puckYPx
                    } else {
                        // Full screen for overlay
                        params.width = WindowManager.LayoutParams.MATCH_PARENT
                        params.height = WindowManager.LayoutParams.MATCH_PARENT
                        params.x = 0
                        params.y = 0
                    }

                    // Disable window animation using reflection
                    try {
                        val lpClass = WindowManager.LayoutParams::class.java
                        val privateFlagsField = lpClass.getField("privateFlags")
                        val noMoveFlagField = lpClass.getField("PRIVATE_FLAG_NO_MOVE_ANIMATION")

                        val current = privateFlagsField.getInt(params)
                        val flag = noMoveFlagField.getInt(null)
                        privateFlagsField.setInt(params, current or flag)
                        windowAnimationDisabled = true
                    } catch (e: Exception) {
                        e.printStackTrace()
                        windowAnimationDisabled = false
                    }

                    windowManager.updateViewLayout(view, params)
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
    }

    private var windowAnimationDisabled = false

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val serviceChannel =
                NotificationChannel(
                    notificationChannelId,
                    "Magic Wand Service Channel",
                    NotificationManager.IMPORTANCE_LOW
                )
            getSystemService(NotificationManager::class.java)
                ?.createNotificationChannel(serviceChannel)
        }
    }

    private fun createOngoingNotification(contentText: String): Notification {
        val stopServiceIntent =
            Intent(this, StopServiceReceiver::class.java).apply { action = ACTION_STOP_SERVICE }
        val pendingIntentFlags =
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            } else {
                PendingIntent.FLAG_UPDATE_CURRENT
            }
        val stopServicePendingIntent =
            PendingIntent.getBroadcast(this, 1, stopServiceIntent, pendingIntentFlags)

        return NotificationCompat.Builder(this, notificationChannelId)
            .setContentText(contentText)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setOngoing(true)
            .addAction(
                R.drawable.ic_launcher_foreground,
                "Stop Service",
                stopServicePendingIntent
            )
            .build()
    }

    private fun foregroundServiceType(): Int {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
        } else {
            0
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        try {
            lifecycleOwner?.let { owner ->
                owner.handleLifecycleEvent(Lifecycle.Event.ON_PAUSE)
                owner.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
                owner.handleLifecycleEvent(Lifecycle.Event.ON_DESTROY)
            }
            lifecycleOwner = null

            overlayView?.let {
                if (it.isAttachedToWindow) {
                    windowManager.removeView(it)
                }
            }
            overlayView = null

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                stopForeground(Service.STOP_FOREGROUND_REMOVE)
            } else {
                @Suppress("DEPRECATION") stopForeground(true)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    companion object {
        const val ACTION_STOP_SERVICE = "org.example.daybook.ACTION_STOP_SERVICE"
    }

    class StopServiceReceiver : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action == ACTION_STOP_SERVICE) {
                context?.let {
                    val stopIntent = Intent(it, MagicWandService::class.java)
                    stopIntent.putExtra("ACTION_FROM_NOTIFICATION", "STOP")
                    it.startService(stopIntent)
                }
            }
        }
    }
}

// Data class for puck state
data class PuckState(
    val position: PuckPosition,
    val isVisible: Boolean = true,
    val isDragging: Boolean = false,
    val mode: OverlayMode = OverlayMode.HIDDEN,
    val dragState: DragState? = null,
    val hoveredButtonId: String? = null,
    val initialTouchOffset: androidx.compose.ui.geometry.Offset =
        androidx.compose.ui.geometry.Offset.Zero
)

// Button position data for collision detection
data class ButtonPosition(
    val id: String,
    val center: androidx.compose.ui.geometry.Offset,
    val radius: Float,
    val onAction: () -> Unit
)

// Optimized button registry for collision detection
class ButtonRegistry {
    private val _buttons = mutableMapOf<String, ButtonPosition>()
    val buttons: Map<String, ButtonPosition>
        get() = _buttons.toMap()

    fun registerButton(
        id: String,
        center: androidx.compose.ui.geometry.Offset,
        radius: Float,
        onAction: () -> Unit
    ) {
        _buttons[id] = ButtonPosition(id, center, radius, onAction)
    }

    fun unregisterButton(id: String) {
        _buttons.remove(id)
    }

    // Point-in-circle collision (original method, kept for reference)
    fun findButtonAt(position: androidx.compose.ui.geometry.Offset): String? {
        if (_buttons.isEmpty()) return null

        return _buttons.values
            .find { button ->
                val dx = position.x - button.center.x
                val dy = position.y - button.center.y
                // Use squared distance to avoid expensive sqrt operation
                val distanceSquared = dx * dx + dy * dy
                distanceSquared <= button.radius * button.radius
            }
            ?.id
    }

    // Optimized circle-to-circle overlap detection
    fun findButtonOverlappingWith(
        puckCenter: androidx.compose.ui.geometry.Offset,
        puckRadius: Float
    ): String? {
        if (_buttons.isEmpty()) return null

        return _buttons.values
            .find { button ->
                val dx = puckCenter.x - button.center.x
                val dy = puckCenter.y - button.center.y
                val combinedRadius = button.radius + puckRadius

                // Use squared distance to avoid expensive sqrt operation
                val distanceSquared = dx * dx + dy * dy
                distanceSquared <= combinedRadius * combinedRadius
            }
            ?.id
    }

    fun handleButtonAction(buttonId: String) {
        _buttons[buttonId]?.onAction?.invoke()
    }

    fun clear() {
        _buttons.clear()
    }
}

// Optimized weighted drag vector calculator with exponential decay
class WeightedDragCalculator {
    private val _dragVectors = mutableListOf<androidx.compose.ui.geometry.Offset>()
    private var _cachedWeightedVector: androidx.compose.ui.geometry.Offset? = null
    private var _cacheInvalid = true

    // Limit vector history to prevent memory bloat and improve performance
    private val maxVectors = 20

    val dragVectors: List<androidx.compose.ui.geometry.Offset>
        get() = _dragVectors

    fun addDragVector(vector: androidx.compose.ui.geometry.Offset) {
        _dragVectors.add(vector)
        _cacheInvalid = true

        // Limit vector history for performance
        if (_dragVectors.size > maxVectors) {
            _dragVectors.removeAt(0)
        }
    }

    fun getWeightedVector(): androidx.compose.ui.geometry.Offset {
        if (_cachedWeightedVector != null && !_cacheInvalid) {
            return _cachedWeightedVector!!
        }

        if (_dragVectors.isEmpty()) {
            _cachedWeightedVector = androidx.compose.ui.geometry.Offset.Zero
            _cacheInvalid = false
            return _cachedWeightedVector!!
        }

        val n = _dragVectors.size
        if (n == 1) {
            _cachedWeightedVector = _dragVectors[0]
            _cacheInvalid = false
            return _cachedWeightedVector!!
        }

        // Optimized calculation - use simpler weighting for better performance
        var weightedSumX = 0f
        var weightedSumY = 0f
        var totalWeight = 0f

        // Simple linear weighting - recent vectors get more weight
        _dragVectors.forEachIndexed { index, vector ->
            val weight = (index + 1).toFloat() // Linear weight increase
            weightedSumX += vector.x * weight
            weightedSumY += vector.y * weight
            totalWeight += weight
        }

        _cachedWeightedVector = if (totalWeight > 0f) {
            androidx.compose.ui.geometry.Offset(
                weightedSumX / totalWeight,
                weightedSumY / totalWeight
            )
        } else {
            androidx.compose.ui.geometry.Offset.Zero
        }

        _cacheInvalid = false
        return _cachedWeightedVector!!
    }

    fun getTotalDistance(): Float = getWeightedVector().getDistance()

    fun clear() {
        _dragVectors.clear()
        _cachedWeightedVector = null
        _cacheInvalid = true
    }
}

// Drag state tracking for sophisticated snapping
data class DragState(
    val startPosition: PuckPosition,
    val currentPosition: PuckPosition,
    val weightedCalculator: WeightedDragCalculator = WeightedDragCalculator(),
    val isActive: Boolean = false
) {
    val totalDragVector: androidx.compose.ui.geometry.Offset
        get() = weightedCalculator.getWeightedVector()

    val dragDistance: Float
        get() = totalDragVector.getDistance()

    val dragDirection: androidx.compose.ui.geometry.Offset
        get() =
            if (dragDistance > 0) totalDragVector / dragDistance
            else androidx.compose.ui.geometry.Offset.Zero

    fun crossedScreenHalves(screenWidth: Dp): Boolean {
        val startHalf = startPosition.x + 32.dp < screenWidth / 2 // Assuming puck size/2
        val endHalf = currentPosition.x + 32.dp < screenWidth / 2
        return startHalf != endHalf
    }
}

// Animation configuration
data class AnimationConfig(val speedFactor: Float = 1f, val isEnabled: Boolean = true)

// Animation strategy interface
interface SnapAnimationStrategy {
    fun calculateTargetPosition(
        dragState: DragState,
        screenWidth: Dp,
        screenHeight: Dp,
        puckSize: Dp
    ): PuckPosition

    fun getAnimationSpec(
        config: AnimationConfig
    ): androidx.compose.animation.core.AnimationSpec<PuckPosition>
}

// Direct snap for drags that cross to the opposite edge
class DirectSnapStrategy : SnapAnimationStrategy {
    override fun calculateTargetPosition(
        dragState: DragState,
        screenWidth: Dp,
        screenHeight: Dp,
        puckSize: Dp
    ): PuckPosition {
        val direction = dragState.dragDirection
        val current = dragState.currentPosition

        // Only snap to left or right edges
        val targetX =
            if (direction.x < 0) {
                0.dp // Left edge
            } else {
                screenWidth - puckSize // Right edge
            }

        // Calculate intersection with the target vertical edge
        val t =
            if (direction.x != 0f) {
                (targetX.value - current.x.value) / direction.x
            } else {
                1f
            }

        val targetY =
            if (t > 0 && direction.x != 0f) {
                // Calculate Y position along the trajectory
                val calculatedY = current.y.value + direction.y * t
                // Clamp to screen bounds
                calculatedY.coerceIn(0f, (screenHeight - puckSize).value).dp
            } else {
                // Fallback to current Y position
                current.y
            }

        return PuckPosition(targetX, targetY)
    }

    override fun getAnimationSpec(
        config: AnimationConfig
    ): androidx.compose.animation.core.AnimationSpec<PuckPosition> {
        if (!config.isEnabled) {
            return androidx.compose.animation.core.snap()
        }

        // Slower stiffness to match parabolic pace better
        val adjustedStiffness =
            androidx.compose.animation.core.Spring.StiffnessLow * config.speedFactor

        return androidx.compose.animation.core.spring(
            dampingRatio = androidx.compose.animation.core.Spring.DampingRatioMediumBouncy,
            stiffness =
                adjustedStiffness.coerceAtLeast(
                    androidx.compose.animation.core.Spring.StiffnessVeryLow
                )
        )
    }
}

// Parabolic snap with drag vector as tangent to the arc
class ParabolicSnapStrategy : SnapAnimationStrategy {
    override fun calculateTargetPosition(
        dragState: DragState,
        screenWidth: Dp,
        screenHeight: Dp,
        puckSize: Dp
    ): PuckPosition {
        val start = dragState.startPosition
        val current = dragState.currentPosition
        val dragVector = dragState.totalDragVector

        // Only snap to left or right edges
        val distanceToLeft = start.x.value
        val distanceToRight = (screenWidth - puckSize).value - start.x.value

        // Determine which edge the puck started closest to
        val targetX =
            if (distanceToLeft < distanceToRight) {
                0.dp // Snap to left edge
            } else {
                screenWidth - puckSize // Snap to right edge
            }

        // Calculate parabolic trajectory where drag vector is tangent
        // The idea: create an arc that has the drag vector as a tangent at the current position

        val targetY =
            if (dragVector.getDistance() > 10f) {
                // Use drag vector to influence the arc
                val normalizedDrag = dragVector / dragVector.getDistance()

                // Calculate arc parameters
                // For a parabolic arc, if we have a tangent vector at a point,
                // we can estimate the trajectory that would naturally lead to the edge

                val horizontalDistance = kotlin.math.abs(targetX.value - current.x.value)
                val verticalInfluence = normalizedDrag.y * horizontalDistance * 0.5f

                // Apply the vertical influence but clamp to screen bounds
                val calculatedY = current.y.value + verticalInfluence
                calculatedY.coerceIn(0f, (screenHeight - puckSize).value).dp
            } else {
                // Fallback to maintaining current Y position for very small drags
                current.y
            }

        return PuckPosition(targetX, targetY)
    }

    override fun getAnimationSpec(
        config: AnimationConfig
    ): androidx.compose.animation.core.AnimationSpec<PuckPosition> {
        if (!config.isEnabled) {
            return androidx.compose.animation.core.snap()
        }

        // Base stiffness adjusted by speed factor
        val adjustedStiffness =
            androidx.compose.animation.core.Spring.StiffnessLow * config.speedFactor

        return androidx.compose.animation.core.spring(
            dampingRatio = androidx.compose.animation.core.Spring.DampingRatioMediumBouncy,
            stiffness =
                adjustedStiffness.coerceAtLeast(
                    androidx.compose.animation.core.Spring.StiffnessVeryLow
                )
        )
    }
}

// Animation manager that orchestrates the snapping behavior
class PuckAnimationManager {
    fun selectStrategy(dragState: DragState, screenWidth: Dp, puckSize: Dp): SnapAnimationStrategy {
        val start = dragState.startPosition
        val current = dragState.currentPosition

        // Calculate which edge the puck started closest to
        val startDistanceToLeft = start.x.value
        val startDistanceToRight = (screenWidth - puckSize).value - start.x.value
        val startedCloserToLeft = startDistanceToLeft < startDistanceToRight

        // Calculate which edge the puck is currently closest to
        val currentDistanceToLeft = current.x.value
        val currentDistanceToRight = (screenWidth - puckSize).value - current.x.value
        val currentCloserToLeft = currentDistanceToLeft < currentDistanceToRight

        // If still closer to the same edge it started from, use parabolic
        // If moved to be closer to the opposite edge, use direct snap
        return if (startedCloserToLeft == currentCloserToLeft) {
            ParabolicSnapStrategy() // Still closest to same edge - parabolic arc
        } else {
            DirectSnapStrategy() // Moved closer to opposite edge - direct trajectory
        }
    }
}

// Data class for puck position in dp
data class PuckPosition(val x: Dp, val y: Dp) {
    fun clampToScreen(screenWidth: Dp, screenHeight: Dp, puckSize: Dp): PuckPosition {
        val clampedX = x.coerceIn(0.dp, screenWidth - puckSize)
        val clampedY = y.coerceIn(0.dp, screenHeight - puckSize)
        return PuckPosition(clampedX, clampedY)
    }

    fun snapToEdge(screenWidth: Dp, puckSize: Dp): PuckPosition {
        val snapToLeft = x + puckSize / 2 < screenWidth / 2
        val newX = if (snapToLeft) 0.dp else screenWidth - puckSize
        return PuckPosition(newX, y)
    }

    operator fun minus(other: PuckPosition): androidx.compose.ui.geometry.Offset {
        return androidx.compose.ui.geometry.Offset((x - other.x).value, (y - other.y).value)
    }

    companion object {
        val VectorConverter =
            androidx.compose.animation.core.TwoWayConverter<
                    PuckPosition, androidx.compose.animation.core.AnimationVector2D>(
                convertToVector = { position ->
                    androidx.compose.animation.core.AnimationVector2D(
                        position.x.value,
                        position.y.value
                    )
                },
                convertFromVector = { vector -> PuckPosition(vector.v1.dp, vector.v2.dp) }
            )
    }
}

// Overlay modes
enum class OverlayMode {
    HIDDEN,
    TAP_MODE,
    DRAG_MODE
}

// Main overlay composable - handles all UI state and logic
@Composable
fun MagicWandOverlay(
    onStopService: () -> Unit = {},
    onOverlayPosChanged: (OverlayMode, PuckPosition, Dp) -> Unit = { _, _, _ -> }
) {

    val density = LocalDensity.current
    val puckSize = 64.dp

    // Get screen dimensions in dp
    val screenWidth = androidx.compose.ui.platform.LocalConfiguration.current.screenWidthDp.dp
    val screenHeight = androidx.compose.ui.platform.LocalConfiguration.current.screenHeightDp.dp

    // Cache frequently used calculations
    val puckSizePx = remember { with(density) { puckSize.toPx() } }
    val puckRadiusPx = remember { puckSizePx / 2f }
    val screenWidthPx = remember { with(density) { screenWidth.toPx() } }
    val screenHeightPx = remember { with(density) { screenHeight.toPx() } }

    var puckState by remember {
        mutableStateOf(
            PuckState(
                position =
                    PuckPosition(9000000.dp, 9000000.dp)
                        .clampToScreen(screenWidth, screenHeight, puckSize)
            )
        ) // Bottom right position
    }

    // Button registry for collision detection
    val buttonRegistry = remember { ButtonRegistry() }

    // Animation manager for sophisticated snapping
    val animationManager = remember { PuckAnimationManager() }

    // Get animation config from system settings
    val animationConfig = getAnimationConfig()

    // Dynamic animation spec based on drag state
    val currentAnimationSpec =
        remember(puckState.dragState, animationConfig) {
            puckState.dragState?.let { dragState ->
                animationManager
                    .selectStrategy(dragState, screenWidth, puckSize)
                    .getAnimationSpec(animationConfig)
            }
                ?: if (animationConfig.isEnabled) {
                    tween(
                        durationMillis = (300 / animationConfig.speedFactor).toInt(),
                        easing = FastOutSlowInEasing
                    )
                } else {
                    snap()
                }
        }

    // Animate the values that go to updateWindowLayout for smooth transitions
    val animatedPuckPosition by
    animateValueAsState(
        targetValue = puckState.position,
        animationSpec = currentAnimationSpec,
        label = "puckPosition",
        typeConverter = PuckPosition.VectorConverter
    )

    // Notify service when animated values change for window resizing
    LaunchedEffect(puckState.mode, animatedPuckPosition) {
        onOverlayPosChanged(puckState.mode, animatedPuckPosition, puckSize)
    }

    Box(modifier = Modifier.fillMaxSize()) {
        // Content overlay (background)
        if (puckState.mode != OverlayMode.HIDDEN) {
            ContentOverlay(
                mode = puckState.mode,
                puckPosition = puckState.position,
                buttonRegistry = buttonRegistry,
                dragState = puckState.dragState,
                animationConfig = animationConfig,
                hoveredButtonId = puckState.hoveredButtonId,
                initialTouchOffset = puckState.initialTouchOffset,
                onDismiss = {
                    puckState =
                        puckState.copy(
                            mode = OverlayMode.HIDDEN,
                            isDragging = false,
                            hoveredButtonId = null,
                            initialTouchOffset =
                                androidx.compose.ui.geometry.Offset.Zero
                        )
                }
            )
        }

        // Floating puck - only consume touches when interacting
        MagicPuck(
            modifier =
                Modifier.offset {
                    when (puckState.mode) {
                        OverlayMode.HIDDEN -> {
                            // When window is resized to hug puck, position at (0,0)
                            IntOffset(0, 0)
                        }

                        OverlayMode.TAP_MODE -> {
                            // In tap mode, use the animated position to ensure consistency
                            // with HIDDEN mode
                            IntOffset(
                                with(density) {
                                    animatedPuckPosition.x.toPx().roundToInt()
                                },
                                with(density) {
                                    animatedPuckPosition.y.toPx().roundToInt()
                                }
                            )
                        }

                        OverlayMode.DRAG_MODE -> {
                            if (puckState.isDragging) {
                                // In drag mode, position puck so its center aligns with
                                // finger position
                                val fingerX =
                                    with(density) { puckState.position.x.toPx() } +
                                            puckState.initialTouchOffset.x
                                val fingerY =
                                    with(density) { puckState.position.y.toPx() } +
                                            puckState.initialTouchOffset.y
                                val puckCenterOffsetX =
                                    fingerX - puckRadiusPx
                                val puckCenterOffsetY =
                                    fingerY - puckRadiusPx
                                IntOffset(
                                    puckCenterOffsetX.roundToInt(),
                                    puckCenterOffsetY.roundToInt()
                                )
                            } else {
                                // Fallback to standard positioning
                                IntOffset(
                                    with(density) {
                                        puckState.position.x.toPx().roundToInt()
                                    },
                                    with(density) {
                                        puckState.position.y.toPx().roundToInt()
                                    }
                                )
                            }
                        }
                    }
                },
            size = puckSize,
            isDragging = puckState.isDragging,
            onTap = {
                // Toggle: if already in TAP_MODE, close; otherwise open TAP_MODE
                if (puckState.mode == OverlayMode.TAP_MODE) {
                    puckState =
                        puckState.copy(
                            mode = OverlayMode.HIDDEN,
                            hoveredButtonId = null,
                            initialTouchOffset =
                                androidx.compose.ui.geometry.Offset.Zero
                        )
                } else {
                    puckState =
                        puckState.copy(
                            mode = OverlayMode.TAP_MODE,
                            initialTouchOffset =
                                androidx.compose.ui.geometry.Offset.Zero
                        )
                }
            },
            onDragStart = { initialOffset ->
                // Resize window to full-screen BEFORE changing mode to prevent flash
                onOverlayPosChanged(OverlayMode.DRAG_MODE, puckState.position, puckSize)
                val calculator = WeightedDragCalculator()
                val dragState =
                    DragState(
                        startPosition = puckState.position,
                        currentPosition = puckState.position,
                        weightedCalculator = calculator,
                        isActive = true
                    )
                puckState =
                    puckState.copy(
                        isDragging = true,
                        mode = OverlayMode.DRAG_MODE,
                        dragState = dragState,
                        initialTouchOffset = initialOffset
                    )
            },
            onDrag = { dragAmount: androidx.compose.ui.geometry.Offset ->
                // Optimized drag handling - minimize object creation and calculations
                val currentDragState = puckState.dragState
                if (currentDragState != null) {
                    // Pre-calculate values to avoid repeated conversions
                    val dragAmountDpX = dragAmount.x / density.density
                    val dragAmountDpY = dragAmount.y / density.density

                    // Update position with minimal object creation
                    val newX = (puckState.position.x.value + dragAmountDpX).coerceIn(
                        0f,
                        (screenWidth - puckSize).value
                    )
                    val newY = (puckState.position.y.value + dragAmountDpY).coerceIn(
                        0f,
                        (screenHeight - puckSize).value
                    )
                    val newPosition = PuckPosition(newX.dp, newY.dp)

                    // Optimized collision detection - only check if in overlay mode
                    val hoveredButton =
                        if (puckState.mode != OverlayMode.HIDDEN && buttonRegistry.buttons.isNotEmpty()) {
                            // Pre-calculate visual center
                            val visualCenterX =
                                newX * density.density + puckState.initialTouchOffset.x
                            val visualCenterY =
                                newY * density.density + puckState.initialTouchOffset.y
                            val visualCenter =
                                androidx.compose.ui.geometry.Offset(visualCenterX, visualCenterY)
                            buttonRegistry.findButtonOverlappingWith(visualCenter, puckRadiusPx)
                        } else null

                    // Add drag vector and update state in one operation
                    currentDragState.weightedCalculator.addDragVector(dragAmount)
                    val updatedDragState = currentDragState.copy(currentPosition = newPosition)

                    puckState = puckState.copy(
                        position = newPosition,
                        dragState = updatedDragState,
                        hoveredButtonId = hoveredButton
                    )
                }
            },
            onDragEnd = {
                // Optimized drag end handling
                val droppedOnButton =
                    if (puckState.mode != OverlayMode.HIDDEN && buttonRegistry.buttons.isNotEmpty()) {
                        // Pre-calculate visual center for final collision check
                        val visualCenterX =
                            puckState.position.x.value * density.density + puckState.initialTouchOffset.x
                        val visualCenterY =
                            puckState.position.y.value * density.density + puckState.initialTouchOffset.y
                        val visualCenter =
                            androidx.compose.ui.geometry.Offset(visualCenterX, visualCenterY)
                        buttonRegistry.findButtonOverlappingWith(visualCenter, puckRadiusPx)
                    } else null

                if (droppedOnButton != null) {
                    // Puck was dropped on a button - handle the action
                    buttonRegistry.handleButtonAction(droppedOnButton)
                    // Snap to edge with simple animation
                    val snappedPosition = puckState.position.snapToEdge(screenWidth, puckSize)
                    puckState =
                        puckState.copy(
                            position = snappedPosition,
                            isDragging = false,
                            mode = OverlayMode.HIDDEN,
                            dragState = null,
                            hoveredButtonId = null,
                            initialTouchOffset =
                                androidx.compose.ui.geometry.Offset.Zero
                        )
                } else {
                    // Use sophisticated snapping based on drag behavior
                    val dragState = puckState.dragState
                    if (dragState != null) {
                        val strategy =
                            animationManager.selectStrategy(
                                dragState,
                                screenWidth,
                                puckSize
                            )
                        val targetPosition =
                            strategy.calculateTargetPosition(
                                dragState,
                                screenWidth,
                                screenHeight,
                                puckSize
                            )
                        puckState =
                            puckState.copy(
                                position = targetPosition,
                                isDragging = false,
                                mode = OverlayMode.HIDDEN,
                                dragState = dragState.copy(isActive = false),
                                hoveredButtonId = null,
                                initialTouchOffset =
                                    androidx.compose.ui.geometry.Offset.Zero
                            )
                    } else {
                        // Fallback to simple edge snap
                        val snappedPosition =
                            puckState.position.snapToEdge(screenWidth, puckSize)
                        puckState =
                            puckState.copy(
                                position = snappedPosition,
                                isDragging = false,
                                mode = OverlayMode.HIDDEN,
                                dragState = null,
                                hoveredButtonId = null,
                                initialTouchOffset =
                                    androidx.compose.ui.geometry.Offset.Zero
                            )
                    }
                }
            }
        )
    }
}

// Content overlay composable
@Composable
fun ContentOverlay(
    mode: OverlayMode,
    puckPosition: PuckPosition,
    buttonRegistry: ButtonRegistry,
    dragState: DragState? = null,
    animationConfig: AnimationConfig = AnimationConfig(),
    hoveredButtonId: String? = null,
    initialTouchOffset: androidx.compose.ui.geometry.Offset =
        androidx.compose.ui.geometry.Offset.Zero,
    onDismiss: () -> Unit
) {
    Surface(
        modifier = Modifier
            .fillMaxSize()
            .pointerInput(Unit) {}, // Consume touch events
        color = Color.Black.copy(alpha = 0.7f)
    ) {
        Column(modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)) {
            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                IconButton(onClick = onDismiss) {
                    Icon(
                        imageVector = Icons.Filled.Close,
                        contentDescription = "Dismiss Overlay",
                        tint = Color.White,
                    )
                }
            }

            Spacer(modifier = Modifier.height(50.dp))

            Column(
                modifier = Modifier.fillMaxWidth(),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center
            ) {
                Text(
                    text = "Magic Wand Overlay",
                    style = MaterialTheme.typography.headlineMedium,
                    color = Color.White
                )
                Spacer(modifier = Modifier.height(32.dp))

                // Action buttons
                Row(
                    horizontalArrangement = Arrangement.spacedBy(24.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    val context = LocalContext.current
                    ActionButton(
                        id = "camera",
                        label = "Camera",
                        icon = "📷",
                        color = Color.Green,
                        buttonRegistry = buttonRegistry,
                        isHovered = hoveredButtonId == "camera",
                        onAction = {
                            Toast.makeText(
                                context,
                                "Camera action triggered!",
                                Toast.LENGTH_SHORT
                            )
                                .show()
                        }
                    )
                    ActionButton(
                        id = "notes",
                        label = "Notes",
                        icon = "📝",
                        color = Color(0xFFFFA500),
                        buttonRegistry = buttonRegistry,
                        isHovered = hoveredButtonId == "notes",
                        onAction = {
                            Toast.makeText(
                                context,
                                "Notes action triggered!",
                                Toast.LENGTH_SHORT
                            )
                                .show()
                        }
                    )
                }

                Spacer(modifier = Modifier.height(32.dp))

                if (true) {
                    when (mode) {
                        OverlayMode.TAP_MODE -> {
                            Text(
                                "Tap buttons or drag puck to them",
                                color = Color.White.copy(alpha = 0.8f),
                                style = MaterialTheme.typography.bodyMedium
                            )
                        }

                        OverlayMode.DRAG_MODE -> {
                            Text(
                                "Drag puck to a button or drop to dismiss",
                                color = Color.White.copy(alpha = 0.8f),
                                style = MaterialTheme.typography.bodyMedium
                            )

                            // Optimized debug text - cache expensive calculations and reduce string operations
                            val puckXInt =
                                remember(puckPosition.x) { puckPosition.x.value.roundToInt() }
                            val puckYInt =
                                remember(puckPosition.y) { puckPosition.y.value.roundToInt() }

                            Text(
                                "Puck: (${puckXInt}dp, ${puckYInt}dp)",
                                color = Color.White.copy(alpha = 0.6f),
                                style = MaterialTheme.typography.bodySmall
                            )

                            val density = LocalDensity.current
                            val fingerX = remember(puckPosition.x, initialTouchOffset.x) {
                                with(density) { puckPosition.x.toPx() } + initialTouchOffset.x
                            }
                            val fingerY = remember(puckPosition.y, initialTouchOffset.y) {
                                with(density) { puckPosition.y.toPx() } + initialTouchOffset.y
                            }
                            val fingerDpX =
                                remember(fingerX) { with(density) { fingerX.toDp().value.roundToInt() } }
                            val fingerDpY =
                                remember(fingerY) { with(density) { fingerY.toDp().value.roundToInt() } }

                            Text(
                                "Finger: (${fingerDpX}dp, ${fingerDpY}dp)",
                                color = Color.Cyan.copy(alpha = 0.6f),
                                style = MaterialTheme.typography.bodySmall
                            )
                            Text(
                                "Mode: $mode | Offset Applied: ${initialTouchOffset != androidx.compose.ui.geometry.Offset.Zero}",
                                color = Color.Magenta.copy(alpha = 0.7f),
                                style = MaterialTheme.typography.bodySmall
                            )
                            dragState?.let { state ->
                                val screenWidthDp =
                                    androidx.compose.ui.platform.LocalConfiguration.current
                                        .screenWidthDp
                                        .dp
                                val startCloserToLeft = state.startPosition.x < screenWidthDp / 2
                                val currentCloserToLeft =
                                    state.currentPosition.x < screenWidthDp / 2
                                val strategy =
                                    if (startCloserToLeft == currentCloserToLeft) "Parabolic"
                                    else "Direct"

                                // Cache expensive calculations
                                val dragDistanceInt =
                                    remember(state.dragDistance) { state.dragDistance.roundToInt() }
                                val vectorSize =
                                    remember(state.weightedCalculator.dragVectors) { state.weightedCalculator.dragVectors.size }

                                Text(
                                    "Distance: ${dragDistanceInt}px | Events: $vectorSize",
                                    color = Color.White.copy(alpha = 0.5f),
                                    style = MaterialTheme.typography.bodySmall
                                )
                                Text(
                                    "Strategy: $strategy | Edge: ${if (startCloserToLeft) "L" else "R"} → ${if (currentCloserToLeft) "L" else "R"}",
                                    color = Color.White.copy(alpha = 0.5f),
                                    style = MaterialTheme.typography.bodySmall
                                )
                            }

                            val buttonCount =
                                remember(buttonRegistry.buttons) { buttonRegistry.buttons.size }
                            val speedFactorStr = remember(animationConfig.speedFactor) {
                                "${(animationConfig.speedFactor * 10).roundToInt() / 10f}x"
                            }

                            Text(
                                "Buttons: $buttonCount | Anim: ${if (animationConfig.isEnabled) "ON" else "OFF"} | Speed: $speedFactorStr",
                                color = Color.White.copy(alpha = 0.5f),
                                style = MaterialTheme.typography.bodySmall
                            )
                            hoveredButtonId?.let { buttonId ->
                                Text(
                                    "Hovering: $buttonId",
                                    color = Color.Yellow.copy(alpha = 0.8f),
                                    style = MaterialTheme.typography.bodySmall
                                )
                            }
                        }

                        OverlayMode.HIDDEN -> {
                            /* Should not happen */
                        }
                    }
                }
            }
        }
    }
}

// Action button component
@Composable
fun ActionButton(
    id: String,
    label: String,
    icon: String,
    color: androidx.compose.ui.graphics.Color,
    buttonRegistry: ButtonRegistry,
    isHovered: Boolean = false,
    onAction: () -> Unit,
    modifier: Modifier = Modifier
) {
    val density = LocalDensity.current
    val buttonSize = 80.dp
    val buttonRadiusPx = with(density) { (buttonSize / 2).toPx() }

    // Hover animations
    val animatedScale by
    animateFloatAsState(
        targetValue = if (isHovered) 1.1f else 1f,
        animationSpec =
            spring(
                dampingRatio = Spring.DampingRatioMediumBouncy,
                stiffness = Spring.StiffnessHigh
            ),
        label = "buttonScale"
    )

    val animatedGlow by
    animateFloatAsState(
        targetValue = if (isHovered) 1f else 0f,
        animationSpec =
            spring(
                dampingRatio = Spring.DampingRatioLowBouncy,
                stiffness = Spring.StiffnessMedium
            ),
        label = "buttonGlow"
    )

    // Clean up when button is removed from composition
    DisposableEffect(id) { onDispose { buttonRegistry.unregisterButton(id) } }

    Button(
        onClick = onAction,
        modifier =
            modifier
                .size(buttonSize)
                .graphicsLayer {
                    scaleX = animatedScale
                    scaleY = animatedScale
                }
                .shadow(
                    elevation = (8 + animatedGlow * 12).dp,
                    shape = CircleShape,
                    ambientColor = color.copy(alpha = animatedGlow * 0.6f),
                    spotColor = color.copy(alpha = animatedGlow * 0.8f)
                )
                .border(
                    width = (2 * animatedGlow).dp,
                    color = Color.White.copy(alpha = animatedGlow * 0.7f),
                    shape = CircleShape
                )
                .onGloballyPositioned { coordinates ->
                    // Calculate center position in window coordinates
                    val centerX =
                        coordinates.positionInWindow().x +
                                coordinates.size.width / 2f
                    val centerY =
                        coordinates.positionInWindow().y +
                                coordinates.size.height / 2f
                    val center = androidx.compose.ui.geometry.Offset(centerX, centerY)

                    // Register button with collision detection system
                    buttonRegistry.registerButton(
                        id = id,
                        center = center,
                        radius = buttonRadiusPx,
                        onAction = onAction
                    )
                },
        shape = CircleShape,
        colors =
            ButtonDefaults.buttonColors(containerColor = color, contentColor = Color.White),
        elevation =
            ButtonDefaults.buttonElevation(
                defaultElevation = 0.dp, // Let shadow handle elevation
                pressedElevation = 0.dp
            )
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center
        ) {
            Text(text = icon, style = MaterialTheme.typography.headlineSmall)
            Text(text = label, style = MaterialTheme.typography.labelSmall, maxLines = 1)
        }
    }
}

// Optimized floating puck composable
@Composable
fun MagicPuck(
    modifier: Modifier = Modifier,
    size: Dp = 64.dp,
    isDragging: Boolean = false,
    onTap: () -> Unit = {},
    onDragStart: (initialOffset: androidx.compose.ui.geometry.Offset) -> Unit = {},
    onDrag: (dragAmount: androidx.compose.ui.geometry.Offset) -> Unit = {},
    onDragEnd: () -> Unit = {}
) {
    val interactionSource = remember { MutableInteractionSource() }
    val isPressed by interactionSource.collectIsPressedAsState()
    var hasDragged by remember { mutableStateOf(false) }

    // Optimize animations - combine state checks and use faster specs
    val isActive = isPressed || isDragging

    val animatedSizeFactor by
    animateFloatAsState(
        targetValue = if (isActive) 1f else 0.5f,
        animationSpec = tween(durationMillis = 100), // Faster animation
        label = "puckSizeFactor"
    )

    val animatedAlpha by
    animateFloatAsState(
        targetValue = if (isActive) 0.9f else 0.7f,
        animationSpec = tween(durationMillis = 100), // Faster animation
        label = "puckAlpha"
    )

    // Cache computed values
    val containerColor = remember(animatedAlpha) { Color.Blue.copy(alpha = animatedAlpha) }
    val puckIcon = remember(isActive) { if (isActive) "✨" else "🪩" }

    Box(
        modifier =
            modifier
                .size(size)
                .pointerInput(
                    onDragStart,
                    onDrag,
                    onDragEnd
                ) { // Add dependencies to prevent recreation
                    detectDragGestures(
                        onDragStart = { offset ->
                            hasDragged = false
                            onDragStart(offset)
                        },
                        onDrag = { _, dragAmount ->
                            hasDragged = true
                            onDrag(dragAmount)
                        },
                        onDragEnd = {
                            if (hasDragged) {
                                onDragEnd()
                            }
                            hasDragged = false
                        }
                    )
                }
                .background(Color.Transparent)
    ) {
        Button(
            onClick = {
                if (!hasDragged && !isDragging) {
                    onTap()
                }
            },
            modifier =
                Modifier
                    .align(Alignment.Center)
                    .size(size * animatedSizeFactor)
                    .clip(CircleShape),
            shape = CircleShape,
            colors =
                ButtonDefaults.buttonColors(
                    containerColor = containerColor,
                    contentColor = Color.White
                ),
            interactionSource = interactionSource,
            elevation = ButtonDefaults.buttonElevation(0.dp, 0.dp, 0.dp, 0.dp, 0.dp)
        ) {
            Text(puckIcon, color = Color.White)
        }
    }
}
