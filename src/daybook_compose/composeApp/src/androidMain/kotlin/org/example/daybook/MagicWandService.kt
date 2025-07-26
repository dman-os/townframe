/*
 * Magic Wand Service - System-wide floating overlay with draggable puck interface
 * 
 * This service creates a floating overlay that works across all apps, featuring:
 * - A draggable "puck" that can be tapped or dragged to trigger actions
 * - Smart window resizing: shrinks to hug the puck when hidden, expands for full overlay
 * - Sophisticated drag physics with parabolic and direct snapping strategies
 * - Button collision detection system for drag-to-action interactions
 * - Performance optimizations including throttled window updates and cached calculations
 * - Animation system that respects Android's global animation settings
 * 
 * Key architectural decisions:
 * - Service handles only system-level concerns (notifications, window management)
 * - Compose UI handles all interaction logic and animations
 * - Window resizing minimizes resource usage when overlay is hidden
 * - Reflection used to disable window animations for smoother experience
 */
package org.example.daybook

import MagicWandLifecycleOwner
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent.FLAG_IMMUTABLE
import android.app.PendingIntent.FLAG_UPDATE_CURRENT
import android.app.PendingIntent.getBroadcast
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.PixelFormat
import android.os.Build
import android.os.Build.VERSION.SDK_INT
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
import androidx.compose.foundation.clickable
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
import androidx.compose.material.icons.filled.Edit
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
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.positionInWindow
import androidx.compose.ui.platform.ComposeView
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalWindowInfo
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationCompat.PRIORITY_LOW
import androidx.core.app.ServiceCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ViewModelStore
import androidx.lifecycle.ViewModelStoreOwner
import androidx.lifecycle.setViewTreeLifecycleOwner
import androidx.lifecycle.setViewTreeViewModelStoreOwner
import androidx.savedstate.setViewTreeSavedStateRegistryOwner
import org.example.daybook.R.drawable.ic_launcher_foreground
import kotlin.math.roundToInt

/**
 * Android foreground service that manages the system overlay window.
 *
 * Responsibilities:
 * - Creates and manages overlay window with proper permissions
 * - Handles foreground service lifecycle and notifications
 * - Optimizes window size based on overlay state (shrinks to hug puck when hidden)
 * - Throttles window updates to maintain 60fps performance
 */
class MagicWandService : Service() {
    companion object {
        const val NOTIFICATION_CHANNEL_ID = "MagicWandServiceChannel"
        const val ACTION_STOP_SERVICE = "org.example.daybook.ACTION_STOP_SERVICE"
        const val NOTIFICATION_ID = 1
    }

    private lateinit var windowManager: WindowManager
    private var overlayView: ComposeView? = null
    private var lifecycleOwner: MagicWandLifecycleOwner? = null
    private var layoutParams: WindowManager.LayoutParams? = null

    override fun onBind(intent: Intent): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        windowManager = getSystemService(WINDOW_SERVICE) as WindowManager
        val serviceChannel =
            NotificationChannel(
                NOTIFICATION_CHANNEL_ID,
                "Magic Wand Service Channel",
                NotificationManager.IMPORTANCE_LOW
            )
        getSystemService(NotificationManager::class.java)
            ?.createNotificationChannel(serviceChannel)
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

        // a persistent notification is needed to have display on top overlays
        val notification = run {
            val stopServicePendingIntent = getBroadcast(
                this,
                1,
                Intent(this, StopServiceReceiver::class.java).apply {
                    this.action =
                        ACTION_STOP_SERVICE
                },
                FLAG_UPDATE_CURRENT or FLAG_IMMUTABLE,
            )
            NotificationCompat.Builder(this, NOTIFICATION_CHANNEL_ID)
                .setContentText("magic wand")
                // TODO: icon for notification
                // NOTE: icons are mandatory
                .setSmallIcon(ic_launcher_foreground)
                .setPriority(PRIORITY_LOW)
                .setOngoing(true)
                .addAction(
                    // TODO: use stop icon
                    ic_launcher_foreground,
                    "Stop Service",
                    stopServicePendingIntent
                )
                .build()
        }
        ServiceCompat.startForeground(
            this,
            NOTIFICATION_ID,
            notification,
            if (SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE
            } else {
                0
            }
        )
        return START_STICKY
    }

    private fun showOverlay() {
        overlayView =
            ComposeView(this).apply {
                setContent {
                    MagicWandOverlay(
                        onStopService = { stopSelf() },
                        onOverlayPosChanged = { mode, puckPosition, puckSize ->
                            // coordinate window resizes and moves
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
                WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY,
                WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or
                        WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or
                        WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN or
                        WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS or
                        // NOTE: the following are used to draw over the status bar
                        // but not exactly? The status bar icons seem to still appear
                        // over the overlay (which is not un-desirable)
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

        windowManager.addView(overlayView, layoutParams)
    }

    fun updateWindowLayout(mode: OverlayMode, puckPosition: PuckPosition, puckSize: Dp) {
        layoutParams?.let { params ->
            overlayView?.let { view ->
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
                } catch (e: Exception) {
                    e.printStackTrace()
                }

                windowManager.updateViewLayout(view, params)
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
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

        stopForeground(STOP_FOREGROUND_REMOVE)
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


/**
 * Animation configuration
 */
data class SysAnimationConfig(val speedFactor: Float = 1f, val isEnabled: Boolean = true)

/**
 * Reads Android's global animation settings to respect user preferences.
 * Returns configuration for animation speed and enable/disable state.
 */
fun getAnimationConfig(context: Context): SysAnimationConfig {
    // Get animation speed scale
    val animationScale =
        android.provider.Settings.Global.getFloat(
            context.contentResolver,
            android.provider.Settings.Global.ANIMATOR_DURATION_SCALE,
            1f
        )
    // Check if animations are disabled globally
    val animationsEnabled = animationScale > 0f

    // Convert duration scale to speed factor (inverse relationship)
    val speedFactor = if (animationScale > 0f) 1f / animationScale else 1f

    return SysAnimationConfig(
        speedFactor = speedFactor.coerceIn(0.1f, 10f),
        isEnabled = animationsEnabled
    )
}

/**
 * Immutable state container for the floating puck.
 *
 * @param initialTouchOffset Critical for natural drag feel - stores where user's finger
 *                          is relative to puck center when drag starts
 */
data class PuckState(
    val position: PuckPosition,
    val isDragging: Boolean = false,
    val mode: OverlayMode = OverlayMode.HIDDEN,
    val dragState: DragState? = null,
    val hoveredButtonId: String? = null,
    val initialTouchOffset: androidx.compose.ui.geometry.Offset =
        androidx.compose.ui.geometry.Offset.Zero
)

/**
 * Base class for widget descriptors with common position and size properties.
 */
abstract class WidgetDesc(
    val id: String,
    val position: PuckPosition,
    val size: Dp = 80.dp
) {
    abstract fun withPositionAndSize(position: PuckPosition, size: Dp): WidgetDesc
}

/**
 * Descriptor for action button widgets.
 */
data class ActionButtonDesc(
    val widgetId: String,
    val label: String,
    val icon: String,
    val color: androidx.compose.ui.graphics.Color,
    val onAction: () -> Unit,
    val widgetPosition: PuckPosition,
    val widgetSize: Dp = 80.dp
) : WidgetDesc(widgetId, widgetPosition, widgetSize) {

    override fun withPositionAndSize(position: PuckPosition, size: Dp): WidgetDesc {
        return ActionButtonDesc(
            widgetId = widgetId,
            label = label,
            icon = icon,
            color = color,
            onAction = onAction,
            widgetPosition = position,
            widgetSize = size
        )
    }
}

/**
 * Descriptor for the puck widget (for edit mode selection).
 */
data class PuckDesc(
    val widgetPosition: PuckPosition,
    val widgetSize: Dp = 64.dp
) : WidgetDesc("puck", widgetPosition, widgetSize) {

    override fun withPositionAndSize(position: PuckPosition, size: Dp): WidgetDesc {
        return PuckDesc(
            widgetPosition = position,
            widgetSize = size
        )
    }
}

/**
 * Widget collision data for real-time collision detection.
 */
data class WidgetCollision(
    val id: String,
    val center: androidx.compose.ui.geometry.Offset,
    val radius: Float
)

/**
 * Registry for managing overlay widgets with position persistence and collision detection.
 *
 * Replaces ButtonRegistry with more sophisticated widget management:
 * - Persists widget positions and properties
 * - Provides collision detection for edit mode
 * - Supports different widget types through WidgetDesc hierarchy
 */
class WidgetRegistry {
    private val _widgets = mutableMapOf<String, WidgetDesc>()
    private val _collisionData = mutableMapOf<String, WidgetCollision>()

    val widgets: Map<String, WidgetDesc>
        get() = _widgets.toMap()

    /**
     * Register or update a widget descriptor.
     */
    fun registerWidget(widget: WidgetDesc) {
        _widgets[widget.id] = widget
    }

    /**
     * Update widget position (for drag operations).
     */
    fun updateWidgetPosition(id: String, position: PuckPosition) {
        _widgets[id]?.let { widget ->
            _widgets[id] = widget.withPositionAndSize(position, widget.size)
        }
    }

    /**
     * Update widget size (for adjustment operations).
     */
    fun updateWidgetSize(id: String, size: Dp) {
        _widgets[id]?.let { widget ->
            _widgets[id] = widget.withPositionAndSize(widget.position, size)
        }
    }

    /**
     * Register collision data for real-time collision detection.
     */
    fun registerCollisionData(
        id: String,
        center: androidx.compose.ui.geometry.Offset,
        radius: Float
    ) {
        _collisionData[id] = WidgetCollision(id, center, radius)
    }

    /**
     * Check if a widget at given position would collide with existing widgets.
     * Excludes the widget being moved from collision check.
     */
    fun checkCollision(
        movingWidgetId: String,
        center: androidx.compose.ui.geometry.Offset,
        radius: Float
    ): String? {
        return _collisionData.values
            .filter { it.id != movingWidgetId }
            .find { collision ->
                val dx = center.x - collision.center.x
                val dy = center.y - collision.center.y
                val combinedRadius = collision.radius + radius
                val distanceSquared = dx * dx + dy * dy
                distanceSquared <= combinedRadius * combinedRadius
            }?.id
    }

    /**
     * Find widget overlapping with puck (for drag-to-action).
     */
    fun findWidgetOverlappingWith(
        puckCenter: androidx.compose.ui.geometry.Offset,
        puckRadius: Float
    ): String? {
        return _collisionData.values
            .find { collision ->
                val dx = puckCenter.x - collision.center.x
                val dy = puckCenter.y - collision.center.y
                val combinedRadius = collision.radius + puckRadius
                val distanceSquared = dx * dx + dy * dy
                distanceSquared <= combinedRadius * combinedRadius
            }?.id
    }

    /**
     * Handle widget action (for ActionButton widgets).
     */
    fun handleWidgetAction(widgetId: String) {
        (_widgets[widgetId] as? ActionButtonDesc)?.onAction?.invoke()
    }

    /**
     * Get default widgets for initial setup.
     */
    fun getDefaultWidgets(screenWidth: Dp): List<WidgetDesc> {
        return listOf(
            ActionButtonDesc(
                widgetId = "camera",
                label = "Camera",
                icon = "📷",
                color = Color.Green,
                onAction = {},
                widgetPosition = PuckPosition(screenWidth / 2 - 120.dp, 200.dp)
            ),
            ActionButtonDesc(
                widgetId = "notes",
                label = "Notes",
                icon = "📝",
                color = Color(0xFFFFA500),
                onAction = {},
                widgetPosition = PuckPosition(screenWidth / 2 + 40.dp, 200.dp)
            )
        )
    }

    fun clear() {
        _widgets.clear()
        _collisionData.clear()
    }
}

/**
 * Calculates weighted drag vectors with exponential decay for sophisticated snapping.
 *
 * Tracks recent drag movements to determine natural trajectory when released.
 * Limited vector history prevents memory bloat during long drags.
 */
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

/**
 * Tracks drag state for sophisticated physics-based snapping.
 *
 * @param crossedScreenHalves Used to determine if puck moved to opposite screen side,
 *                           triggering different snapping strategies
 */
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

/**
 * Animation strategy interface
 */
interface SnapAnimationStrategy {
    fun calculateTargetPosition(
        dragState: DragState,
        screenWidth: Dp,
        screenHeight: Dp,
        puckSize: Dp
    ): PuckPosition

    fun getAnimationSpec(
        config: SysAnimationConfig
    ): androidx.compose.animation.core.AnimationSpec<PuckPosition>
}

/**
 * Animation strategy for direct trajectory snapping.
 * Used when drag crosses to opposite side of screen - aims for opposite edge.
 */
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
        config: SysAnimationConfig
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

/**
 * Animation strategy for parabolic arc snapping.
 * Used when staying on same side - creates natural arc to nearest edge.
 * Uses drag vector as tangent to determine arc shape.
 */
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
        config: SysAnimationConfig
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

/**
 * Selects appropriate snapping strategy based on drag behavior.
 *
 * Strategy selection:
 * - Parabolic: if puck stays closer to the same edge it started from
 * - Direct: if puck moves closer to the opposite edge (crossing screen)
 */
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

/**
 * Position in dp coordinates with utility methods for screen bounds and snapping.
 *
 * Includes custom animation converter for smooth position transitions.
 */
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

/**
 * Overlay modes
 */
enum class OverlayMode {
    HIDDEN,
    TAP_MODE,
    DRAG_MODE,
    EDIT_MODE
}

/**
 * Edit mode state for the overlay editor.
 */
data class EditModeState(
    val isEditMode: Boolean = false,
    val selectedWidgetId: String? = null,
    val dragStartPosition: PuckPosition? = null
)

/**
 * Main overlay UI coordinator handling puck state and interactions.
 *
 * Key responsibilities:
 * - Manages puck state transitions (hidden/tap/drag modes)
 * - Coordinates between visual puck position and window layout updates
 * - Handles sophisticated drag physics and button collision detection
 * - Optimizes performance with cached calculations and minimal object creation
 */
@Composable
fun MagicWandOverlay(
    onStopService: () -> Unit = {},
    onOverlayPosChanged: (OverlayMode, PuckPosition, Dp) -> Unit = { _, _, _ -> }
) {

    val density = LocalDensity.current
    val puckSize = 64.dp

    // LocalContext.current.resources..
    // Get screen dimensions in dp
    val (screenWidth, screenHeight) = with(density) {
        Pair(
            LocalWindowInfo.current.containerSize.width.toDp(),
            LocalWindowInfo.current.containerSize.height.toDp()
        )
    }


    // Cache frequently used calculations
    val puckRadiusPx = remember { with(density) { puckSize.toPx() / 2f } }

    var puckState by remember {
        mutableStateOf(
            PuckState(
                position =
                    PuckPosition(9000000.dp, 9000000.dp)
                        .clampToScreen(screenWidth, screenHeight, puckSize)
            )
        ) // Bottom right position
    }

    // Widget registry for collision detection and position persistence
    val widgetRegistry = remember { WidgetRegistry() }

    // Edit mode state
    var editModeState by remember { mutableStateOf(EditModeState()) }

    // Animation manager for sophisticated snapping
    val animationManager = remember { PuckAnimationManager() }

    // Get animation config from system settings
    val animationConfig = getAnimationConfig(LocalContext.current)

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
                widgetRegistry = widgetRegistry,
                editModeState = editModeState,
                hoveredWidgetId = puckState.hoveredButtonId,
                onDismiss = {
                    puckState =
                        puckState.copy(
                            mode = OverlayMode.HIDDEN,
                            isDragging = false,
                            hoveredButtonId = null,
                            initialTouchOffset =
                                androidx.compose.ui.geometry.Offset.Zero
                        )
                    editModeState = EditModeState() // Reset edit mode when dismissing
                },
                onEditModeToggle = {
                    editModeState = editModeState.copy(
                        isEditMode = !editModeState.isEditMode,
                        selectedWidgetId = null
                    )
                },
                onWidgetSelected = { widgetId ->
                    editModeState = editModeState.copy(selectedWidgetId = widgetId)
                },
                onWidgetDragStart = { widgetId, startPosition ->
                    editModeState = editModeState.copy(dragStartPosition = startPosition)
                },
                onWidgetDragEnd = { widgetId, position, hasCollision ->
                    // Widget handles its own collision reversion
                    editModeState = editModeState.copy(dragStartPosition = null)
                }
            )
        }

        // Floating puck - only consume touches when interacting
        MagicPuck(
            editModeState = editModeState,
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

                        OverlayMode.EDIT_MODE -> {
                            // In edit mode, use standard positioning
                            IntOffset(
                                with(density) {
                                    animatedPuckPosition.x.toPx().roundToInt()
                                },
                                with(density) {
                                    animatedPuckPosition.y.toPx().roundToInt()
                                }
                            )
                        }
                    }
                },
            size = puckSize,
            isDragging = puckState.isDragging,
            onTap = {
                // Toggle overlay modes based on current state
                when {
                    editModeState.isEditMode && puckState.mode != OverlayMode.HIDDEN -> {
                        // In edit mode, select the puck for adjustment
                        editModeState = editModeState.copy(selectedWidgetId = "puck")
                        // Register puck as a widget for adjustment
                        widgetRegistry.registerWidget(
                            PuckDesc(widgetPosition = puckState.position, widgetSize = puckSize)
                        )
                    }

                    puckState.mode == OverlayMode.TAP_MODE || puckState.mode == OverlayMode.EDIT_MODE -> {
                        // Close overlay
                        puckState = puckState.copy(
                            mode = OverlayMode.HIDDEN,
                            hoveredButtonId = null,
                            initialTouchOffset = androidx.compose.ui.geometry.Offset.Zero
                        )
                        editModeState = EditModeState() // Reset edit mode
                    }

                    else -> {
                        // Open overlay
                        puckState = puckState.copy(
                            mode = if (editModeState.isEditMode) OverlayMode.EDIT_MODE else OverlayMode.TAP_MODE,
                            initialTouchOffset = androidx.compose.ui.geometry.Offset.Zero
                        )
                    }
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
                    val hoveredWidget =
                        if (puckState.mode != OverlayMode.HIDDEN && widgetRegistry.widgets.isNotEmpty()) {
                            // Pre-calculate visual center
                            val visualCenterX =
                                newX * density.density + puckState.initialTouchOffset.x
                            val visualCenterY =
                                newY * density.density + puckState.initialTouchOffset.y
                            val visualCenter =
                                androidx.compose.ui.geometry.Offset(visualCenterX, visualCenterY)
                            widgetRegistry.findWidgetOverlappingWith(visualCenter, puckRadiusPx)
                        } else null

                    // Add drag vector and update state in one operation
                    currentDragState.weightedCalculator.addDragVector(dragAmount)
                    val updatedDragState = currentDragState.copy(currentPosition = newPosition)

                    puckState = puckState.copy(
                        position = newPosition,
                        dragState = updatedDragState,
                        hoveredButtonId = hoveredWidget
                    )
                }
            },
            onDragEnd = {
                // Optimized drag end handling
                val droppedOnWidget =
                    if (puckState.mode != OverlayMode.HIDDEN && widgetRegistry.widgets.isNotEmpty()) {
                        // Pre-calculate visual center for final collision check
                        val visualCenterX =
                            puckState.position.x.value * density.density + puckState.initialTouchOffset.x
                        val visualCenterY =
                            puckState.position.y.value * density.density + puckState.initialTouchOffset.y
                        val visualCenter =
                            androidx.compose.ui.geometry.Offset(visualCenterX, visualCenterY)
                        widgetRegistry.findWidgetOverlappingWith(visualCenter, puckRadiusPx)
                    } else null

                if (droppedOnWidget != null) {
                    // Puck was dropped on a widget - handle the action
                    widgetRegistry.handleWidgetAction(droppedOnWidget)
                    // Snap to edge with simple animation
                    val snappedPosition = puckState.position.snapToEdge(screenWidth, puckSize)
                    puckState =
                        puckState.copy(
                            position = snappedPosition,
                            isDragging = false,
                            mode = if (editModeState.isEditMode) OverlayMode.EDIT_MODE else OverlayMode.HIDDEN,
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
                                mode = if (editModeState.isEditMode) OverlayMode.EDIT_MODE else OverlayMode.HIDDEN,
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
                                mode = if (editModeState.isEditMode) OverlayMode.EDIT_MODE else OverlayMode.HIDDEN,
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

/**
 * Semi-transparent overlay content shown when puck is activated.
 * Contains widgets, edit mode controls, and debug information.
 */
@Composable
fun ContentOverlay(
    mode: OverlayMode,
    puckPosition: PuckPosition,
    widgetRegistry: WidgetRegistry,
    editModeState: EditModeState,
    hoveredWidgetId: String? = null,
    onDismiss: () -> Unit,
    onEditModeToggle: () -> Unit,
    onWidgetSelected: (String) -> Unit,
    onWidgetDragStart: (String, PuckPosition) -> Unit,
    onWidgetDragEnd: (String, PuckPosition, Boolean) -> Unit
) {
    val context = LocalContext.current
    val screenHeight = androidx.compose.ui.platform.LocalConfiguration.current.screenHeightDp.dp
    val screenWidth = androidx.compose.ui.platform.LocalConfiguration.current.screenWidthDp.dp

    // Initialize default widgets if registry is empty
    LaunchedEffect(widgetRegistry.widgets.isEmpty()) {
        if (widgetRegistry.widgets.isEmpty()) {
            widgetRegistry.getDefaultWidgets(screenWidth).forEach { widget ->
                widgetRegistry.registerWidget(widget)
            }
        }
    }

    Surface(
        modifier = Modifier
            .fillMaxSize()
            .pointerInput(Unit) {}, // Consume touch events
        color = Color.Black.copy(alpha = 0.7f)
    ) {
        Box(modifier = Modifier.fillMaxSize()) {
            // Main overlay content
            Column(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(16.dp)
            ) {

                // Top bar with close and edit buttons
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    // Edit button
                    IconButton(
                        onClick = onEditModeToggle,
                        modifier = Modifier.background(
                            color = if (editModeState.isEditMode)
                                Color.Blue.copy(alpha = 0.3f) else Color.Transparent,
                            shape = CircleShape
                        )
                    ) {
                        Icon(
                            imageVector = Icons.Filled.Edit,
                            contentDescription = if (editModeState.isEditMode) "Exit Edit Mode" else "Enter Edit Mode",
                            tint = if (editModeState.isEditMode) Color.Cyan else Color.White,
                        )
                    }

                    // Close button
                    IconButton(onClick = onDismiss) {
                        Icon(
                            imageVector = Icons.Filled.Close,
                            contentDescription = "Dismiss Overlay",
                            tint = Color.White,
                        )
                    }
                }

                Spacer(modifier = Modifier.height(50.dp))

                // Title and mode indicator
                Column(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalAlignment = Alignment.CenterHorizontally
                ) {
                    Text(
                        text = if (editModeState.isEditMode) "Edit Mode" else "Magic Wand Overlay",
                        style = MaterialTheme.typography.headlineMedium,
                        color = if (editModeState.isEditMode) Color.Cyan else Color.White
                    )

                    if (editModeState.isEditMode) {
                        Text(
                            text = "Drag widgets to move • Tap to select • Adjust size below",
                            style = MaterialTheme.typography.bodyMedium,
                            color = Color.White.copy(alpha = 0.8f)
                        )
                    }
                }

                Spacer(modifier = Modifier.height(32.dp))

                // Debug information (only when not in edit mode and in drag mode)
                if (!editModeState.isEditMode && mode == OverlayMode.DRAG_MODE) {
                    Column(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalAlignment = Alignment.CenterHorizontally
                    ) {
                        Text(
                            "Drag puck to a widget or drop to dismiss",
                            color = Color.White.copy(alpha = 0.8f),
                            style = MaterialTheme.typography.bodyMedium
                        )

                        // Debug text (optimized)
                        val puckXInt =
                            remember(puckPosition.x) { puckPosition.x.value.roundToInt() }
                        val puckYInt =
                            remember(puckPosition.y) { puckPosition.y.value.roundToInt() }

                        Text(
                            "Puck: (${puckXInt}dp, ${puckYInt}dp)",
                            color = Color.White.copy(alpha = 0.6f),
                            style = MaterialTheme.typography.bodySmall
                        )

                        hoveredWidgetId?.let { widgetId ->
                            Text(
                                "Hovering: $widgetId",
                                color = Color.Yellow.copy(alpha = 0.8f),
                                style = MaterialTheme.typography.bodySmall
                            )
                        }
                    }
                } else if (!editModeState.isEditMode && mode == OverlayMode.TAP_MODE) {
                    Text(
                        "Tap widgets or drag puck to them",
                        color = Color.White.copy(alpha = 0.8f),
                        style = MaterialTheme.typography.bodyMedium,
                        modifier = Modifier.fillMaxWidth(),
                        textAlign = TextAlign.Center
                    )
                }
            }

            // Render widgets
            widgetRegistry.widgets.values.forEach { widget ->
                when (widget) {
                    is ActionButtonDesc -> {
                        OverlayWidget(
                            widget = widget,
                            widgetRegistry = widgetRegistry,
                            editModeState = editModeState,
                            isHovered = hoveredWidgetId == widget.id,
                            onWidgetSelected = onWidgetSelected,
                            onWidgetDragStart = onWidgetDragStart,
                            onWidgetDragEnd = onWidgetDragEnd,
                            onWidgetAction = { widgetId ->
                                if (!editModeState.isEditMode) {
                                    widget.onAction()
                                    Toast.makeText(
                                        context,
                                        "${widget.label} action triggered!",
                                        Toast.LENGTH_SHORT
                                    ).show()
                                }
                            }
                        ) { desc, isSelected, isHovered, modifier ->
                            ActionButtonContent(
                                actionButton = desc as ActionButtonDesc,
                                isSelected = isSelected,
                                isHovered = isHovered,
                                modifier = modifier
                            )
                        }
                    }
                }
            }

            // Widget adjustment controls (only in edit mode)
            if (editModeState.isEditMode) {
                val selectedWidget = editModeState.selectedWidgetId?.let { id ->
                    widgetRegistry.widgets[id]
                }

                WidgetAdjustmentControls(
                    selectedWidget = selectedWidget,
                    widgetRegistry = widgetRegistry,
                    screenHeight = screenHeight
                )
            }
        }
    }
}

/**
 * Generic overlay widget that handles dragging, selection, and collision detection.
 *
 * Features:
 * - Drag and drop with collision detection in edit mode
 * - Selection highlighting and controls
 * - Hover animations for puck interactions
 * - Automatic position updates to WidgetRegistry
 */
@Composable
fun OverlayWidget(
    widget: WidgetDesc,
    widgetRegistry: WidgetRegistry,
    editModeState: EditModeState,
    isHovered: Boolean = false,
    onWidgetSelected: (String) -> Unit = {},
    onWidgetDragStart: (String, PuckPosition) -> Unit = { _, _ -> },
    onWidgetDragEnd: (String, PuckPosition, Boolean) -> Unit = { _, _, _ -> },
    onWidgetAction: (String) -> Unit = {},
    modifier: Modifier = Modifier,
    content: @Composable (WidgetDesc, Boolean, Boolean, Modifier) -> Unit
) {
    val density = LocalDensity.current
    var isDragging by remember { mutableStateOf(false) }
    var dragStartPosition by remember { mutableStateOf<PuckPosition?>(null) }
    var currentPosition by remember { mutableStateOf(widget.position) }

    val isSelected = editModeState.selectedWidgetId == widget.id
    val radiusPx = with(density) { (widget.size / 2).toPx() }

    // Update current position when widget position changes in registry
    LaunchedEffect(widget.position) {
        if (!isDragging) {
            currentPosition = widget.position
        }
    }

    // Hover and selection animations
    val animatedScale by animateFloatAsState(
        targetValue = when {
            isDragging -> 1.2f
            isHovered -> 1.1f
            isSelected -> 1.05f
            else -> 1f
        },
        animationSpec = spring(
            dampingRatio = Spring.DampingRatioMediumBouncy,
            stiffness = Spring.StiffnessHigh
        ),
        label = "widgetScale"
    )

    val animatedGlow by animateFloatAsState(
        targetValue = when {
            isDragging -> 1f
            isHovered -> 0.8f
            isSelected -> 0.6f
            else -> 0f
        },
        animationSpec = spring(
            dampingRatio = Spring.DampingRatioLowBouncy,
            stiffness = Spring.StiffnessMedium
        ),
        label = "widgetGlow"
    )

    // Clean up when widget is removed from composition
    DisposableEffect(widget.id) {
        onDispose {
            // Widget cleanup if needed
        }
    }

    Box(
        modifier = modifier
            .offset {
                IntOffset(
                    with(density) { currentPosition.x.toPx().roundToInt() },
                    with(density) { currentPosition.y.toPx().roundToInt() }
                )
            }
            .size(widget.size)
            .graphicsLayer {
                scaleX = animatedScale
                scaleY = animatedScale
            }
            .onGloballyPositioned { coordinates ->
                // Register collision data for this widget
                val centerX = coordinates.positionInWindow().x + coordinates.size.width / 2f
                val centerY = coordinates.positionInWindow().y + coordinates.size.height / 2f
                val center = androidx.compose.ui.geometry.Offset(centerX, centerY)
                widgetRegistry.registerCollisionData(widget.id, center, radiusPx)
            }
            .pointerInput(widget.id, editModeState.isEditMode) {
                if (editModeState.isEditMode) {
                    // Edit mode: handle dragging
                    detectDragGestures(
                        onDragStart = { offset ->
                            isDragging = true
                            dragStartPosition = currentPosition
                            onWidgetDragStart(widget.id, currentPosition)
                        },
                        onDrag = { _, dragAmount ->
                            if (isDragging) {
                                val newX =
                                    (currentPosition.x.value + dragAmount.x / density.density).coerceIn(
                                        0f, 1000f // Use a reasonable max value
                                    )
                                val newY =
                                    (currentPosition.y.value + dragAmount.y / density.density).coerceIn(
                                        0f, 2000f // Use a reasonable max value
                                    )
                                currentPosition = PuckPosition(newX.dp, newY.dp)
                            }
                        },
                        onDragEnd = {
                            if (isDragging) {
                                isDragging = false

                                // Check for collision using current position
                                val centerX =
                                    currentPosition.x.value * density.density + widget.size.value * density.density / 2
                                val centerY =
                                    currentPosition.y.value * density.density + widget.size.value * density.density / 2
                                val center = androidx.compose.ui.geometry.Offset(centerX, centerY)

                                val hasCollision = widgetRegistry.checkCollision(
                                    widget.id, center, radiusPx
                                ) != null

                                if (hasCollision) {
                                    // Revert to start position
                                    currentPosition = dragStartPosition ?: widget.position
                                } else {
                                    // Update registry with new position
                                    widgetRegistry.updateWidgetPosition(widget.id, currentPosition)
                                }

                                onWidgetDragEnd(widget.id, currentPosition, hasCollision)
                                dragStartPosition = null
                            }
                        }
                    )
                }
            }
            .clickable(enabled = !isDragging) {
                if (editModeState.isEditMode) {
                    onWidgetSelected(widget.id)
                } else {
                    onWidgetAction(widget.id)
                }
            }
    ) {
        // Selection border
        if (isSelected) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .border(
                        width = 3.dp,
                        color = Color.Cyan,
                        shape = CircleShape
                    )
            )
        }

        // Hover glow effect - only show border, not shadow
        if (animatedGlow > 0f) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .border(
                        width = (2 * animatedGlow).dp,
                        color = Color.White.copy(alpha = animatedGlow * 0.5f),
                        shape = CircleShape
                    )
            )
        }

        // Widget content
        content(widget, isSelected, isHovered, Modifier.fillMaxSize())
    }
}

/**
 * Action button content for OverlayWidget.
 */
@Composable
fun ActionButtonContent(
    actionButton: ActionButtonDesc,
    isSelected: Boolean,
    isHovered: Boolean,
    modifier: Modifier = Modifier
) {
    Button(
        onClick = { /* Handled by OverlayWidget */ },
        modifier = modifier.clip(CircleShape),
        shape = CircleShape,
        colors = ButtonDefaults.buttonColors(
            containerColor = actionButton.color,
            contentColor = Color.White
        ),
        elevation = ButtonDefaults.buttonElevation(0.dp, 0.dp, 0.dp, 0.dp, 0.dp)
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center
        ) {
            Text(text = actionButton.icon, style = MaterialTheme.typography.headlineSmall)
            Text(
                text = actionButton.label,
                style = MaterialTheme.typography.labelSmall,
                maxLines = 1
            )
        }
    }
}

/**
 * Adjustment controls for editing widget properties in edit mode.
 *
 * Positions itself opposite to the selected widget to stay visible.
 * Currently supports size adjustment with plans for more properties.
 */
@Composable
fun WidgetAdjustmentControls(
    selectedWidget: WidgetDesc?,
    widgetRegistry: WidgetRegistry,
    screenHeight: Dp,
    modifier: Modifier = Modifier
) {
    if (selectedWidget == null) return

    val density = LocalDensity.current

    // Get the current widget from registry to ensure we have the latest size
    val currentWidget = widgetRegistry.widgets[selectedWidget.id]
    if (currentWidget == null) return

    // Calculate position opposite to the selected widget
    val controlsHeight = 80.dp
    val controlsY = if (selectedWidget.position.y > screenHeight / 2) {
        // Widget is in bottom half, show controls at top
        32.dp
    } else {
        // Widget is in top half, show controls at bottom
        screenHeight - controlsHeight - 32.dp
    }

    Box(
        modifier = modifier
            .fillMaxWidth()
            .height(controlsHeight)
            .offset(y = controlsY)
            .padding(horizontal = 16.dp)
    ) {
        Surface(
            modifier = Modifier.fillMaxSize(),
            shape = androidx.compose.foundation.shape.RoundedCornerShape(12.dp),
            color = Color.Black.copy(alpha = 0.8f),
            border = androidx.compose.foundation.BorderStroke(1.dp, Color.White.copy(alpha = 0.3f))
        ) {
            Row(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(16.dp),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                // Widget info
                Column {
                    Text(
                        text = when (currentWidget) {
                            is ActionButtonDesc -> currentWidget.label
                            is PuckDesc -> "Puck"
                            else -> "Widget"
                        },
                        color = Color.White,
                        style = MaterialTheme.typography.titleMedium
                    )
                    Text(
                        text = "Size: ${currentWidget.size.value.roundToInt()}dp",
                        color = Color.White.copy(alpha = 0.7f),
                        style = MaterialTheme.typography.bodySmall
                    )
                }

                // Size adjustment controls
                Row(
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    // Decrease size button
                    IconButton(
                        onClick = {
                            val newSize = (currentWidget.size.value - 8f).coerceAtLeast(32f).dp
                            widgetRegistry.updateWidgetSize(currentWidget.id, newSize)
                        }
                    ) {
                        Text(
                            text = "−",
                            color = Color.White,
                            style = MaterialTheme.typography.titleLarge
                        )
                    }

                    // Size display
                    Text(
                        text = "${currentWidget.size.value.roundToInt()}",
                        color = Color.White,
                        style = MaterialTheme.typography.titleMedium,
                        modifier = Modifier.padding(horizontal = 8.dp)
                    )

                    // Increase size button
                    IconButton(
                        onClick = {
                            val newSize = (currentWidget.size.value + 8f).coerceAtMost(120f).dp
                            widgetRegistry.updateWidgetSize(currentWidget.id, newSize)
                        }
                    ) {
                        Text(
                            text = "+",
                            color = Color.White,
                            style = MaterialTheme.typography.titleLarge
                        )
                    }
                }
            }
        }
    }
}

/**
 * The floating draggable puck with sophisticated touch handling.
 *
 * Key features:
 * - Distinguishes between taps and drags to prevent accidental triggers
 * - Provides initial touch offset for natural drag feel
 * - Optimized animations with cached calculations
 */
@Composable
fun MagicPuck(
    editModeState: EditModeState = EditModeState(),
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
