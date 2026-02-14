package org.example.daybook.tables

// offset and drawContent are provided by different packages on some platforms; use graphicsLayer
// for translation and drawWithContent's this.drawContent() is available in the lambda.
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.AnimationSpec
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.requiredHeightIn
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.BottomSheetDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.contentColorFor
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.Stable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.drawWithContent
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.graphics.TransformOrigin
import androidx.compose.ui.graphics.drawscope.clipRect
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.nestedscroll.NestedScrollConnection
import androidx.compose.ui.input.nestedscroll.NestedScrollSource
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.Layout
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.Velocity
import androidx.compose.ui.unit.dp
import androidx.compose.ui.util.fastForEach
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

@Stable
class RevealBottomSheetState(
    private val scope: CoroutineScope,
    initialVisible: Boolean,
    initialProgress: Float,
    initialAnchors: List<Float> = listOf(0f, 0.5f, 1f)
) {
    private val anim = Animatable(initialProgress)
    var isVisible by mutableStateOf(initialVisible)
        private set

    // backing progress used for immediate, non-suspending updates during pointer drag
    private val _progressBacking = mutableFloatStateOf(initialProgress)
    val progress: Float
        get() = _progressBacking.floatValue

    fun setProgressImmediate(p: Float) {
        _progressBacking.floatValue = p.coerceIn(0f, 1f)
    }

    // Anchors can be updated at runtime (e.g. different sheet content requires different anchors)
    var anchors: List<Float> = initialAnchors
        private set

    fun setAnchors(newAnchors: List<Float>) {
        anchors = newAnchors
    }

    fun showInstant() {
        isVisible = true
        scope.launch {
            anim.snapTo(1f)
            setProgressImmediate(anim.value)
        }
    }

    fun hideInstant() {
        scope.launch {
            anim.snapTo(0f)
            setProgressImmediate(anim.value)
            isVisible = false
        }
    }

    fun show(animationSpec: AnimationSpec<Float> = tween(0)) {
        scope.launch {
            isVisible = true
            anim.animateTo(1f, animationSpec = animationSpec)
            setProgressImmediate(anim.value)
        }
    }

    fun hide(animationSpec: AnimationSpec<Float> = tween(0)) {
        scope.launch {
            anim.animateTo(0f, animationSpec = animationSpec)
            setProgressImmediate(anim.value)
            isVisible = false
        }
    }

    suspend fun snapToProgress(p: Float) {
        anim.snapTo(p.coerceIn(0f, 1f))
        // keep backing in sync
        setProgressImmediate(anim.value)
    }

    fun showToProgress(p: Float) {
        scope.launch {
            isVisible = true
            anim.snapTo(p.coerceIn(0f, 1f))
            setProgressImmediate(anim.value)
        }
    }

    fun settle(velocity: Float, animationSpec: AnimationSpec<Float> = spring()) {
        scope.launch {
            val current = anim.value
            val anchors = this@RevealBottomSheetState.anchors
            // If a strong fling, bias toward direction
            val biasedTarget =
                when {
                    velocity < -300f -> 1f

                    // strong upward fling -> expand
                    velocity > 300f -> 0f

                    // strong downward fling -> hide
                    else -> null
                }

            val target =
                biasedTarget ?: run {
                    // choose nearest anchor
                    anchors.minByOrNull { kotlin.math.abs(it - current) }
                        ?: if (current < 0.5f) 0f else 1f
                }

            anim.animateTo(target.coerceIn(0f, 1f), animationSpec = animationSpec)
            setProgressImmediate(anim.value)
            isVisible = anim.value > 0f
        }
    }
}

private enum class RevealSheetValue { Hidden, PartiallyExpanded, Expanded }

@OptIn(ExperimentalMaterial3Api::class)
internal fun Modifier.verticalScaleUp(state: RevealBottomSheetState) = this.then(
    graphicsLayer {
        // No-op safe implementation for now; placeholder for future scale logic
        transformOrigin = TransformOrigin(pivotFractionX = 0.5f, pivotFractionY = 0f)
    }
)

@OptIn(ExperimentalMaterial3Api::class)
internal fun Modifier.verticalScaleDown(state: RevealBottomSheetState) = this.then(
    graphicsLayer {
        // Placeholder: no-op for now
        transformOrigin = TransformOrigin(pivotFractionX = 0.5f, pivotFractionY = 0f)
    }
)

@Composable
fun rememberRevealBottomSheetState(
    initiallyVisible: Boolean = false,
    initialProgress: Float = if (initiallyVisible) 1f else 0f
): RevealBottomSheetState {
    val scope = rememberCoroutineScope()
    return remember(scope, initiallyVisible, initialProgress) {
        RevealBottomSheetState(scope, initiallyVisible, initialProgress)
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun RevealBottomSheetScaffold(
    sheetContent: @Composable ColumnScope.() -> Unit,
    modifier: Modifier = Modifier,
    sheetState: RevealBottomSheetState = rememberRevealBottomSheetState(),
    sheetPeekHeight: Dp = 0.dp,
    sheetMaxWidth: Dp = Dp.Unspecified,
    sheetHeader: (@Composable (Modifier) -> Unit)? = null,
    sheetHeaderUnderlay: (@Composable (Modifier) -> Unit)? = null,
    sheetContainerColor: Color = BottomSheetDefaults.ContainerColor,
    sheetContentColor: Color = contentColorFor(sheetContainerColor),
    sheetShape: Shape = BottomSheetDefaults.ExpandedShape,
    sheetTonalElevation: Dp = 0.dp,
    sheetShadowElevation: Dp = BottomSheetDefaults.Elevation,
    sheetDragHandle: (@Composable () -> Unit)? = { BottomSheetDefaults.DragHandle() },
    showAnimationSpec: AnimationSpec<Float> = tween(),
    hideAnimationSpec: AnimationSpec<Float> = tween(),
    settleAnimationSpec: AnimationSpec<Float> = spring(),
    topBar: @Composable (() -> Unit)? = null,
    snackBarHost: @Composable (SnackbarHostState) -> Unit = { SnackbarHost(it) },
    containerColor: Color = MaterialTheme.colorScheme.surface,
    contentColor: Color = contentColorFor(containerColor),
    sheetAnchors: List<Float>? = null,
    content: @Composable (PaddingValues) -> Unit
) {
    val density = LocalDensity.current
    val scope = rememberCoroutineScope()
    // Precompute drawing values so drawWithContent doesn't call composables
    val handleColor = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.12f)
    val handleWidthPx = with(density) { 36.dp.toPx() }
    val handleHeightPx = with(density) { 4.dp.toPx() }
    var sheetHeightPx by remember { mutableIntStateOf(0) }
    val peekPx = with(density) { sheetPeekHeight.toPx() }

    // If caller provided explicit anchors, apply them to the sheet state so settle logic uses them
    LaunchedEffect(sheetState, sheetAnchors) {
        sheetAnchors?.let { sheetState.setAnchors(it) }
    }

    Box(modifier.fillMaxSize().background(containerColor)) {
        // Simple layout: top bar, body, sheet, optional header, then snackbar
        // Header will be composed with a modifier we control so it can receive drag gestures.
        // Make the header background match the sheet container so it visually "follows" the sheet.
        val headerSlotModifierBase = Modifier.background(sheetContainerColor)
        val headerSlotModifier = headerSlotModifierBase
        Layout(
            contents =
                listOf<@Composable () -> Unit>(
                    { if (topBar != null) topBar() else TopAppBar(title = {}) },
                    { content(PaddingValues(bottom = sheetPeekHeight)) },
                    {
                        // nested-scroll connection: convert nested pre-scroll into sheet progress
                        // Use onPostScroll instead of onPreScroll to allow children to consume scroll first
                        val sheetNestedScroll =
                            remember(sheetState, sheetHeightPx, peekPx) {
                                object : NestedScrollConnection {
                                    override fun onPostScroll(
                                        consumed: Offset,
                                        available: Offset,
                                        source: NestedScrollSource
                                    ): Offset {
                                        // Only consume scroll if children didn't consume it (available.y != 0)
                                        val dy = available.y
                                        if (dy == 0f) return Offset.Zero

                                        val total = (sheetHeightPx - peekPx).coerceAtLeast(1f)
                                        if (total <= 0f) return Offset.Zero
                                        val currentVisible = peekPx + total * sheetState.progress
                                        val nextVisible =
                                            (currentVisible - dy).coerceIn(
                                                0f,
                                                sheetHeightPx.toFloat()
                                            )
                                        val nextProgress = ((nextVisible - peekPx) / total).coerceIn(
                                            0f,
                                            1f
                                        )
                                        // synchronous update via suspend scope in pointer handler; here we launch
                                        scope.launch { sheetState.snapToProgress(nextProgress) }
                                        return Offset(0f, dy)
                                    }

                                    override suspend fun onPostFling(
                                        consumed: Velocity,
                                        available: Velocity
                                    ): Velocity {
                                        val vy = available.y.toFloat()
                                        sheetState.settle(vy)
                                        return available
                                    }
                                }
                            }

                        // drawWithContent on Surface to clip background and content to the reveal edge
                        if (sheetState.isVisible) {
                            Surface(
                                modifier =
                                    Modifier
                                        .widthIn(max = sheetMaxWidth)
                                        .fillMaxWidth()
                                        .requiredHeightIn(min = sheetPeekHeight)
                                        .onSizeChanged { sheetHeightPx = it.height }
                                        .drawWithContent {
                                            val total = (sheetHeightPx - peekPx).coerceAtLeast(1f)
                                            val visible =
                                                if (sheetState.isVisible) {
                                                    (
                                                        peekPx +
                                                            total * sheetState.progress
                                                        )
                                                } else {
                                                    0f
                                                }
                                            val revealTop = size.height - visible
                                            clipRect(top = revealTop.coerceAtLeast(0f)) {
                                                this@drawWithContent.drawContent()
                                            }
                                        }.verticalScaleUp(sheetState)
                                        .nestedScroll(sheetNestedScroll)
                                        .pointerInput(sheetState, sheetHeightPx, peekPx) {
                                            detectVerticalDragGestures(
                                                onDragStart = { /* no-op */ },
                                                onDragEnd = {
                                                    scope.launch { sheetState.settle(0f) }
                                                },
                                                onDragCancel = {
                                                    scope.launch { sheetState.settle(0f) }
                                                }
                                            ) { change, dragAmount ->
                                                // pointerInput is a suspend scope; update progress synchronously
                                                val total = (sheetHeightPx - peekPx).coerceAtLeast(
                                                    1f
                                                )
                                                if (total <= 0f) return@detectVerticalDragGestures
                                                val currentVisible =
                                                    peekPx + total * sheetState.progress
                                                val nextVisible =
                                                    (currentVisible - dragAmount).coerceIn(
                                                        0f,
                                                        sheetHeightPx.toFloat()
                                                    )
                                                val nextProgress =
                                                    ((nextVisible - peekPx) / total).coerceIn(
                                                        0f,
                                                        1f
                                                    )
                                                change.consume()
                                                // update backing progress immediately for tight finger follow
                                                sheetState.setProgressImmediate(nextProgress)
                                            }
                                        },
                                color = sheetContainerColor,
                                contentColor = sheetContentColor
                            ) {
                                // compute reveal geometry (pixels)
                                val total = (sheetHeightPx - peekPx).coerceAtLeast(1f)
                                val visiblePx =
                                    if (sheetState.isVisible) (peekPx + total * sheetState.progress) else 0f
                                val revealTopPx = (sheetHeightPx - visiblePx).coerceAtLeast(0f)

                                // Layout: header/handle sit above the clipped content but inside the Surface.
                                // The clipped content is revealed from the bottom by clipping its top edge.
                                Box(Modifier.fillMaxWidth().fillMaxHeight()) {
                                    // header/handle area removed here; header is provided by the header slot and
                                    // will be positioned by the parent Layout so it participates in hit testing.

                                    // clipped content region (revealed from bottom)
                                    Box(
                                        Modifier
                                            .matchParentSize()
                                            .drawWithContent {
                                                val total = (sheetHeightPx - peekPx).coerceAtLeast(
                                                    1f
                                                )
                                                val visible =
                                                    if (sheetState.isVisible) {
                                                        (
                                                            peekPx +
                                                                total * sheetState.progress
                                                            )
                                                    } else {
                                                        0f
                                                    }
                                                val revealTop = size.height - visible
                                                clipRect(top = revealTop.coerceAtLeast(0f)) {
                                                    this@drawWithContent.drawContent()
                                                }
                                            }
                                    ) {
                                        Column(
                                            Modifier
                                                .fillMaxWidth()
                                                .fillMaxHeight(),
                                            verticalArrangement = Arrangement.Bottom
                                        ) {
                                            sheetContent()
                                        }
                                    }
                                }
                            }
                        }
                    },
                    // underlay slot measured separately and anchored to the header reveal position
                    {
                        if (sheetHeaderUnderlay != null) {
                            sheetHeaderUnderlay(Modifier)
                        }
                    },
                    // header slot measured separately so we can position it relative to the reveal edge
                    {
                        if (sheetHeader != null) {
                            sheetHeader(
                                headerSlotModifier.then(
                                    Modifier
                                        .pointerInput(Unit) {
                                            // forward vertical drags from header into the same draggable state
                                            detectVerticalDragGestures(
                                                onDragStart = { /* no-op */ },
                                                onDragEnd = {
                                                    scope.launch { sheetState.settle(0f) }
                                                },
                                                onDragCancel = {
                                                    scope.launch { sheetState.settle(0f) }
                                                }
                                            ) { change, dragAmount ->
                                                // convert drag into sheet progress updates - reuse the same logic
                                                val total = (sheetHeightPx - peekPx).coerceAtLeast(
                                                    1f
                                                )
                                                val currentVisible =
                                                    peekPx + total * sheetState.progress
                                                val nextVisible =
                                                    (currentVisible - dragAmount).coerceIn(
                                                        0f,
                                                        sheetHeightPx.toFloat()
                                                    )
                                                val nextProgress =
                                                    ((nextVisible - peekPx) / total).coerceIn(
                                                        0f,
                                                        1f
                                                    )
                                                change.consume()
                                                scope.launch {
                                                    sheetState.snapToProgress(nextProgress)
                                                }
                                            }
                                        }
                                )
                            )
                        }
                    },
                    { snackBarHost(remember { SnackbarHostState() }) }
                )
        ) { measurables, constraints ->
            val topBarMeasurables = measurables[0]
            val bodyMeasurables = measurables[1]
            val sheetMeasurables = measurables[2]
            val underlayMeasurables = measurables[3]
            val headerMeasurables = measurables[4]
            val snackbarMeasurables = measurables[5]
            val layoutWidth = constraints.maxWidth
            val layoutHeight = constraints.maxHeight
            val loose = constraints.copy(minWidth = 0, minHeight = 0)

            val topBarPlaceables = topBarMeasurables.map { it.measure(loose) }
            val topBarHeight = topBarPlaceables.maxOfOrNull { it.height } ?: 0
            val bodyConstraints = loose.copy(maxHeight = layoutHeight - topBarHeight)
            val bodyPlaceables = bodyMeasurables.map { it.measure(bodyConstraints) }
            val sheetPlaceables = sheetMeasurables.map { it.measure(loose) }
            val underlayPlaceables = underlayMeasurables.map { it.measure(loose) }
            val headerPlaceables = headerMeasurables.map { it.measure(loose) }
            val snackbarPlaceables = snackbarMeasurables.map { it.measure(loose) }

            layout(layoutWidth, layoutHeight) {
                bodyPlaceables.fastForEach { it.placeRelative(0, topBarHeight) }
                topBarPlaceables.fastForEach { it.placeRelative(0, 0) }

                // Place sheet fixed at bottom (content draw is clipped by modifier inside the sheet)
                sheetPlaceables.fastForEach { p ->
                    val left = (layoutWidth - p.width) / 2
                    p.placeRelative(left, layoutHeight - p.height)
                }

                if (underlayPlaceables.isNotEmpty() || headerPlaceables.isNotEmpty()) {
                    // compute visible px from sheetHeightPx and progress
                    val total = (sheetHeightPx - peekPx).coerceAtLeast(1f)
                    val visiblePx =
                        if (sheetState.isVisible) (peekPx + total * sheetState.progress) else 0f
                    val headerTopGlobal = (layoutHeight - visiblePx).toInt()


                    // Place underlay first so header can partially obscure it.
                    underlayPlaceables.fastForEach { up ->
                        val ux = (layoutWidth - up.width) / 2
                        up.placeRelative(ux, headerTopGlobal)
                    }
                    // Position header above the sheet at the reveal edge if provided
                    headerPlaceables.fastForEach { hp ->
                        val hx = (layoutWidth - hp.width) / 2
                        // place header so its top aligns with reveal edge
                        // also ensure the header is part of the sheet's touch target by placing it
                        // directly above the sheet surface
                        hp.placeRelative(hx, headerTopGlobal)
                    }
                }

                // Place snackbar at bottom of layout
                snackbarPlaceables.fastForEach { sp ->
                    val sx = (layoutWidth - sp.width) / 2
                    val sy = layoutHeight - sp.height
                    sp.placeRelative(sx, sy)
                }
            }
        }
    }
}
