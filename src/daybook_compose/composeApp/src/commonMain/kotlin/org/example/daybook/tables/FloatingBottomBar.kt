package org.example.daybook.tables

import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.draggable
import androidx.compose.foundation.gestures.rememberDraggableState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ChevronRight
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.geometry.RoundRect
import androidx.compose.ui.graphics.Outline
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.LayoutDirection
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.times
import kotlinx.coroutines.launch

private object FloatingBarDefaults {
    val barHeight = 64.dp
    val horizontalPadding = 16.dp
    val verticalPadding = 8.dp
    val menuTopRadius = 28.dp
}

private class NippleBarShape(private val protrusionPx: Float, private val cornerRadiusPx: Float) :
    Shape {
    override fun createOutline(
        size: androidx.compose.ui.geometry.Size,
        layoutDirection: LayoutDirection,
        density: androidx.compose.ui.unit.Density
    ): Outline {
        val protrusion = protrusionPx.coerceAtLeast(0f)
        val centerY = size.height / 2f
        val nippleRadius = (size.height * 0.16f).coerceAtLeast(1f)
        val bodyLeft = protrusion
        val bodyRight = size.width - protrusion

        val path = Path().apply {
            addRoundRect(
                RoundRect(
                    left = bodyLeft,
                    top = 0f,
                    right = bodyRight,
                    bottom = size.height,
                    cornerRadius = CornerRadius(cornerRadiusPx, cornerRadiusPx)
                )
            )
            if (protrusion > 0f) {
                addOval(
                    Rect(
                        left = 0f,
                        top = centerY - nippleRadius,
                        right = protrusion * 2f,
                        bottom = centerY + nippleRadius
                    )
                )
                addOval(
                    Rect(
                        left = size.width - protrusion * 2f,
                        top = centerY - nippleRadius,
                        right = size.width,
                        bottom = centerY + nippleRadius
                    )
                )
            }
        }
        return Outline.Generic(path)
    }
}

@Composable
fun FloatingBottomNavigationBar(
    centerContent: @Composable RowScope.() -> Unit,
    menuOpenProgress: Float,
    onBarHeightChanged: (Dp) -> Unit = {},
    bottomBarModifier: Modifier = Modifier
) {
    val density = LocalDensity.current
    val animatedOpenProgress by animateFloatAsState(
        targetValue = menuOpenProgress.coerceIn(0f, 1f),
        animationSpec = tween(durationMillis = 180),
        label = "floating_bar_open_progress"
    )
    val protrusionFraction = 1f - animatedOpenProgress
    val protrusionPx = with(density) { 8.dp.toPx() } * protrusionFraction
    val cornerRadiusPx = with(density) { 28.dp.toPx() }
    val nippleAlpha = protrusionFraction

    Box(
        modifier =
            bottomBarModifier
                .fillMaxWidth()
                .safeContentPadding()
                .padding(
                    horizontal = FloatingBarDefaults.horizontalPadding,
                    vertical = FloatingBarDefaults.verticalPadding
                )
    ) {
        Surface(
            modifier =
                Modifier
                    .fillMaxWidth()
                    .onSizeChanged {
                        onBarHeightChanged(with(density) { it.height.toDp() })
                    },
            color = MaterialTheme.colorScheme.surfaceContainerLow.copy(alpha = 0.94f),
            shape = NippleBarShape(protrusionPx = protrusionPx, cornerRadiusPx = cornerRadiusPx),
            shadowElevation = 10.dp,
            tonalElevation = 0.dp
        ) {
            Row(
                modifier = Modifier.fillMaxWidth().padding(horizontal = 20.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                centerContent()
            }
        }

        Icon(
            imageVector = Icons.Default.ChevronRight,
            contentDescription = "Swipe right for drawer",
            tint = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.78f * nippleAlpha),
            modifier = Modifier.align(
                Alignment.CenterStart
            ).padding(start = 7.dp).width(13.dp).height(13.dp)
        )
        Icon(
            imageVector = Icons.Default.KeyboardArrowUp,
            contentDescription = "Swipe up for menu",
            tint = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.78f * nippleAlpha),
            modifier = Modifier.align(
                Alignment.CenterEnd
            ).padding(end = 7.dp).width(13.dp).height(13.dp)
        )
    }
}

@Composable
fun FloatingGrowingMenuSheet(
    sheetState: RevealBottomSheetState,
    maxAnchor: Float,
    barHeight: Dp,
    menuItems: List<FeatureItem>,
    highlightedMenuItem: String?,
    activationReadyMenuItem: String?,
    enableDragToClose: Boolean,
    onMenuItemLayout: (key: String, rect: Rect) -> Unit,
    onDismiss: () -> Unit,
    onItemActivate: suspend (FeatureItem) -> Unit,
    modifier: Modifier = Modifier
) {
    if (!sheetState.isVisible) return

    val scope = rememberCoroutineScope()
    val density = LocalDensity.current
    val maxMenuHeight = remember { 560.dp }
    val maxMenuHeightPx = with(density) { maxMenuHeight.toPx().coerceAtLeast(1f) }
    val bottomInset = barHeight + 8.dp
    val flingCloseThreshold = 220f
    val armedIndicatorColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.95f)
    val menuItemShape = RoundedCornerShape(28.dp)

    Box(
        modifier = modifier.fillMaxSize().padding(
            horizontal = FloatingBarDefaults.horizontalPadding
        )
    ) {
        val openFraction = (sheetState.progress / maxAnchor).coerceIn(0f, 1f)
        val targetHeight = (maxMenuHeight * openFraction)
        val dragModifier =
            Modifier.draggable(
                state =
                    rememberDraggableState { dragAmount ->
                        val total = maxMenuHeightPx
                        val boundedProgress = sheetState.progress.coerceIn(0f, maxAnchor)
                        val currentVisible = total * (boundedProgress / maxAnchor).coerceIn(0f, 1f)
                        val nextVisible = (currentVisible - dragAmount).coerceIn(0f, total)
                        val nextProgress = ((nextVisible / total) * maxAnchor).coerceIn(
                            0f,
                            maxAnchor
                        )
                        sheetState.setProgressImmediate(nextProgress)
                    },
                orientation = Orientation.Vertical,
                onDragStopped = { velocityY ->
                    if (velocityY > flingCloseThreshold) {
                        scope.launch {
                            val anim = Animatable(sheetState.progress.coerceIn(0f, maxAnchor))
                            anim.animateTo(0f, animationSpec = tween(durationMillis = 200)) {
                                sheetState.setProgressImmediate(value)
                            }
                            sheetState.hideInstant()
                            onDismiss()
                        }
                    } else {
                        sheetState.settle(velocityY) { settledProgress ->
                            if (settledProgress <= 0f) {
                                onDismiss()
                            }
                        }
                    }
                }
            )
        val barDragAreaHeight = barHeight + FloatingBarDefaults.verticalPadding * 2
        val surfaceHeight = targetHeight.coerceAtLeast(1.dp)
        val effectiveDragModifier = if (enableDragToClose) dragModifier else Modifier

        Surface(
            modifier =
                Modifier
                    .align(Alignment.BottomCenter)
                    .fillMaxWidth()
                    .padding(bottom = FloatingBarDefaults.verticalPadding)
                    .height(surfaceHeight)
                    .then(effectiveDragModifier),
            color = MaterialTheme.colorScheme.surfaceContainerLow.copy(alpha = 0.96f),
            shape = RoundedCornerShape(
                topStart = FloatingBarDefaults.menuTopRadius,
                topEnd = FloatingBarDefaults.menuTopRadius,
                bottomStart = 28.dp,
                bottomEnd = 28.dp
            ),
            shadowElevation = 12.dp,
            tonalElevation = 0.dp
        ) {
            Column(
                modifier = Modifier.fillMaxSize().padding(horizontal = 16.dp),
                verticalArrangement = Arrangement.Bottom
            ) {
                Box(
                    modifier =
                        Modifier
                            .fillMaxWidth()
                            .padding(top = 8.dp, bottom = 6.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Box(
                        modifier =
                            Modifier
                                .height(4.dp)
                                .width(36.dp)
                                .clip(RoundedCornerShape(2.dp))
                                .background(MaterialTheme.colorScheme.onSurface.copy(alpha = 0.18f))
                    )
                }
                Spacer(Modifier.weight(1f, fill = true))
                Column(
                    modifier =
                        Modifier
                            .fillMaxWidth()
                            .verticalScroll(rememberScrollState()),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    menuItems.forEach { item ->
                        val isActivationReady = item.key == activationReadyMenuItem
                        NavigationDrawerItem(
                            selected = item.key == highlightedMenuItem,
                            onClick = {
                                scope.launch {
                                    if (item.enabled) {
                                        onItemActivate(item)
                                    }
                                }
                            },
                            icon = { item.icon() },
                            label = { item.labelContent?.invoke() ?: Text(item.label) },
                            shape = menuItemShape,
                            modifier =
                                Modifier
                                    .fillMaxWidth()
                                    .then(
                                        if (isActivationReady) {
                                            Modifier.border(
                                                width = 1.5.dp,
                                                color = armedIndicatorColor,
                                                shape = menuItemShape
                                            )
                                        } else {
                                            Modifier
                                        }
                                    )
                                    .onGloballyPositioned {
                                        onMenuItemLayout(item.key, it.boundsInWindow())
                                    }
                        )
                    }
                    Spacer(Modifier.height(bottomInset))
                }
            }
        }
        Box(
            modifier =
                Modifier
                    .align(Alignment.BottomCenter)
                    .fillMaxWidth()
                    .height(barDragAreaHeight)
                    .then(effectiveDragModifier)
        )
    }
}
