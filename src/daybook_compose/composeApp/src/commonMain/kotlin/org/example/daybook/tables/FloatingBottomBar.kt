package org.example.daybook.tables

import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
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
import androidx.compose.foundation.layout.heightIn
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
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.drawWithContent
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.geometry.RoundRect
import androidx.compose.ui.graphics.Outline
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.graphics.drawscope.clipRect
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.LayoutDirection
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch

private object FloatingBarDefaults {
    val barHeight = 56.dp
    val horizontalPadding = 16.dp
    val verticalPadding = 2.dp
    val menuTopRadius = 28.dp
}

private class NippleBarShape(
    private val protrusionPx: Float,
    private val cornerRadiusPx: Float
) : Shape {
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
            modifier = Modifier.fillMaxWidth().height(FloatingBarDefaults.barHeight),
            color = MaterialTheme.colorScheme.surfaceContainerLow.copy(alpha = 0.94f),
            shape = NippleBarShape(protrusionPx = protrusionPx, cornerRadiusPx = cornerRadiusPx),
            shadowElevation = 10.dp,
            tonalElevation = 0.dp
        ) {
            Row(
                modifier = Modifier.fillMaxSize().padding(horizontal = 30.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                centerContent()
            }
        }

        Icon(
            imageVector = Icons.Default.ChevronRight,
            contentDescription = "Swipe right for drawer",
            tint = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.78f * nippleAlpha),
            modifier = Modifier.align(Alignment.CenterStart).padding(start = 7.dp).width(13.dp).height(13.dp)
        )
        Icon(
            imageVector = Icons.Default.KeyboardArrowUp,
            contentDescription = "Swipe up for menu",
            tint = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.78f * nippleAlpha),
            modifier = Modifier.align(Alignment.CenterEnd).padding(end = 7.dp).width(13.dp).height(13.dp)
        )
    }
}

@Composable
fun FloatingGrowingMenuSheet(
    sheetState: RevealBottomSheetState,
    maxAnchor: Float,
    menuItems: List<FeatureItem>,
    highlightedMenuItem: String?,
    enableDragToClose: Boolean,
    onMenuItemLayout: (key: String, rect: Rect) -> Unit,
    onDismiss: () -> Unit,
    onItemActivate: suspend (FeatureItem) -> Unit,
    modifier: Modifier = Modifier
) {
    if (!sheetState.isVisible) return

    val scope = rememberCoroutineScope()
    val density = LocalDensity.current
    var sheetHeightPx by remember { mutableIntStateOf(1) }
    val maxMenuHeight = remember { 560.dp }
    val bottomInset = with(density) { (FloatingBarDefaults.barHeight.toPx() + 8.dp.toPx()).toDp() }

    val dragModifier =
        if (enableDragToClose) {
            Modifier.draggable(
                state =
                    rememberDraggableState { dragAmount ->
                        val total = sheetHeightPx.coerceAtLeast(1).toFloat()
                        val boundedProgress = sheetState.progress.coerceIn(0f, maxAnchor)
                        val currentVisible = total * boundedProgress
                        val nextVisible = (currentVisible - dragAmount).coerceIn(0f, total)
                        val nextProgress = (nextVisible / total).coerceIn(0f, maxAnchor)
                        sheetState.setProgressImmediate(nextProgress)
                    },
                orientation = Orientation.Vertical,
                onDragStopped = { velocityY ->
                    sheetState.settle(velocityY)
                    if (sheetState.progress <= 0f) {
                        onDismiss()
                    }
                }
            )
        } else {
            Modifier
        }

    Surface(
        modifier =
            modifier
                .fillMaxSize()
                .padding(horizontal = FloatingBarDefaults.horizontalPadding)
                .padding(bottom = FloatingBarDefaults.verticalPadding)
                .onSizeChanged { sheetHeightPx = it.height.coerceAtLeast(1) }
                .drawWithReveal(
                    sheetHeightPx = sheetHeightPx,
                    progress = sheetState.progress.coerceIn(0f, maxAnchor)
                )
                .then(dragModifier),
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
            modifier = Modifier.fillMaxWidth().heightIn(max = maxMenuHeight).padding(horizontal = 16.dp),
            verticalArrangement = Arrangement.Bottom
        ) {
            Spacer(Modifier.weight(1f, fill = true))
            Column(
                modifier =
                    Modifier
                        .fillMaxWidth()
                        .verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                menuItems.forEach { item ->
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
                        modifier =
                            Modifier
                                .fillMaxWidth()
                                .onGloballyPositioned {
                                    onMenuItemLayout(item.key, it.boundsInWindow())
                                }
                    )
                }
                Spacer(Modifier.height(bottomInset))
            }
        }
    }
}

private fun Modifier.drawWithReveal(sheetHeightPx: Int, progress: Float): Modifier =
    this.then(
        Modifier
            .heightIn(max = 560.dp)
            .fillMaxWidth()
            .drawWithContent {
                val total = sheetHeightPx.coerceAtLeast(1).toFloat()
                val visible = (total * progress).coerceIn(0f, total)
                val revealTop = size.height - visible
                clipRect(top = revealTop.coerceAtLeast(0f)) {
                    this@drawWithContent.drawContent()
                }
            }
    )
