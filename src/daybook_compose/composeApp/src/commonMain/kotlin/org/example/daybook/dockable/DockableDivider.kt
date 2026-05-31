package org.example.daybook.dockable

import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.Layout
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.dp
import kotlin.math.roundToInt

@Composable
fun DockableDivider(
    orientation: Orientation,
    onDragStart: () -> Unit = {},
    onDrag: (Float) -> Unit,
    onDragEnd: () -> Unit,
    onDragCancel: () -> Unit,
    modifier: Modifier = Modifier,
    pillInvisibleWhenNotHovered: Boolean = true,
    centerContentVisibleOnlyOnHover: Boolean = false,
    centerContent: (@Composable () -> Unit)? = null
) {
    val density = LocalDensity.current
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    var isDragging by remember { mutableStateOf(false) }
    val isActive = isHovered || isDragging
    val startDrag = {
        isDragging = true
        onDragStart()
    }
    val endDrag = {
        isDragging = false
        onDragEnd()
    }
    val cancelDrag = {
        isDragging = false
        onDragCancel()
    }
    val glowColor =
        if (isActive) {
            MaterialTheme.colorScheme.primary.copy(alpha = 0.18f)
        } else {
            MaterialTheme.colorScheme.outline.copy(alpha = 0f)
        }
    val lineColor =
        if (isActive) {
            MaterialTheme.colorScheme.primary.copy(alpha = 0.70f)
        } else {
            if (pillInvisibleWhenNotHovered) {
                MaterialTheme.colorScheme.outline.copy(alpha = 0f)
            } else {
                MaterialTheme.colorScheme.outline.copy(alpha = 0.28f)
            }
        }
    val outerModifier =
        modifier
            .then(
                when (orientation) {
                    Orientation.Horizontal -> Modifier.width(4.dp).fillMaxHeight()
                    Orientation.Vertical -> Modifier.height(4.dp).fillMaxWidth()
                }
            )
            .pointerHoverIcon(PointerIcon.Hand)
            .background(glowColor)
            .hoverable(interactionSource)
            .pointerInput(Unit) {
                when (orientation) {
                    Orientation.Horizontal -> {
                        detectHorizontalDragGestures(
                            onDragStart = { startDrag() },
                            onHorizontalDrag = { change, dragAmount ->
                                change.consume()
                                onDrag(dragAmount)
                            },
                            onDragEnd = { endDrag() },
                            onDragCancel = { cancelDrag() }
                        )
                    }

                    Orientation.Vertical -> {
                        detectVerticalDragGestures(
                            onDragStart = { startDrag() },
                            onVerticalDrag = { change, dragAmount ->
                                change.consume()
                                onDrag(dragAmount)
                            },
                            onDragEnd = { endDrag() },
                            onDragCancel = { cancelDrag() }
                        )
                    }
                }
            }

    Layout(
        modifier = outerModifier,
        content = {
            Box(
                modifier =
                    when (orientation) {
                        Orientation.Horizontal ->
                            Modifier
                                .width(8.dp)
                                .height(18.dp)
                                .pointerHoverIcon(PointerIcon.Hand)
                                .hoverable(interactionSource)
                                .pointerInput(Unit) {
                                    detectHorizontalDragGestures(
                                        onDragStart = {
                                            startDrag()
                                        },
                                        onHorizontalDrag = { change, dragAmount ->
                                            change.consume()
                                            onDrag(dragAmount)
                                        },
                                        onDragEnd = { endDrag() },
                                        onDragCancel = { cancelDrag() }
                                    )
                                }

                        Orientation.Vertical ->
                            Modifier
                                .width(18.dp)
                                .height(8.dp)
                                .pointerHoverIcon(PointerIcon.Hand)
                                .hoverable(interactionSource)
                                .pointerInput(Unit) {
                                    detectVerticalDragGestures(
                                        onDragStart = {
                                            startDrag()
                                        },
                                        onVerticalDrag = { change, dragAmount ->
                                            change.consume()
                                            onDrag(dragAmount)
                                        },
                                        onDragEnd = { endDrag() },
                                        onDragCancel = { cancelDrag() }
                                    )
                                }
                    }.background(
                        color = lineColor,
                        shape = RoundedCornerShape(4.dp)
                    )
            )
            val showCenterContent =
                centerContent != null &&
                    (
                        !centerContentVisibleOnlyOnHover ||
                            (isHovered && !isDragging)
                        )
            if (showCenterContent) {
                Box(
                    modifier = Modifier.hoverable(interactionSource)
                ) {
                    centerContent()
                }
            }
        }
    ) { measurables, constraints ->
        val fallbackLengthPx = with(density) { 18.dp.roundToPx() }
        val lineThicknessPx = with(density) { 8.dp.roundToPx() }
        val layoutWidth =
            if (constraints.maxWidth != androidx.compose.ui.unit.Constraints.Infinity) {
                constraints.maxWidth
            } else {
                lineThicknessPx
            }
        val layoutHeight =
            if (constraints.maxHeight != androidx.compose.ui.unit.Constraints.Infinity) {
                constraints.maxHeight
            } else {
                fallbackLengthPx
            }
        val lineLengthPx =
            when (orientation) {
                Orientation.Horizontal -> maxOf(
                    fallbackLengthPx,
                    (layoutHeight * 0.5f).roundToInt()
                )

                Orientation.Vertical -> maxOf(fallbackLengthPx, (layoutWidth * 0.5f).roundToInt())
            }

        val linePlaceable =
            measurables.first().measure(
                when (orientation) {
                    Orientation.Horizontal -> androidx.compose.ui.unit.Constraints.fixed(
                        lineThicknessPx,
                        lineLengthPx
                    )

                    Orientation.Vertical -> androidx.compose.ui.unit.Constraints.fixed(
                        lineLengthPx,
                        lineThicknessPx
                    )
                }
            )
        val centerPlaceable = measurables.getOrNull(
            1
        )?.measure(androidx.compose.ui.unit.Constraints())

        layout(layoutWidth, layoutHeight) {
            linePlaceable.place(
                (layoutWidth - linePlaceable.width) / 2,
                (layoutHeight - linePlaceable.height) / 2
            )
            centerPlaceable?.place(
                (layoutWidth - centerPlaceable.width) / 2,
                (layoutHeight - centerPlaceable.height) / 2
            )
        }
    }
}
