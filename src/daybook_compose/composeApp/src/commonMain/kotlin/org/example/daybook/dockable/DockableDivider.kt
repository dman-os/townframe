package org.example.daybook.dockable

import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.hoverable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsHoveredAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.unit.dp

@Composable
fun DockableDivider(
    orientation: Orientation,
    onDragStart: () -> Unit = {},
    onDrag: (Float) -> Unit,
    onDragEnd: () -> Unit,
    onDragCancel: () -> Unit,
    modifier: Modifier = Modifier,
    centerContent: (@Composable () -> Unit)? = null
) {
    val interactionSource = remember { MutableInteractionSource() }
    val isHovered by interactionSource.collectIsHoveredAsState()
    val glowColor =
        if (isHovered) {
            MaterialTheme.colorScheme.primary.copy(alpha = 0.18f)
        } else {
            MaterialTheme.colorScheme.outline.copy(alpha = 0.06f)
        }
    val lineColor =
        if (isHovered) {
            MaterialTheme.colorScheme.primary.copy(alpha = 0.70f)
        } else {
            MaterialTheme.colorScheme.outline.copy(alpha = 0.28f)
        }
    val outerModifier =
        modifier
            .then(
                when (orientation) {
                    Orientation.Horizontal -> Modifier.width(8.dp).fillMaxHeight()
                    Orientation.Vertical -> Modifier.height(8.dp).fillMaxWidth()
                }
            )
            .pointerHoverIcon(PointerIcon.Hand)
            .background(glowColor)
            .hoverable(interactionSource)
            .pointerInput(Unit) {
                when (orientation) {
                    Orientation.Horizontal -> {
                        detectHorizontalDragGestures(
                            onDragStart = { onDragStart() },
                            onHorizontalDrag = { change, dragAmount ->
                                change.consume()
                                onDrag(dragAmount)
                            },
                            onDragEnd = onDragEnd,
                            onDragCancel = onDragCancel
                        )
                    }

                    Orientation.Vertical -> {
                        detectVerticalDragGestures(
                            onDragStart = { onDragStart() },
                            onVerticalDrag = { change, dragAmount ->
                                change.consume()
                                onDrag(dragAmount)
                            },
                            onDragEnd = onDragEnd,
                            onDragCancel = onDragCancel
                        )
                    }
                }
            }

    BoxWithConstraints(
        modifier = outerModifier,
        contentAlignment = Alignment.Center
    ) {
        val lineLength =
            when (orientation) {
                Orientation.Horizontal -> maxOf(18.dp, maxHeight * 0.25f)
                Orientation.Vertical -> maxOf(18.dp, maxWidth * 0.25f)
            }
        val lineThickness = 8.dp

        Box(
            modifier =
                when (orientation) {
                    Orientation.Horizontal ->
                        Modifier.width(lineThickness).height(lineLength)

                    Orientation.Vertical ->
                        Modifier.width(lineLength).height(lineThickness)
                }.background(
                    color = lineColor,
                    shape = RoundedCornerShape(4.dp)
                )
        )
        if (centerContent != null) {
            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                centerContent()
            }
        }
    }
}
