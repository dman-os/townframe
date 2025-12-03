package org.example.daybook.dockable

// import androidx.compose.foundation.background
// import androidx.compose.foundation.gestures.detectHorizontalDragGestures
// import androidx.compose.foundation.hoverable
// import androidx.compose.foundation.interaction.MutableInteractionSource
// import androidx.compose.foundation.interaction.collectIsHoveredAsState
// import androidx.compose.foundation.layout.Box
// import androidx.compose.foundation.layout.BoxWithConstraints
// import androidx.compose.foundation.layout.Column
// import androidx.compose.foundation.layout.Row
// import androidx.compose.foundation.layout.fillMaxHeight
// import androidx.compose.foundation.layout.fillMaxSize
// import androidx.compose.foundation.layout.fillMaxWidth
// import androidx.compose.material3.MaterialTheme
// import androidx.compose.runtime.Composable
// import androidx.compose.runtime.getValue
// import androidx.compose.runtime.mutableStateOf
// import androidx.compose.runtime.remember
// import androidx.compose.runtime.setValue
// import androidx.compose.ui.Modifier
// import androidx.compose.ui.input.pointer.pointerInput
// import androidx.compose.ui.platform.LocalDensity
// import androidx.compose.ui.unit.dp
// import androidx.compose.ui.zIndex
//
// @Composable
// private fun VerticalSplitter(
//     onDelta: (Float) -> Unit,
//     modifier: Modifier = Modifier
// ) {
//     val interactionSource = remember { MutableInteractionSource() }
//     val isHovered by interactionSource.collectIsHoveredAsState()
//
//     Box(
//         modifier
//             .width(8.dp)
//             .fillMaxHeight()
//             .background(
//                 if (isHovered) {
//                     MaterialTheme.colorScheme.primary.copy(alpha = 0.5f)
//                 } else {
//                     MaterialTheme.colorScheme.outline.copy(alpha = 0.2f)
//                 }
//             )
//             .hoverable(interactionSource)
//             .pointerInput(Unit) {
//                 detectHorizontalDragGestures { change, dragAmount ->
//                     change.consume()
//                     onDelta(dragAmount)
//                 }
//             }
//             .zIndex(1f)
//     )
// }
//
//
// @Composable
// fun HorizontalSplitPane(
//     modifier: Modifier = Modifier,
//     left: @Composable (modifier: Modifier) -> Unit,
//     right: @Composable (modifier: Modifier) -> Unit,
//     initialSplit: Float = 0.5f,
//     minSplit: Float = 0.1f,
//     maxSplit: Float = 0.9f
// ) {
//     var split by remember { mutableStateOf(initialSplit) }
//     // Get density as a Float once outside BoxWithConstraintsScope
//     val density = LocalDensity.current.density 
//
//     BoxWithConstraints(modifier = modifier.fillMaxSize()) {
//         val totalWidthPx = constraints.maxWidth.value * density // Dp to Px conversion
//
//         Row(modifier = Modifier.fillMaxSize()) {
//             left(
//                 Modifier
//                     .weight(split)
//                     .fillMaxHeight()
//             )
//
//             VerticalSplitter(onDelta = { delta ->
//                 if (totalWidthPx > 0) {
//                     split = (split + delta / totalWidthPx).coerceIn(minSplit, maxSplit)
//                 }
//             })
//
//             right(
//                 Modifier
//                     .weight(1f - split)
//                     .fillMaxHeight()
//             )
//         }
//     }
// }
//
// @Composable
// private fun HorizontalSplitter(
//     onDelta: (Float) -> Unit,
//     modifier: Modifier = Modifier
// ) {
//     val interactionSource = remember { MutableInteractionSource() }
//     val isHovered by interactionSource.collectIsHoveredAsState()
//
//     Box(
//         modifier
//             .fillMaxWidth()
//             .background(
//                 if (isHovered) {
//                     MaterialTheme.colorScheme.primary.copy(alpha = 0.5f)
//                 } else {
//                     MaterialTheme.colorScheme.outline.copy(alpha = 0.2f)
//                 }
//             )
//             .hoverable(interactionSource)
//             .pointerInput(Unit) {
//                 detectHorizontalDragGestures { change, dragAmount ->
//                     change.consume()
//                     onDelta(dragAmount)
//                 }
//             }
//             .zIndex(1f)
//     )
// }
//
// @Composable
// fun VerticalSplitPane(
//     modifier: Modifier = Modifier,
//     top: @Composable (modifier: Modifier) -> Unit,
//     bottom: @Composable (modifier: Modifier) -> Unit,
//     initialSplit: Float = 0.5f,
//     minSplit: Float = 0.1f,
//     maxSplit: Float = 0.9f
// ) {
//     var split by remember { mutableStateOf(initialSplit) }
//     // Get density as a Float once outside BoxWithConstraintsScope
//     val density = LocalDensity.current.density 
//
//     BoxWithConstraints(modifier = modifier.fillMaxSize()) {
//         val totalHeightPx = constraints.maxHeight.value * density // Dp to Px conversion
//
//         Column(modifier = Modifier.fillMaxSize()) {
//             top(
//                 Modifier
//                     .weight(split)
//                     .fillMaxWidth()
//             )
//
//             HorizontalSplitter(onDelta = { delta ->
//                 if (totalHeightPx > 0) {
//                     split = (split + delta / totalHeightPx).coerceIn(minSplit, maxSplit)
//                 }
//             })
//
//             bottom(
//                 Modifier
//                     .weight(1f - split)
//                     .fillMaxWidth()
//             )
//         }
//     }
// }
