package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.MutableTransitionState
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.RectangleShape
import androidx.compose.ui.unit.dp
import androidx.navigation3.runtime.NavEntry
import androidx.navigation3.runtime.NavMetadataKey
import androidx.navigation3.runtime.contains
import androidx.navigation3.runtime.metadata
import androidx.navigation3.scene.OverlayScene
import androidx.navigation3.scene.Scene
import androidx.navigation3.scene.SceneStrategy
import androidx.navigation3.scene.SceneStrategyScope

fun bigDialog(): Map<String, Any> = BigDialogSceneStrategy.bigDialog()

@OptIn(ExperimentalMaterial3Api::class)
class BigDialogSceneStrategy<T : Any>(private val narrowScreen: Boolean) : SceneStrategy<T> {
    override fun SceneStrategyScope<T>.calculateScene(entries: List<NavEntry<T>>): Scene<T>? {
        val lastEntry = entries.lastOrNull()
        val hasBigDialog = lastEntry?.metadata?.contains(BigDialogMetadataKey) == true
        if (!hasBigDialog) return null

        @Suppress("UNCHECKED_CAST")
        return BigDialogScene(
            key = lastEntry.contentKey as T,
            previousEntries = entries.dropLast(1),
            overlaidEntries = entries.dropLast(1),
            entry = lastEntry,
            narrowScreen = narrowScreen,
            onBack = onBack,
        )
    }

    companion object {
        object BigDialogMetadataKey : NavMetadataKey<Boolean>

        fun bigDialog(): Map<String, Any> = metadata {
            put(BigDialogMetadataKey, true)
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
private class BigDialogScene<T : Any>(
    override val key: T,
    override val previousEntries: List<NavEntry<T>>,
    override val overlaidEntries: List<NavEntry<T>>,
    private val entry: NavEntry<T>,
    private val narrowScreen: Boolean,
    private val onBack: () -> Unit,
) : OverlayScene<T> {
    override val entries: List<NavEntry<T>> = listOf(entry)

    override val content: @Composable () -> Unit = {
        if (narrowScreen) {
            ModalBottomSheet(
                onDismissRequest = onBack,
            ) {
                entry.Content()
            }
        } else {
            val visibilityState = remember { MutableTransitionState(false) }
            var shouldRender by remember { mutableStateOf(true) }

            LaunchedEffect(Unit) {
                visibilityState.targetState = true
            }
            LaunchedEffect(visibilityState.isIdle, visibilityState.currentState) {
                if (visibilityState.isIdle && !visibilityState.currentState) {
                    shouldRender = false
                }
            }

            if (shouldRender) {
                Box(
                    modifier = Modifier.fillMaxSize(),
                ) {
                    Box(
                        modifier =
                        Modifier
                            .fillMaxSize()
                            .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
                            .clickable(onClick = onBack),
                    )
                    AnimatedVisibility(
                        visibleState = visibilityState,
                        enter = slideInHorizontally { it } + fadeIn(),
                        exit = slideOutHorizontally { it } + fadeOut(),
                        modifier = Modifier.align(Alignment.CenterEnd),
                    ) {
                        Surface(
                            shape = RectangleShape,
                            tonalElevation = 2.dp,
                            shadowElevation = 8.dp,
                            modifier =
                            Modifier
                                .fillMaxHeight()
                                .widthIn(min = 420.dp, max = 560.dp),
                        ) {
                            entry.Content()
                        }
                    }
                }
            }
        }
    }
}
