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
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
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

internal interface BigDialogController {
    val isShowing: Boolean

    fun show(content: @Composable () -> Unit)

    fun dismiss()
}

internal class BigDialogHostState : BigDialogController {
    internal var dialogContent by mutableStateOf<(@Composable () -> Unit)?>(null)

    override val isShowing: Boolean
        get() = dialogContent != null

    override fun show(content: @Composable () -> Unit) {
        dialogContent = content
    }

    override fun dismiss() {
        dialogContent = null
    }
}

internal val LocalBigDialogController =
    staticCompositionLocalOf<BigDialogController> {
        error("BigDialogHost not provided")
    }

@Composable
internal fun BigDialogHost(narrowScreen: Boolean, modifier: Modifier = Modifier, content: @Composable () -> Unit) {
    val hostState = remember { BigDialogHostState() }
    CompositionLocalProvider(LocalBigDialogController provides hostState) {
        Box(modifier = modifier.fillMaxSize()) {
            content()
            hostState.dialogContent?.let { dialogContent ->
                BigDialogFrame(
                    narrowScreen = narrowScreen,
                    onDismissRequest = hostState::dismiss,
                ) {
                    dialogContent()
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun BigDialogFrame(narrowScreen: Boolean, onDismissRequest: () -> Unit, content: @Composable () -> Unit) {
    if (narrowScreen) {
        ModalBottomSheet(
            onDismissRequest = onDismissRequest,
        ) {
            content()
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
                        .clickable(onClick = onDismissRequest),
                )
                AnimatedVisibility(
                    visibleState = visibilityState,
                    enter = slideInHorizontally { it } + fadeIn(),
                    exit = slideOutHorizontally { it } + fadeOut(),
                    modifier = Modifier.align(Alignment.CenterEnd),
                ) {
                    Surface(
                        tonalElevation = 2.dp,
                        shadowElevation = 8.dp,
                        modifier =
                        Modifier
                            .fillMaxHeight()
                            .widthIn(min = 420.dp, max = 560.dp),
                    ) {
                        content()
                    }
                }
            }
        }
    }
}

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
        BigDialogFrame(
            narrowScreen = narrowScreen,
            onDismissRequest = onBack,
        ) {
            entry.Content()
        }
    }
}
