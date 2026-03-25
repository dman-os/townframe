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

class BigDialogState {
    var isVisible: Boolean by mutableStateOf(false)

    fun show() {
        isVisible = true
    }

    fun dismiss() {
        isVisible = false
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun BigDialogHost(
    state: BigDialogState,
    narrowScreen: Boolean,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit,
) {
    val isVisible = state.isVisible
    var shouldRender by remember { mutableStateOf(false) }

    LaunchedEffect(isVisible) {
        if (isVisible) {
            shouldRender = true
        }
    }
    LaunchedEffect(narrowScreen, isVisible) {
        if (narrowScreen && !isVisible) {
            shouldRender = false
        }
    }
    if (!shouldRender) return

    if (narrowScreen) {
        if (!isVisible) {
            return
        }
        ModalBottomSheet(
            onDismissRequest = { state.dismiss() },
            modifier = modifier,
        ) {
            content()
        }
    } else {
        val visibilityState = remember { MutableTransitionState(false) }
        LaunchedEffect(isVisible) {
            visibilityState.targetState = isVisible
        }
        LaunchedEffect(visibilityState.isIdle, visibilityState.currentState, isVisible) {
            if (!isVisible && visibilityState.isIdle && !visibilityState.currentState) {
                shouldRender = false
            }
        }
        Box(
            modifier = modifier.fillMaxSize()
        ) {
            Box(
                modifier =
                    Modifier
                        .fillMaxSize()
                        .background(MaterialTheme.colorScheme.scrim.copy(alpha = 0.32f))
                        .clickable(onClick = { state.dismiss() })
            )
            AnimatedVisibility(
                visibleState = visibilityState,
                enter = slideInHorizontally { it } + fadeIn(),
                exit = slideOutHorizontally { it } + fadeOut(),
                modifier = Modifier.align(Alignment.CenterEnd)
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
                    content()
                }
            }
        }
    }
}
