@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook

import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * State object that drives the Scaffold's chrome (AppBar, etc.)
 */
@Immutable
data class ChromeState(
    val title: String? = null,
    val navigationIcon: (@Composable () -> Unit)? = null,
    val actions: (@Composable () -> Unit)? = null,
    val showTopBar: Boolean = true
) {
    companion object {
        val Empty = ChromeState(showTopBar = false)
    }
}

/**
 * Stack-based manager for ChromeState, allowing screens to push/pop their chrome configuration
 */
class ChromeStateStack {
    private val stack = mutableListOf<ChromeState>()
    private val _topState = MutableStateFlow<ChromeState>(ChromeState.Empty)
    
    /**
     * Reactive StateFlow for the top ChromeState
     */
    val topState: StateFlow<ChromeState> = _topState.asStateFlow()
    
    /**
     * Push a ChromeState onto the stack
     */
    fun push(state: ChromeState) {
        stack.add(state)
        println("${stack} pushing XXX")
        _topState.value = state
    }
    
    /**
     * Pop the top ChromeState from the stack
     */
    fun pop() {
        println("popping XXX")
        if (stack.isNotEmpty()) {
            stack.removeAt(stack.size - 1)
            _topState.value = stack.lastOrNull() ?: ChromeState.Empty
        }
    }
    
    /**
     * Get the top ChromeState from the stack, or Empty if stack is empty
     */
    fun top(): ChromeState {
        return stack.lastOrNull() ?: ChromeState.Empty
    }
    
    /**
     * Check if stack is empty
     */
    fun isEmpty(): Boolean = stack.isEmpty()
}

/**
 * CompositionLocal for ChromeStateStack, allowing screens to push/pop their chrome configuration
 */
val LocalChromeStateStack = compositionLocalOf<ChromeStateStack> { 
    error("no ChromeStateStack provided")
}

/**
 * Helper composable for screens to push their ChromeState when composing and pop when removed
 */
@Composable
fun ProvideChromeState(
    state: ChromeState,
    content: @Composable () -> Unit
) {
    val stack = LocalChromeStateStack.current
    
    // Push state when composing
    LaunchedEffect(state) {
        stack.push(state)
    }
    
    // Pop state when removed
    DisposableEffect(Unit) {
        onDispose {
            stack.pop()
        }
    }
    
    content()
}

/**
 * Composable that builds a TopAppBar from ChromeState
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ChromeStateTopAppBar(chromeState: ChromeState) {
    if (!chromeState.showTopBar) return
    
    TopAppBar(
        title = {
            if (chromeState.title != null) {
                androidx.compose.material3.Text(chromeState.title)
            }
        },
        navigationIcon = chromeState.navigationIcon ?: {},
        actions = {
            chromeState.actions?.invoke() ?: Unit
        }
    )
}
