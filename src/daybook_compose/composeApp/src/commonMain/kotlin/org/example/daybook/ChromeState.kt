@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
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
 * Variants for main feature action button in ChromeState
 */
@Immutable
sealed interface MainFeatureActionButton {
    /**
     * A button variant with icon, label (both as Composables), enabled state, and onClick handler
     */
    data class Button(
        val icon: @Composable () -> Unit,
        val label: @Composable () -> Unit,
        val enabled: Boolean = true,
        val onClick: suspend () -> Unit
    ) : MainFeatureActionButton
}

/**
 * Additional feature buttons that can be provided by chrome state.
 * These can be prominent (shown in center nav bar/sidebar) or non-prominent (shown in menus).
 */
@Immutable
data class AdditionalFeatureButton(
    val key: String,
    val icon: @Composable () -> Unit,
    val label: @Composable () -> Unit,
    val prominent: Boolean = false,
    val enabled: Boolean = true,
    val onClick: suspend () -> Unit
)

/**
 * State object that drives the Scaffold's chrome (AppBar, etc.)
 */
@Immutable
data class ChromeState(
    val title: String? = null,
    val navigationIcon: (@Composable () -> Unit)? = null,
    val onBack: (() -> Unit)? = null,
    val actions: (@Composable () -> Unit)? = null,
    val showTopBar: Boolean = true,
    val mainFeatureActionButton: MainFeatureActionButton? = null,
    val additionalFeatureButtons: List<AdditionalFeatureButton> = emptyList()
) {
    companion object {
        val Empty = ChromeState(showTopBar = false)
    }
}

/**
 * Direct manager for ChromeState - routes directly set their chrome state
 */
class ChromeStateManager {
    private val _currentState = MutableStateFlow<ChromeState>(ChromeState.Empty)
    
    /**
     * Reactive StateFlow for the current ChromeState
     */
    val currentState: StateFlow<ChromeState> = _currentState.asStateFlow()
    
    /**
     * Set the current ChromeState directly
     */
    fun setState(state: ChromeState) {
        _currentState.value = state
    }
}

/**
 * CompositionLocal for ChromeStateManager, allowing screens to set their chrome configuration
 */
val LocalChromeStateManager = compositionLocalOf<ChromeStateManager> { 
    error("no ChromeStateManager provided")
}

/**
 * Helper composable for screens to set their ChromeState
 * Routes should always call this, even if they don't want customization (use ChromeState.Empty)
 * No cleanup needed - the next route will replace this state
 */
@Composable
fun ProvideChromeState(
    state: ChromeState,
    content: @Composable () -> Unit
) {
    val manager = LocalChromeStateManager.current
    
    // Set state immediately when composing and update when state changes
    // Use SideEffect to ensure this happens synchronously during composition
    androidx.compose.runtime.SideEffect {
        manager.setState(state)
    }
    
    // Also update in LaunchedEffect to catch any missed updates
    LaunchedEffect(state) {
        manager.setState(state)
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
        navigationIcon = {
            if (chromeState.navigationIcon != null) {
                chromeState.navigationIcon.invoke()
            } else if (chromeState.onBack != null) {
                IconButton(onClick = chromeState.onBack) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            }
        },
        actions = {
            chromeState.actions?.invoke() ?: Unit
        }
    )
}
