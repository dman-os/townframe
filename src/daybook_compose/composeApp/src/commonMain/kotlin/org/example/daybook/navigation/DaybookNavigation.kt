package org.example.daybook.navigation

import androidx.compose.runtime.Composable
import androidx.compose.runtime.Stable
import androidx.compose.runtime.remember
import androidx.navigation3.runtime.NavBackStack
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.savedstate.serialization.SavedStateConfiguration
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic

@Serializable
sealed interface DaybookNavKey : NavKey {
    @Serializable
    data object Home : DaybookNavKey

    @Serializable
    data object Capture : DaybookNavKey

    @Serializable
    data object Tables : DaybookNavKey

    @Serializable
    data object Progress : DaybookNavKey

    @Serializable
    data object Settings : DaybookNavKey

    @Serializable
    data object Drawer : DaybookNavKey

    @Serializable
    data object DocEditor : DaybookNavKey
}

private val daybookNavConfig =
    SavedStateConfiguration {
        serializersModule =
            SerializersModule {
                polymorphic(NavKey::class) {
                    subclass(DaybookNavKey.Home::class, DaybookNavKey.Home.serializer())
                    subclass(DaybookNavKey.Capture::class, DaybookNavKey.Capture.serializer())
                    subclass(DaybookNavKey.Tables::class, DaybookNavKey.Tables.serializer())
                    subclass(DaybookNavKey.Progress::class, DaybookNavKey.Progress.serializer())
                    subclass(DaybookNavKey.Settings::class, DaybookNavKey.Settings.serializer())
                    subclass(DaybookNavKey.Drawer::class, DaybookNavKey.Drawer.serializer())
                    subclass(DaybookNavKey.DocEditor::class, DaybookNavKey.DocEditor.serializer())
                }
            }
    }

@Stable
class DaybookNavigationState(val backStack: NavBackStack<NavKey>) {
    val currentDestination: DaybookNavKey?
        get() = backStack.lastOrNull() as? DaybookNavKey

    fun navigate(destination: DaybookNavKey) {
        backStack.add(destination)
    }

    fun pop(): Boolean = backStack.removeLastOrNull() != null
}

@Composable
fun rememberDaybookNavigationState(): DaybookNavigationState {
    val backStack = rememberNavBackStack(daybookNavConfig, DaybookNavKey.Home)
    return remember(backStack) {
        DaybookNavigationState(backStack)
    }
}
