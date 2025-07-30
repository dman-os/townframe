package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.NavHostController
import androidx.navigation.compose.rememberNavController
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

import daybook.composeapp.generated.resources.Res
import daybook.composeapp.generated.resources.compose_multiplatform
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig

data class PermissionsContext(
    val hasCamera: Boolean = false,
    val hasNotifications: Boolean = false,
    val hasMicrophone: Boolean = false,
    val hasOverlay: Boolean = false,
    val requestAllPermissions: () -> Unit = {},
) {
    val hasAll = hasCamera and
            hasNotifications and
            hasMicrophone and
            hasOverlay;
}


val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

data class AppConfig(
    val theme: ThemeConfig = ThemeConfig.Dark,
)

enum class AppScreens {
    Home
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
@Preview
fun App(
    config: AppConfig = AppConfig(),
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController = rememberNavController()
) {
    DaybookTheme(themeConfig = config.theme) {
        Scaffold(
            modifier = surfaceModifier,
            topBar = {
                TopAppBar(
                    title = { Text("Daybook") }
                )
            },
            bottomBar = {
                NavigationBar {
                    NavigationBarItem(
                        selected = true,
                        onClick = {},
                        icon = {
                            Text("H")
                        },
                        label = {
                            Text("Home")
                        }
                    )
                }
            }
        ) { innerPadding ->
            NavHost(
                startDestination = AppScreens.Home.name,
                navController = navController,
                modifier = Modifier
                    .fillMaxSize()
                    .verticalScroll(rememberScrollState())
                    .padding(innerPadding)
            ) {
                composable(route = AppScreens.Home.name) {
                    var showContent by remember { mutableStateOf(false) }
                    Column(
                        modifier = Modifier
                            .safeContentPadding()
                            .fillMaxSize(),
                        horizontalAlignment = Alignment.CenterHorizontally,
                    ) {
                        Button(onClick = {
                            showContent = !showContent
                            // uniffi.daybook_core.uniffiEnsureInitialized()
                            uniffi.daybook_core.init()
                            extraAction?.invoke()
                        }) {
                            Text("Click me!")
                        }
                        run {
                            val permCtx = LocalPermCtx.current
                            if (permCtx != null) {
                                if (permCtx.hasAll) {
                                    Text("All permissions avail")
                                } else {
                                    Button(onClick = {
                                        permCtx.requestAllPermissions()
                                    }) {
                                        Text("Ask for permissions")
                                    }
                                }
                            }
                        }
                        AnimatedVisibility(showContent) {
                            val greeting = remember { Greeting().greet() }
                            Column(
                                Modifier.fillMaxWidth(),
                                horizontalAlignment = Alignment.CenterHorizontally
                            ) {
                                Image(painterResource(Res.drawable.compose_multiplatform), null)
                                Text("Compose: $greeting")
                            }
                        }
                    }
                }
            }
        }
    }
}

