package org.example.daybook

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.compositionLocalOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.navigation.NavHostController
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import daybook.composeapp.generated.resources.Res
import daybook.composeapp.generated.resources.compose_multiplatform
import org.example.daybook.capture.screens.CaptureScreen
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.DocsRepo
import org.example.daybook.uniffi.FfiCtx
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.ui.tooling.preview.Preview

val LocalPermCtx = compositionLocalOf<PermissionsContext?> { null }

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
            hasOverlay
}

data class AppContainer(
    val ffiCtx: FfiCtx,
    val docsRepo: DocsRepo
)

val LocalContainer = staticCompositionLocalOf<AppContainer> {
    error("no AppContainer provided")
}

data class AppConfig(
    val theme: ThemeConfig = ThemeConfig.Dark,
)

enum class AppScreens {
    Home,
    Capture
}

private sealed interface AppInitState {
    data object Loading: AppInitState
    data class Ready(val container: AppContainer): AppInitState
    data class Error(val throwable: Throwable): AppInitState
}

@Composable
@Preview
fun App(
    config: AppConfig = AppConfig(),
    surfaceModifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController = rememberNavController(),
) {
    var initAttempt by remember { mutableStateOf(0) }
    var initState by remember { mutableStateOf<AppInitState>(AppInitState.Loading) }

    LaunchedEffect(initAttempt) {
        initState = AppInitState.Loading
        val fcx = FfiCtx.forFfi()
        val repo = DocsRepo.forFfi(fcx = fcx)
        initState = AppInitState.Ready(
            AppContainer(
                ffiCtx = fcx,
                docsRepo = repo
            )
        )
    }

    DaybookTheme(themeConfig = config.theme) {
        when (val state = initState) {
            is AppInitState.Loading -> {
                LoadingScreen()
            }
            is AppInitState.Error -> {
                ErrorScreen(
                    message = state.throwable.message ?: "Unknown error",
                    onRetry = { initAttempt += 1 }
                )
            }
            is AppInitState.Ready -> {
                val appContainer = state.container

                // Ensure FFI resources are closed when the composition leaves
                androidx.compose.runtime.DisposableEffect(appContainer) {
                    onDispose {
                        appContainer.docsRepo.close()
                        appContainer.ffiCtx.close()
                    }
                }

                CompositionLocalProvider(
                    LocalContainer provides appContainer,
                ) {
                    AppScaffold(modifier = surfaceModifier, navController = navController) { innerPadding ->
                        Routes(
                            modifier = Modifier.padding(innerPadding),
                            extraAction = extraAction,
                            navController = navController
                        )
                    }
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun AppScaffold(
    modifier: Modifier = Modifier,
    navController: NavHostController,
    content: @Composable (innerPadding: PaddingValues) -> Unit
) {
    Scaffold(
        modifier = modifier,
        topBar = {
            TopAppBar(
                title = { Text("Daybook") }
            )
        },
        bottomBar = {
            NavigationBar {
                NavigationBarItem(
                    selected = true,
                    onClick = {
                        navController.navigate(AppScreens.Home.name)
                    },
                    icon = {
                        Text("H")
                    },
                    label = {
                        Text("Home")
                    }
                )
                NavigationBarItem(
                    selected = true,
                    onClick = {
                        navController.navigate(AppScreens.Capture.name)
                    },
                    icon = {
                        Text("C")
                    },
                    label = {
                        Text("Capture")
                    }
                )
            }
        }
    ) { innerPadding ->
        content(innerPadding)
    }
}

@Composable
fun Routes(
    modifier: Modifier = Modifier,
    extraAction: (() -> Unit)? = null,
    navController: NavHostController,
) {

    NavHost(
        startDestination = AppScreens.Home.name,
        navController = navController,
        modifier = modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
    ) {
        composable(route = AppScreens.Capture.name) {
            CaptureScreen()
        }
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

@Composable
private fun LoadingScreen() {
    Box(
        modifier = Modifier
            .fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            CircularProgressIndicator()
            Spacer(Modifier.height(16.dp))
            Text("Preparing Daybook…")
        }
    }
}

@Composable
private fun ErrorScreen(
    message: String,
    onRetry: () -> Unit,
) {
    Box(
        modifier = Modifier
            .fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text("Failed to initialize")
            Spacer(Modifier.height(8.dp))
            Text(message)
            Spacer(Modifier.height(16.dp))
            Button(onClick = onRetry) { Text("Retry") }
        }
    }
}
