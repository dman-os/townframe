@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)

// FIXME: remove usage of Result

package org.example.daybook

import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ArrowBack
import androidx.compose.material.icons.filled.CreateNewFolder
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.FolderOpen
import androidx.compose.material.icons.filled.QrCodeScanner
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ElevatedCard
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LargeTopAppBar
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavBackStack
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.navigation3.runtime.rememberSaveableStateHolderNavEntryDecorator
import androidx.navigation3.ui.NavDisplay
import androidx.savedstate.serialization.SavedStateConfiguration
import io.github.vinceglb.filekit.dialogs.compose.rememberDirectoryPickerLauncher
import io.github.vinceglb.filekit.path
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.example.daybook.capture.data.CameraOverlay
import org.example.daybook.capture.data.CameraPreviewQrBridge
import org.example.daybook.capture.data.CameraQrOverlayBridge
import org.example.daybook.capture.ui.DaybookCameraViewport
import org.example.daybook.progress.ProgressAmountBlock
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.CameraQrAnalyzerFfi
import org.example.daybook.uniffi.CloneInfo
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.core.KnownRepoEntry
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets

sealed interface CloneUiState {
    data class UrlInput(val urlInput: String = "", val isResolving: Boolean = false, val errorMessage: String? = null) :
        CloneUiState

    data class Scanner(val currentUrlInput: String, val errorMessage: String? = null) : CloneUiState

    data class PickingLocation(
        val sourceUrl: String,
        val info: CloneInfo,
        val destinationPath: String,
        val isCloning: Boolean = false,
        val errorMessage: String? = null,
        val destinationWarning: String? = null,
    ) : CloneUiState

    data class Syncing(
        val sourceUrl: String,
        val initialSyncComplete: Boolean = false,
        val phaseMessage: String = "Opening cloned repo…",
        val errorMessage: String? = null,
    ) : CloneUiState
}

sealed interface CreateRepoUiState {
    data class Editing(
        val repoName: String = "",
        val parentPath: String = "",
        val isCreating: Boolean = false,
        val errorMessage: String? = null,
        val destinationWarning: String? = null,
    ) : CreateRepoUiState
}

@Serializable
sealed interface WelcomeNavKey : NavKey {
    @Serializable
    data object Menu : WelcomeNavKey

    @Serializable
    data object RepoDetail : WelcomeNavKey

    @Serializable
    data object CreateRepo : WelcomeNavKey

    @Serializable
    data object CloneUrl : WelcomeNavKey

    @Serializable
    data object CloneScanner : WelcomeNavKey

    @Serializable
    data object CloneLocation : WelcomeNavKey
}

private val welcomeNavConfig =
    SavedStateConfiguration {
        serializersModule =
            SerializersModule {
                polymorphic(NavKey::class) {
                    subclass(WelcomeNavKey.Menu::class, WelcomeNavKey.Menu.serializer())
                    subclass(WelcomeNavKey.RepoDetail::class, WelcomeNavKey.RepoDetail.serializer())
                    subclass(WelcomeNavKey.CreateRepo::class, WelcomeNavKey.CreateRepo.serializer())
                    subclass(WelcomeNavKey.CloneUrl::class, WelcomeNavKey.CloneUrl.serializer())
                    subclass(WelcomeNavKey.CloneScanner::class, WelcomeNavKey.CloneScanner.serializer())
                    subclass(WelcomeNavKey.CloneLocation::class, WelcomeNavKey.CloneLocation.serializer())
                }
            }
    }

private class WelcomeNavigationState(val backStack: NavBackStack<NavKey>) {
    val currentDestination: WelcomeNavKey?
        get() = backStack.lastOrNull() as? WelcomeNavKey

    fun navigate(destination: WelcomeNavKey) {
        if (currentDestination == destination) return
        backStack.add(destination)
    }

    fun pop(): Boolean = backStack.removeLastOrNull() != null
}

@Composable
private fun rememberWelcomeNavigationState(): WelcomeNavigationState {
    val backStack = rememberNavBackStack(welcomeNavConfig, WelcomeNavKey.Menu)
    return remember(backStack) {
        WelcomeNavigationState(backStack)
    }
}

private suspend fun fetchDefaultParentDir(): Result<String> = try {
    val defaultParent = withAppFfiCtx { gcx -> gcx.defaultCloneParentDir().trim() }
    Result.success(defaultParent)
} catch (error: Throwable) {
    if (error is CancellationException) throw error
    Result.failure(error)
}

@Composable
fun WelcomeFlowNavHost(
    repos: List<KnownRepoEntry>,
    permCtx: PermissionsContext?,
    cameraPreviewFfi: CameraPreviewFfi?,
    selectedWelcomeRepo: KnownRepoEntry?,
    cloneUiState: CloneUiState?,
    createRepoUiState: CreateRepoUiState?,
    cloneSourceUrlPendingOpen: String?,
    cloneInitRequest: Pair<String, String>?,
    createRepoInitRequest: String?,
    pendingForgetRepoId: String?,
    onSelectedWelcomeRepoChange: (KnownRepoEntry?) -> Unit,
    onCloneUiStateChange: (CloneUiState?) -> Unit,
    onCreateRepoUiStateChange: (CreateRepoUiState?) -> Unit,
    onCloneSourceUrlPendingOpenChange: (String?) -> Unit,
    onCloneInitRequestChange: (Pair<String, String>?) -> Unit,
    onCreateRepoInitRequestChange: (String?) -> Unit,
    onPendingOpenRepoPath: (String) -> Unit,
    onPendingForgetRepoId: (String) -> Unit,
) {
    val navState = rememberWelcomeNavigationState()
    val currentDestination = navState.currentDestination
    var pendingScannerOpen by remember { mutableStateOf(false) }
    val isAndroidPlatform = getPlatform().name.startsWith("Android")
    val exitRequest = LocalAppExitRequest.current

    val onNavBack: () -> Unit =
        if (navState.backStack.size > 1) {
            { navState.pop() }
        } else {
            { exitRequest?.invoke() }
        }

    val (title, subtitle) =
        when (currentDestination) {
            WelcomeNavKey.RepoDetail -> "Repository Details" to "Review before opening"

            WelcomeNavKey.CreateRepo ->
                "Create Repository" to
                    if (isAndroidPlatform) "App-private storage" else "Choose name and location"

            WelcomeNavKey.CloneUrl -> "Clone Repo" to "Enter a URL or scan a code"

            WelcomeNavKey.CloneScanner -> "Scan Clone URL" to "Point camera at a QR code"

            WelcomeNavKey.CloneLocation ->
                "Clone Destination" to
                    if (isAndroidPlatform) "App-private storage" else "Choose destination"

            else -> "Welcome to Daybook" to "Select a repository to continue"
        }

    val onBack: (() -> Unit)? =
        when (currentDestination) {
            WelcomeNavKey.RepoDetail -> {
                {
                    onSelectedWelcomeRepoChange(null)
                    navState.pop()
                }
            }

            WelcomeNavKey.CreateRepo -> {
                {
                    onCreateRepoUiStateChange(null)
                    navState.pop()
                }
            }

            WelcomeNavKey.CloneUrl -> {
                {
                    onCloneUiStateChange(null)
                    navState.pop()
                }
            }

            WelcomeNavKey.CloneScanner -> {
                {
                    val scannerState = cloneUiState as? CloneUiState.Scanner
                    onCloneUiStateChange(
                        scannerState?.let { CloneUiState.UrlInput(urlInput = it.currentUrlInput) }
                            ?: CloneUiState.UrlInput(),
                    )
                    navState.pop()
                }
            }

            WelcomeNavKey.CloneLocation -> {
                {
                    val locationState = cloneUiState as? CloneUiState.PickingLocation
                    onCloneUiStateChange(
                        locationState?.let { CloneUiState.UrlInput(urlInput = it.sourceUrl) }
                            ?: CloneUiState.UrlInput(),
                    )
                    navState.pop()
                }
            }

            else -> null
        }

    WelcomeFlowScaffold(
        title = title,
        subtitle = subtitle,
        onBack = onBack,
    ) {
        Box(
            modifier =
            Modifier
                .fillMaxSize()
                .background(MaterialTheme.colorScheme.surface),
        ) {
            NavDisplay(
                backStack = navState.backStack,
                onBack = onNavBack,
                entryDecorators = listOf(
                    rememberSaveableStateHolderNavEntryDecorator(),
                    rememberViewModelStoreNavEntryDecorator(),
                ),
                entryProvider = entryProvider {
                    entry<WelcomeNavKey.Menu> {
                        WelcomeScreen(
                            repos = repos,
                            onOpenRepo = onPendingOpenRepoPath,
                            onInspectRepo = { repo ->
                                onSelectedWelcomeRepoChange(repo)
                                navState.navigate(WelcomeNavKey.RepoDetail)
                            },
                            onStartCreateRepo = {
                                onCreateRepoUiStateChange(
                                    CreateRepoUiState.Editing(
                                        repoName = "daybook-repo",
                                        parentPath = "",
                                        isCreating = false,
                                    ),
                                )
                                navState.navigate(WelcomeNavKey.CreateRepo)
                            },
                            onStartClone = {
                                onCloneUiStateChange(CloneUiState.UrlInput())
                                navState.navigate(WelcomeNavKey.CloneUrl)
                            },
                        )
                    }

                    entry<WelcomeNavKey.RepoDetail> {
                        val repo = selectedWelcomeRepo
                        if (repo == null) {
                            LaunchedEffect(Unit) { navState.pop() }
                        } else {
                            WelcomeRepoDetailScreen(
                                repo = repo,
                                onOpen = { onPendingOpenRepoPath(repo.path) },
                                onForget = { onPendingForgetRepoId(repo.id) },
                                forgetting = pendingForgetRepoId == repo.id,
                            )
                        }
                    }

                    entry<WelcomeNavKey.CreateRepo> {
                        val editState = createRepoUiState as? CreateRepoUiState.Editing

                        fun updateCreateState(transform: (CreateRepoUiState.Editing) -> CreateRepoUiState.Editing) {
                            val current = createRepoUiState as? CreateRepoUiState.Editing ?: return
                            onCreateRepoUiStateChange(transform(current))
                        }

                        if (editState == null) {
                            Surface(
                                modifier = Modifier.fillMaxSize(),
                                color = MaterialTheme.colorScheme.background,
                            ) {
                                Box(
                                    modifier = Modifier.fillMaxSize(),
                                    contentAlignment = Alignment.Center,
                                ) {
                                    CircularProgressIndicator()
                                }
                            }
                            LaunchedEffect(Unit) {
                                val repoName = "daybook-repo"
                                val defaultParentResult = fetchDefaultParentDir()
                                if (defaultParentResult.isSuccess) {
                                    val defaultParent = defaultParentResult.getOrThrow()
                                    onCreateRepoUiStateChange(
                                        CreateRepoUiState.Editing(
                                            repoName = repoName,
                                            parentPath = defaultParent,
                                            isCreating = false,
                                        ),
                                    )
                                } else {
                                    val error = defaultParentResult.exceptionOrNull() ?: error("unknown failure")
                                    onCreateRepoUiStateChange(
                                        CreateRepoUiState.Editing(
                                            repoName = repoName,
                                            parentPath = "",
                                            isCreating = false,
                                            errorMessage = "Failed loading default parent: ${describeThrowable(error)}",
                                        ),
                                    )
                                }
                            }
                            return@entry
                        }

                        CreateRepoScreen(
                            state = editState,
                            onRepoNameChange = { next ->
                                updateCreateState { current ->
                                    current.copy(
                                        repoName = next,
                                        errorMessage = null,
                                        destinationWarning = null,
                                    )
                                }
                            },
                            onParentPathChange = { next ->
                                updateCreateState { current ->
                                    current.copy(
                                        parentPath = next,
                                        errorMessage = null,
                                        destinationWarning = null,
                                    )
                                }
                            },
                            onContinue = {
                                val current =
                                    createRepoUiState as? CreateRepoUiState.Editing ?: return@CreateRepoScreen
                                val destination = joinPath(current.parentPath, current.repoName)
                                onCreateRepoUiStateChange(
                                    current.copy(
                                        isCreating = true,
                                        errorMessage = null,
                                        destinationWarning = null,
                                    ),
                                )
                                onCreateRepoInitRequestChange(destination)
                            },
                        )

                        if (editState.parentPath.isBlank() && !editState.isCreating) {
                            LaunchedEffect(editState.parentPath, editState.isCreating) {
                                val defaultParentResult = fetchDefaultParentDir()
                                if (defaultParentResult.isSuccess) {
                                    val defaultParent = defaultParentResult.getOrThrow()
                                    val latest =
                                        createRepoUiState as? CreateRepoUiState.Editing ?: return@LaunchedEffect
                                    if (latest.parentPath.isBlank()) {
                                        onCreateRepoUiStateChange(
                                            latest.copy(
                                                parentPath = defaultParent,
                                                errorMessage = null,
                                            ),
                                        )
                                    }
                                } else {
                                    val error = defaultParentResult.exceptionOrNull() ?: error("unknown failure")
                                    updateCreateState { current ->
                                        current.copy(
                                            errorMessage = "Failed loading default parent: ${describeThrowable(error)}",
                                        )
                                    }
                                }
                            }
                        }

                        LaunchedEffect(editState.parentPath, editState.repoName) {
                            val current = createRepoUiState as? CreateRepoUiState.Editing ?: return@LaunchedEffect
                            val destination = joinPath(current.parentPath, current.repoName)
                            if (destination.isBlank() || current.repoName.isBlank()) {
                                onCreateRepoUiStateChange(current.copy(destinationWarning = null))
                                return@LaunchedEffect
                            }
                            if (current.repoName.contains("/") || current.repoName.contains("\\")) {
                                onCreateRepoUiStateChange(
                                    current.copy(
                                        destinationWarning = "Repository name cannot contain path separators.",
                                    ),
                                )
                                return@LaunchedEffect
                            }
                            try {
                                val check = withAppFfiCtx { gcx ->
                                    gcx.checkCloneDestination(destination)
                                }
                                val warning =
                                    when {
                                        !check.exists -> null
                                        !check.isDir -> "Destination exists and is not a directory."
                                        !check.isEmpty -> "Destination directory is not empty."
                                        else -> null
                                    }
                                updateCreateState { latest -> latest.copy(destinationWarning = warning) }
                            } catch (error: Throwable) {
                                if (error is CancellationException) throw error
                                updateCreateState { latest ->
                                    latest.copy(
                                        destinationWarning = "Destination check failed: ${describeThrowable(error)}",
                                    )
                                }
                            }
                        }

                        if (editState.isCreating && createRepoInitRequest != null) {
                            LaunchedEffect(createRepoInitRequest) {
                                val request = createRepoInitRequest ?: return@LaunchedEffect
                                try {
                                    val resolvedDestination = withAppFfiCtx { gcx ->
                                        resolveNonClashingDestination(
                                            gcx = gcx,
                                            requestedPath = request,
                                            autoRename = isAndroidPlatform,
                                        )
                                    }
                                    val preflight = withAppFfiCtx { gcx ->
                                        gcx.checkCloneDestination(resolvedDestination.path)
                                    }
                                    if (preflight.exists && preflight.isDir && !preflight.isEmpty) {
                                        updateCreateState { current ->
                                            current.copy(
                                                isCreating = false,
                                                errorMessage = "Destination directory is not empty. Choose an empty directory.",
                                                destinationWarning = "Destination directory is not empty.",
                                            )
                                        }
                                        return@LaunchedEffect
                                    }
                                    if (resolvedDestination.note != null) {
                                        updateCreateState { current ->
                                            current.copy(
                                                parentPath = parentPathOf(resolvedDestination.path),
                                                repoName = leafNameOf(resolvedDestination.path),
                                                destinationWarning = null,
                                                errorMessage = resolvedDestination.note,
                                                isCreating = false,
                                            )
                                        }
                                    }
                                    onPendingOpenRepoPath(resolvedDestination.path)
                                    onCreateRepoUiStateChange(null)
                                } catch (error: Throwable) {
                                    if (error is CancellationException) throw error
                                    updateCreateState { current ->
                                        current.copy(
                                            isCreating = false,
                                            errorMessage = "Create initialization failed: ${describeThrowable(error)}",
                                        )
                                    }
                                } finally {
                                    onCreateRepoInitRequestChange(null)
                                }
                            }
                        }
                    }

                    entry<WelcomeNavKey.CloneUrl> {
                        val urlState =
                            when (val state = cloneUiState) {
                                is CloneUiState.UrlInput -> state
                                is CloneUiState.Scanner -> CloneUiState.UrlInput(urlInput = state.currentUrlInput)
                                is CloneUiState.PickingLocation -> CloneUiState.UrlInput(urlInput = state.sourceUrl)
                                is CloneUiState.Syncing -> CloneUiState.UrlInput(urlInput = state.sourceUrl)
                                null -> CloneUiState.UrlInput()
                            }
                        LaunchedEffect(permCtx?.hasCamera, pendingScannerOpen, urlState.urlInput) {
                            if (!pendingScannerOpen) return@LaunchedEffect
                            val hasCamera = permCtx?.hasCamera ?: false
                            if (!hasCamera) return@LaunchedEffect
                            pendingScannerOpen = false
                            onCloneUiStateChange(CloneUiState.Scanner(currentUrlInput = urlState.urlInput))
                            navState.navigate(WelcomeNavKey.CloneScanner)
                        }
                        CloneUrlScreen(
                            state = urlState,
                            onUrlChange = { next ->
                                onCloneUiStateChange(urlState.copy(urlInput = next, errorMessage = null))
                            },
                            onOpenScanner = {
                                if (permCtx != null && !permCtx.hasCamera) {
                                    pendingScannerOpen = true
                                    permCtx.requestPermissions(PermissionRequest(camera = true))
                                    return@CloneUrlScreen
                                }
                                onCloneUiStateChange(CloneUiState.Scanner(currentUrlInput = urlState.urlInput))
                                navState.navigate(WelcomeNavKey.CloneScanner)
                            },
                            onContinue = { sourceUrl ->
                                onCloneUiStateChange(urlState.copy(isResolving = true, errorMessage = null))
                                onCloneSourceUrlPendingOpenChange(sourceUrl)
                            },
                        )
                        if (urlState.isResolving && cloneSourceUrlPendingOpen != null) {
                            LaunchedEffect(cloneSourceUrlPendingOpen) {
                                val sourceUrl = cloneSourceUrlPendingOpen ?: return@LaunchedEffect
                                try {
                                    val info = withAppFfiCtx { gcx ->
                                        gcx.resolveCloneUrl(sourceUrl)
                                    }
                                    val defaultParent = withAppFfiCtx { gcx ->
                                        gcx.defaultCloneParentDir().trim()
                                    }
                                    if (defaultParent.isBlank()) {
                                        error("empty clone parent directory from FFI")
                                    }
                                    val initialRepoName = info.repoName.ifBlank { "daybook-repo" }
                                    onCloneUiStateChange(
                                        CloneUiState.PickingLocation(
                                            sourceUrl = sourceUrl,
                                            info = info,
                                            destinationPath = joinPath(defaultParent, initialRepoName),
                                        ),
                                    )
                                    navState.navigate(WelcomeNavKey.CloneLocation)
                                } catch (error: Throwable) {
                                    if (error is CancellationException) throw error
                                    onCloneUiStateChange(
                                        urlState.copy(
                                            isResolving = false,
                                            errorMessage = "Resolve failed: ${describeThrowable(error)}",
                                        ),
                                    )
                                } finally {
                                    onCloneSourceUrlPendingOpenChange(null)
                                }
                            }
                        }
                    }

                    entry<WelcomeNavKey.CloneScanner> {
                        val scannerState = cloneUiState as? CloneUiState.Scanner
                        if (scannerState == null) {
                            LaunchedEffect(Unit) { navState.pop() }
                        } else {
                            if (cameraPreviewFfi == null) {
                                Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                    Column(
                                        horizontalAlignment = Alignment.CenterHorizontally,
                                        verticalArrangement = Arrangement.spacedBy(12.dp),
                                    ) {
                                        CircularProgressIndicator()
                                        Text("Initializing camera…", style = MaterialTheme.typography.bodyMedium)
                                    }
                                }
                            } else {
                                CloneQrScannerScreen(
                                    cameraPreviewFfi = cameraPreviewFfi,
                                    onDetectedUrl = { detectedUrl ->
                                        onCloneUiStateChange(CloneUiState.UrlInput(urlInput = detectedUrl))
                                        while (navState.currentDestination != WelcomeNavKey.CloneUrl &&
                                            navState.pop()
                                        ) {
                                            // Pop back to the clone URL entry.
                                        }
                                    },
                                )
                            }
                        }
                    }

                    entry<WelcomeNavKey.CloneLocation> {
                        val locationState = cloneUiState as? CloneUiState.PickingLocation
                        if (locationState == null) {
                            LaunchedEffect(Unit) { navState.pop() }
                        } else {
                            fun updateLocationState(
                                transform: (CloneUiState.PickingLocation) -> CloneUiState.PickingLocation,
                            ) {
                                val current = cloneUiState as? CloneUiState.PickingLocation ?: return
                                onCloneUiStateChange(transform(current))
                            }
                            CloneLocationScreen(
                                state = locationState,
                                onDestinationChange = { destinationPath ->
                                    updateLocationState { current ->
                                        current.copy(
                                            destinationPath = destinationPath,
                                            errorMessage = null,
                                            destinationWarning = null,
                                        )
                                    }
                                },
                                onContinue = { destinationPath ->
                                    val current =
                                        cloneUiState as? CloneUiState.PickingLocation
                                            ?: return@CloneLocationScreen
                                    onCloneUiStateChange(
                                        current.copy(
                                            destinationPath = destinationPath,
                                            isCloning = true,
                                            errorMessage = null,
                                        ),
                                    )
                                    onCloneInitRequestChange(current.sourceUrl to destinationPath)
                                },
                            )
                            LaunchedEffect(locationState.destinationPath) {
                                val current = cloneUiState as? CloneUiState.PickingLocation ?: return@LaunchedEffect
                                val destination = current.destinationPath.trim()
                                if (destination.isBlank()) {
                                    onCloneUiStateChange(current.copy(destinationWarning = null))
                                    return@LaunchedEffect
                                }
                                try {
                                    val check = withAppFfiCtx { gcx ->
                                        gcx.checkCloneDestination(destination)
                                    }
                                    val warning =
                                        when {
                                            !check.exists -> null
                                            !check.isDir -> "Destination exists and is not a directory."
                                            !check.isEmpty -> "Destination directory is not empty."
                                            else -> null
                                        }
                                    updateLocationState { latest -> latest.copy(destinationWarning = warning) }
                                } catch (error: Throwable) {
                                    if (error is CancellationException) throw error
                                    updateLocationState { latest ->
                                        latest.copy(
                                            destinationWarning = "Destination check failed: ${describeThrowable(
                                                error,
                                            )}",
                                        )
                                    }
                                }
                            }
                            if (locationState.isCloning && cloneInitRequest != null) {
                                LaunchedEffect(cloneInitRequest) {
                                    val request = cloneInitRequest ?: return@LaunchedEffect
                                    try {
                                        val resolvedDestination = withAppFfiCtx { gcx ->
                                            resolveNonClashingDestination(
                                                gcx = gcx,
                                                requestedPath = request.second,
                                                autoRename = isAndroidPlatform,
                                            )
                                        }
                                        val preflight = withAppFfiCtx { gcx ->
                                            gcx.checkCloneDestination(resolvedDestination.path)
                                        }
                                        if (preflight.exists && preflight.isDir && !preflight.isEmpty) {
                                            updateLocationState { latest ->
                                                latest.copy(
                                                    isCloning = false,
                                                    errorMessage = "Destination directory is not empty. Choose an empty directory.",
                                                    destinationWarning = "Destination directory is not empty.",
                                                )
                                            }
                                            return@LaunchedEffect
                                        }
                                        val out = withAppFfiCtx { gcx ->
                                            gcx.cloneRepoInitFromUrl(request.first, resolvedDestination.path)
                                        }
                                        onCloneUiStateChange(
                                            CloneUiState.Syncing(
                                                sourceUrl = request.first,
                                                initialSyncComplete = false,
                                                phaseMessage =
                                                resolvedDestination.note?.let {
                                                    "Opening cloned repo… $it"
                                                } ?: "Opening cloned repo…",
                                                errorMessage = null,
                                            ),
                                        )
                                        onCloneSourceUrlPendingOpenChange(request.first)
                                        onPendingOpenRepoPath(out.repoPath)
                                    } catch (error: Throwable) {
                                        if (error is CancellationException) throw error
                                        updateLocationState { latest ->
                                            latest.copy(
                                                isCloning = false,
                                                errorMessage = "Clone initialization failed: ${describeThrowable(
                                                    error,
                                                )}",
                                            )
                                        }
                                    } finally {
                                        onCloneInitRequestChange(null)
                                    }
                                }
                            }
                        }
                    }
                },
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun WelcomeFlowScaffold(
    title: String,
    subtitle: String? = null,
    onBack: (() -> Unit)? = null,
    content: @Composable () -> Unit,
) {
    Scaffold(
        topBar = {
            LargeTopAppBar(
                title = {
                    Column {
                        Text(title)
                        if (!subtitle.isNullOrBlank()) {
                            Text(
                                subtitle,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f),
                            )
                        }
                    }
                },
                navigationIcon = {
                    if (onBack != null) {
                        IconButton(onClick = onBack) {
                            Icon(Icons.Default.ArrowBack, contentDescription = "Back")
                        }
                    }
                },
            )
        },
    ) { innerPadding ->
        Box(
            modifier =
            Modifier
                .fillMaxSize()
                .background(MaterialTheme.colorScheme.surface)
                .padding(innerPadding),
        ) {
            content()
        }
    }
}

@Composable
private fun WelcomeScreen(
    repos: List<KnownRepoEntry>,
    onOpenRepo: (String) -> Unit,
    onInspectRepo: (KnownRepoEntry) -> Unit,
    onStartCreateRepo: () -> Unit,
    onStartClone: () -> Unit,
) {
    val isAndroidPlatform = getPlatform().name.startsWith("Android")
    val openRepoLauncher = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        onOpenRepo(selectedPath)
    }

    val isDesktop = getPlatform().getScreenWidthDp().value >= 1000f
    Box (
        modifier = Modifier.fillMaxSize().padding(24.dp),
        contentAlignment = Alignment.Center,
    ) {
        Column(
            modifier = Modifier.fillMaxHeight(),
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            TextButton(onClick = onStartCreateRepo) {
                Icon(Icons.Default.CreateNewFolder, contentDescription = null)
                Spacer(Modifier.width(8.dp))
                Text("create new repo")
            }
            if (!isAndroidPlatform) {
                TextButton(onClick = { openRepoLauncher.launch() }) {
                    Icon(Icons.Default.FolderOpen, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("open directory")
                }
            }
            TextButton(onClick = onStartClone) {
                Icon(Icons.Default.Description, contentDescription = null)
                Spacer(Modifier.width(8.dp))
                Text("clone repo")
            }


            if (repos.isEmpty()) {
                // Text(
                //     text = "No known repositories yet.",
                //     style = MaterialTheme.typography.bodyMedium,
                //     color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.6f),
                // )
            } else {
                Spacer(Modifier.width(16.dp))
                HorizontalDivider()
                Spacer(Modifier.width(15.dp))
                LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    items(repos, key = { repo -> repo.id }) { repo ->
                        Surface(
                            modifier = Modifier.fillMaxWidth().clickable { onInspectRepo(repo) },
                            shape = MaterialTheme.shapes.medium,
                            tonalElevation = 2.dp,
                        ) {
                            Column(modifier = Modifier.fillMaxWidth().padding(12.dp)) {
                                Text(
                                    text = if (repo.name.isNotBlank()) repo.name else repo.path,
                                    style = MaterialTheme.typography.bodyLarge,
                                )
                                Text(
                                    text = repo.path,
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f),
                                )
                                Text(
                                    text = "Last opened: ${repo.lastOpenedAtUnixSecs}",
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f),
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun WelcomeRepoDetailScreen(
    repo: KnownRepoEntry,
    onOpen: () -> Unit,
    onForget: () -> Unit,
    forgetting: Boolean,
) {
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Column(
            modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            ElevatedCard(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    Text(
                        text = if (repo.name.isBlank()) repo.path else repo.name,
                        style = MaterialTheme.typography.headlineSmall,
                    )
                    Text(
                        text = repo.path,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f),
                    )
                    Text(
                        text = "Created: ${repo.createdAtUnixSecs}",
                        style = MaterialTheme.typography.bodySmall,
                    )
                    Text(
                        text = "Last opened: ${repo.lastOpenedAtUnixSecs}",
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
            Column(
                modifier = Modifier.fillMaxWidth(),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                Button(
                    onClick = onOpen,
                    modifier = Modifier.fillMaxWidth(),
                    enabled = !forgetting,
                ) {
                    Text("Open Repo")
                }
                OutlinedButton(
                    onClick = onForget,
                    modifier = Modifier.fillMaxWidth(),
                    enabled = !forgetting,
                ) {
                    Text(if (forgetting) "Forgetting..." else "Forget Repo")
                }
            }
        }
    }
}

@Composable
private fun CloneUrlScreen(
    state: CloneUiState.UrlInput,
    onUrlChange: (String) -> Unit,
    onOpenScanner: () -> Unit,
    onContinue: (String) -> Unit,
) {
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Column(
            modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            ElevatedCard {
                Column(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    OutlinedTextField(
                        value = state.urlInput,
                        onValueChange = onUrlChange,
                        label = { Text("Clone URL") },
                        modifier = Modifier.fillMaxWidth(),
                        enabled = !state.isResolving,
                    )
                    HorizontalDivider()
                    Button(
                        onClick = onOpenScanner,
                        enabled = !state.isResolving,
                        modifier = Modifier.fillMaxWidth(),
                    ) {
                        Icon(Icons.Default.QrCodeScanner, contentDescription = null)
                        Spacer(Modifier.width(8.dp))
                        Text("Scan QR Code")
                    }
                    if (state.isResolving) {
                        Row(
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp))
                            Text("Resolving clone URL…", style = MaterialTheme.typography.bodySmall)
                        }
                    }
                    if (!state.errorMessage.isNullOrBlank()) {
                        Text(
                            state.errorMessage,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                }
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                Button(
                    onClick = { onContinue(state.urlInput.trim()) },
                    enabled = state.urlInput.trim().isNotBlank() && !state.isResolving,
                ) {
                    Text("Continue")
                }
            }
        }
    }
}

@Composable
private fun CloneQrScannerScreen(cameraPreviewFfi: CameraPreviewFfi, onDetectedUrl: (String) -> Unit) {
    fun looksLikeUrl(candidate: String): Boolean = candidate.matches(Regex("^[A-Za-z][A-Za-z0-9+.-]*:.*$"))

    val useNativePreviewQr = remember(cameraPreviewFfi) { cameraPreviewFfi.supportsNativeQrAnalysis() }
    var analyzer by remember { mutableStateOf<CameraQrAnalyzerFfi?>(null) }
    LaunchedEffect(Unit) {
        if (analyzer == null && !useNativePreviewQr) {
            analyzer = withContext(Dispatchers.IO) { CameraQrAnalyzerFfi.load() }
        }
    }
    val analyzerReady = analyzer
    if (!useNativePreviewQr && analyzerReady == null) {
        Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                CircularProgressIndicator()
                Text("Initializing QR analyzer…", style = MaterialTheme.typography.bodyMedium)
            }
        }
        return
    }
    val uiScope = rememberCoroutineScope()
    var userVisibleError by remember { mutableStateOf<String?>(null) }
    var hasCompleted by remember { mutableStateOf(false) }
    val frameBridge =
        remember(analyzerReady) {
            analyzerReady?.let { readyAnalyzer ->
                CameraQrOverlayBridge(
                    analyzer = readyAnalyzer,
                    onDetectedText = { rawText ->
                        uiScope.launch {
                            if (hasCompleted) return@launch
                            val candidate = rawText.trim()
                            if (!looksLikeUrl(candidate)) {
                                userVisibleError = "Detected QR is not a URL."
                                return@launch
                            }
                            hasCompleted = true
                            onDetectedUrl(candidate)
                        }
                    },
                )
            }
        }
    val previewBridge =
        remember(cameraPreviewFfi) {
            CameraPreviewQrBridge(
                cameraPreviewFfi = cameraPreviewFfi,
                onDetectedText = { rawText ->
                    uiScope.launch {
                        if (hasCompleted) return@launch
                        val candidate = rawText.trim()
                        if (!looksLikeUrl(candidate)) {
                            userVisibleError = "Detected QR is not a URL."
                            return@launch
                        }
                        hasCompleted = true
                        onDetectedUrl(candidate)
                    }
                },
            )
        }
    val overlayState by
        (
            if (useNativePreviewQr) {
                previewBridge.state
            } else {
                frameBridge?.state ?: previewBridge.state
            }
            ).collectAsState()

    androidx.compose.runtime.DisposableEffect(analyzerReady, frameBridge, previewBridge, useNativePreviewQr) {
        if (useNativePreviewQr) {
            previewBridge.start()
        } else {
            frameBridge?.start()
        }
        onDispose {
            previewBridge.stop()
            frameBridge?.stop()
            analyzerReady?.close()
        }
    }

    Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Column(
            modifier = Modifier.fillMaxSize(),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            ElevatedCard(modifier = Modifier.fillMaxWidth().weight(1f)) {
                DaybookCameraViewport(
                    cameraPreviewFfi = cameraPreviewFfi,
                    modifier = Modifier.fillMaxSize(),
                    overlays =
                    if (overlayState.overlays.isEmpty()) {
                        listOf(CameraOverlay.Grid)
                    } else {
                        overlayState.overlays
                    },
                    onFrameAvailable =
                    if (useNativePreviewQr) {
                        null
                    } else {
                        frameBridge?.let { bridge -> { sample -> bridge.submitFrame(sample) } }
                    },
                )
            }
            val errorText = userVisibleError ?: overlayState.latestError
            if (!errorText.isNullOrBlank()) {
                Text(
                    text = errorText,
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodySmall,
                )
            } else {
                Text(
                    text = "Scanning… detected URLs auto-fill the clone form.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.72f),
                )
            }
        }
    }
}

@Composable
private fun CloneLocationScreen(
    state: CloneUiState.PickingLocation,
    onDestinationChange: (String) -> Unit,
    onContinue: (String) -> Unit,
) {
    val isAndroidPlatform = getPlatform().name.startsWith("Android")
    val hasRecoverableCollision =
        isAndroidPlatform && state.destinationWarning == "Destination directory is not empty."
    val picker = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        onDestinationChange(selectedPath)
    }
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Column(
            modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            ElevatedCard {
                Column(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    Text("Repo: ${state.info.repoName}")
                    state.info.deviceName?.let { deviceName ->
                        Text("Device: $deviceName")
                    }
                    if (isAndroidPlatform) {
                        Text(
                            text = "Clone path: ${state.destinationPath}",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f),
                        )
                    } else {
                        OutlinedTextField(
                            value = state.destinationPath,
                            onValueChange = onDestinationChange,
                            label = { Text("Clone Path") },
                            enabled = !state.isCloning,
                            modifier = Modifier.fillMaxWidth(),
                        )
                        Button(
                            onClick = { picker.launch() },
                            enabled = !state.isCloning,
                        ) {
                            Text("Browse")
                        }
                    }
                    if (state.isCloning) {
                        Row(
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp))
                            Text("Initializing clone…", style = MaterialTheme.typography.bodySmall)
                        }
                    }
                    if (!state.errorMessage.isNullOrBlank()) {
                        Text(
                            state.errorMessage,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                    if (!state.destinationWarning.isNullOrBlank()) {
                        Text(
                            state.destinationWarning,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                }
            }
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                Button(
                    onClick = { onContinue(state.destinationPath.trim()) },
                    enabled =
                    state.destinationPath.trim().isNotBlank() &&
                        !state.isCloning &&
                        (state.destinationWarning.isNullOrBlank() || hasRecoverableCollision),
                ) {
                    Text("Continue")
                }
            }
        }
    }
}

@Composable
private fun CreateRepoScreen(
    state: CreateRepoUiState.Editing,
    onRepoNameChange: (String) -> Unit,
    onParentPathChange: (String) -> Unit,
    onContinue: () -> Unit,
) {
    val isAndroidPlatform = getPlatform().name.startsWith("Android")
    val hasRecoverableCollision =
        isAndroidPlatform && state.destinationWarning == "Destination directory is not empty."
    val picker = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        onParentPathChange(selectedPath)
    }
    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.6f else 1f
    Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Column(
            modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            ElevatedCard {
                Column(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    OutlinedTextField(
                        value = state.repoName,
                        onValueChange = onRepoNameChange,
                        label = { Text("Repository Name") },
                        enabled = !state.isCreating,
                        modifier = Modifier.fillMaxWidth(),
                    )
                    if (isAndroidPlatform) {
                        Text(
                            text = "Base path: ${state.parentPath}",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f),
                        )
                    } else {
                        OutlinedTextField(
                            value = state.parentPath,
                            onValueChange = onParentPathChange,
                            label = { Text("Parent Directory") },
                            enabled = !state.isCreating,
                            modifier = Modifier.fillMaxWidth(),
                        )
                        Button(
                            onClick = { picker.launch() },
                            enabled = !state.isCreating,
                        ) {
                            Text("Browse")
                        }
                    }
                    Text(
                        text = "Destination: ${joinPath(state.parentPath, state.repoName)}",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.75f),
                    )
                    if (state.isCreating) {
                        Row(
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp))
                            Text("Creating repository…", style = MaterialTheme.typography.bodySmall)
                        }
                    }
                    if (!state.errorMessage.isNullOrBlank()) {
                        Text(
                            state.errorMessage,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                    if (!state.destinationWarning.isNullOrBlank()) {
                        Text(
                            state.destinationWarning,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                }
            }
            Button(
                onClick = onContinue,
                enabled =
                state.repoName.isNotBlank() &&
                    state.parentPath.isNotBlank() &&
                    !state.isCreating &&
                    (state.destinationWarning.isNullOrBlank() || hasRecoverableCollision),
            ) {
                Text("Continue")
            }
        }
    }
}

@Composable
fun CloneShareDialogContent(onClose: () -> Unit) {
    val syncRepo = LocalContainer.current.syncRepo
    var ticketUrl by remember { mutableStateOf<String?>(null) }
    var qrPngBytes by remember { mutableStateOf<ByteArray?>(null) }
    var errorMessage by remember { mutableStateOf<String?>(null) }
    var reloadKey by remember { mutableIntStateOf(0) }
    var copied by remember { mutableStateOf(false) }
    val clipboardManager = LocalClipboardManager.current

    LaunchedEffect(reloadKey) {
        copied = false
        errorMessage = null
        ticketUrl = null
        qrPngBytes = null
        if (syncRepo == null) {
            errorMessage = "Sync service is still starting. Try again in a moment."
            return@LaunchedEffect
        }
        try {
            val ticket = syncRepo.getTicketWithQrPng(768u)
            ticketUrl = ticket.ticketUrl
            qrPngBytes = ticket.qrPngBytes
        } catch (error: Throwable) {
            if (error is CancellationException) throw error
            val details = error.message ?: error.toString()
            errorMessage = "Failed to prepare clone ticket: $details"
        }
    }

    val qrBitmap = remember(qrPngBytes) {
        qrPngBytes?.let { decodePngImageBitmap(it) }
    }

    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .verticalScroll(rememberScrollState())
            .padding(horizontal = 20.dp, vertical = 16.dp),
        verticalArrangement = Arrangement.spacedBy(14.dp),
    ) {
        Text(
            text = "Clone This Repo",
            style = MaterialTheme.typography.headlineSmall,
        )
        Text(
            text = "Share this URL or QR code to clone from this device.",
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )

        HorizontalDivider()

        if (errorMessage != null) {
            Text(
                text = errorMessage ?: "",
                color = MaterialTheme.colorScheme.error,
                style = MaterialTheme.typography.bodyMedium,
            )
            Button(onClick = { reloadKey += 1 }) {
                Text("Retry")
            }
        } else if (ticketUrl == null || qrBitmap == null) {
            Column(
                modifier = Modifier.fillMaxWidth().heightIn(min = 220.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center,
            ) {
                CircularProgressIndicator()
                Text(
                    "Preparing clone details…",
                    modifier = Modifier.padding(top = 12.dp),
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
        } else {
            Surface(
                shape = RoundedCornerShape(16.dp),
                color = MaterialTheme.colorScheme.surfaceContainerLow,
                modifier = Modifier.fillMaxWidth(),
            ) {
                Box(
                    modifier = Modifier.fillMaxWidth().padding(16.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Image(
                        bitmap = qrBitmap,
                        contentDescription = "Clone URL QR code",
                        modifier = Modifier.size(260.dp),
                    )
                }
            }

            Surface(
                shape = RoundedCornerShape(12.dp),
                color = MaterialTheme.colorScheme.surfaceContainerLowest,
                modifier = Modifier.fillMaxWidth(),
            ) {
                Text(
                    text = ticketUrl ?: "",
                    style = MaterialTheme.typography.bodySmall,
                    maxLines = 4,
                    overflow = TextOverflow.Ellipsis,
                    fontFamily = FontFamily.Monospace,
                    modifier = Modifier.fillMaxWidth().padding(12.dp),
                )
            }

            Button(
                onClick = {
                    clipboardManager.setText(AnnotatedString(ticketUrl ?: ""))
                    copied = true
                },
                modifier = Modifier.fillMaxWidth(),
            ) {
                Text("Copy URL")
            }
            if (copied) {
                Text(
                    text = "Copied.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.primary,
                )
                LaunchedEffect(ticketUrl, copied) {
                    delay(1400)
                    copied = false
                }
            }
        }

        OutlinedButton(
            onClick = onClose,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Text("Close")
        }
    }
}

@Composable
fun CloneSyncScreen(
    progressRepo: ProgressRepoFfi?,
    state: CloneUiState.Syncing,
    onSyncInBackground: () -> Unit,
    onRetry: () -> Unit,
) {
    var statusMessage by remember(state.phaseMessage) { mutableStateOf(state.phaseMessage) }
    var syncTasks by remember { mutableStateOf(emptyList<ProgressTask>()) }
    var fullySyncedPeers by remember { mutableStateOf(emptySet<String>()) }

    LaunchedEffect(progressRepo, state.sourceUrl, state.phaseMessage) {
        if (progressRepo == null) {
            statusMessage = state.phaseMessage
            syncTasks = emptyList()
            fullySyncedPeers = emptySet()
            return@LaunchedEffect
        }
        while (true) {
            try {
                syncTasks = progressRepo.listByTagPrefix("/sync/full")
                val active = syncTasks.count { it.state == ProgressTaskState.ACTIVE }
                statusMessage =
                    if (syncTasks.isEmpty()) {
                        state.phaseMessage
                    } else {
                        "${state.phaseMessage} ($active active / ${syncTasks.size} tasks)"
                    }
                fullySyncedPeers =
                    syncTasks
                        .asSequence()
                        .filter { task ->
                            val statusMessageText =
                                (task.latestUpdate?.update?.deets as? ProgressUpdateDeets.Status)?.message
                            statusMessageText?.startsWith("peer fully synced") == true
                        }
                        .map { task -> task.id.removePrefix("sync/full/peer/") }
                        .toSet()
            } catch (error: Throwable) {
                if (error is CancellationException) throw error
                statusMessage = "Unable to read sync progress right now."
            }
            kotlinx.coroutines.delay(1000)
        }
    }

    val widthFraction = if (getPlatform().getScreenWidthDp().value >= 1000f) 0.7f else 1f
    WelcomeFlowScaffold(
        title = "Sync",
        subtitle = "Clone ongoing",
    ) {
        Box(modifier = Modifier.fillMaxSize().padding(24.dp)) {
            Column(
                modifier = Modifier.fillMaxWidth(widthFraction).align(Alignment.TopCenter),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                Text(statusMessage, style = MaterialTheme.typography.bodyMedium)
                if (fullySyncedPeers.isNotEmpty()) {
                    Text(
                        "Fully synced with ${fullySyncedPeers.size} peer(s).",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.primary,
                    )
                }
                if (!state.errorMessage.isNullOrBlank()) {
                    Text(
                        state.errorMessage,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                LazyColumn(
                    modifier = Modifier.fillMaxWidth().weight(1f),
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    items(syncTasks, key = { it.id }) { task ->
                        ElevatedCard(modifier = Modifier.fillMaxWidth()) {
                            Column(
                                modifier = Modifier.fillMaxWidth().padding(12.dp),
                                verticalArrangement = Arrangement.spacedBy(6.dp),
                            ) {
                                Text(
                                    text = task.title ?: task.id,
                                    style = MaterialTheme.typography.titleSmall,
                                )
                                Text(
                                    text = task.latestUpdate?.let { "Update #${it.sequence}" } ?: "No updates yet",
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.7f),
                                )
                                when (val deets = task.latestUpdate?.update?.deets) {
                                    is ProgressUpdateDeets.Amount ->
                                        ProgressAmountBlock(deets, modifier = Modifier.fillMaxWidth())

                                    is ProgressUpdateDeets.Status ->
                                        Text(deets.message, style = MaterialTheme.typography.bodySmall)

                                    is ProgressUpdateDeets.Completed ->
                                        Text(
                                            deets.message ?: deets.state.name.lowercase(),
                                            style = MaterialTheme.typography.bodySmall,
                                        )

                                    null -> {}
                                }
                            }
                        }
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Button(onClick = onSyncInBackground, enabled = state.initialSyncComplete) {
                        Text("Sync in background")
                    }
                    TextButton(onClick = onRetry) {
                        Text("Retry connection")
                    }
                }
            }
        }
    }
}
