package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.example.daybook.uniffi.BlobsRepoFfi
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.DispatchRepoFfi
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.InitRepoFfi
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.RtFfi
import org.example.daybook.uniffi.SqliteLocalStateRepoFfi
import org.example.daybook.uniffi.SyncRepoFfi
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.core.CreateProgressTaskArgs
import org.example.daybook.uniffi.core.ProgressFinalState
import org.example.daybook.uniffi.core.ProgressRetentionPolicy
import org.example.daybook.uniffi.core.ProgressSeverity
import org.example.daybook.uniffi.core.ProgressUpdate
import org.example.daybook.uniffi.core.ProgressUpdateDeets
import kotlin.time.Clock
import kotlin.time.TimeMark
import kotlin.time.TimeSource

internal suspend fun warmUpTablesRepo(tablesRepo: TablesRepoFfi) {
    tablesRepo.listWindows()
    tablesRepo.listTabs()
    tablesRepo.listPanels()
    tablesRepo.listTables()
}

internal const val STARTUP_PROGRESS_TASK_ID = "app/init/startup"
private const val OPEN_REPO_STAGE_COUNT = 10

private class StartupProgressTask(
    private val progressRepo: ProgressRepoFfi,
    private val taskId: String,
    private val appElapsedMillis: () -> Long,
    private val totalStages: Int,
) {
    private var doneStages = 0

    suspend fun begin(repoPath: String, startupElapsed: String, phaseId: String) {
        progressRepo.upsertTask(
            CreateProgressTaskArgs(
                id = taskId,
                tags = listOf("/app/init", "/app/init/open-repo"),
                retention = ProgressRetentionPolicy.UserDismissable,
            ),
        )
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                ProgressUpdateDeets.Status(
                    severity = ProgressSeverity.INFO,
                    message =
                    "startup phase begin phase=$phaseId " +
                        "(open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})",
                ),
            ),
        )
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                ProgressUpdateDeets.Status(
                    severity = ProgressSeverity.INFO,
                    message = "opening repo $repoPath",
                ),
            ),
        )
    }

    suspend fun stage(label: String, stageBlock: suspend () -> Unit) {
        stageBlock()
        doneStages += 1
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                ProgressUpdateDeets.Status(
                    severity = ProgressSeverity.INFO,
                    message = "stage $doneStages/$totalStages: $label",
                ),
            ),
        )
    }

    suspend fun complete(message: String) {
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                ProgressUpdateDeets.Completed(
                    state = ProgressFinalState.SUCCEEDED,
                    message = message,
                ),
            ),
        )
    }

    suspend fun fail(message: String, elapsed: String) {
        progressRepo.addUpdate(
            taskId,
            ProgressUpdate(
                at = Clock.System.now(),
                title = "App startup",
                deets =
                ProgressUpdateDeets.Completed(
                    state = ProgressFinalState.FAILED,
                    message = "$message (from_app_start_ms=$elapsed)",
                ),
            ),
        )
    }
}

internal suspend fun shutdownAppContainer(appContainer: AppContainer, ioDispatcher: CoroutineDispatcher) {
    println("[APP_SHUTDOWN] flushing to disk: begin")
    val failures = mutableListOf<Throwable>()

    fun recordFailure(throwable: Throwable) {
        failures += throwable
        println("[APP_SHUTDOWN] cleanup failed err=${throwable.message}")
    }

    suspend fun stopOnIo(label: String, block: suspend () -> Unit) {
        runCatching {
            withContext(ioDispatcher) {
                println("[APP_SHUTDOWN] flushing to disk: stopping $label")
                block()
            }
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            recordFailure(exception)
        }
    }

    fun closeSafely(label: String, block: () -> Unit) {
        runCatching {
            println("[APP_SHUTDOWN] flushing to disk: closing $label")
            block()
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            recordFailure(exception)
        }
    }

    val rtFfi = appContainer.rtFfi
    val syncRepo = appContainer.syncRepo
    if (syncRepo != null) {
        stopOnIo("sync repo") { syncRepo.stop() }
    }
    if (rtFfi != null) {
        stopOnIo("runtime repo") { rtFfi.stop() }
    }
    stopOnIo("init repo") { appContainer.initRepo.stop() }
    stopOnIo("sqlite local state repo") { appContainer.sqliteLsRepo.stop() }
    stopOnIo("progress repo") { appContainer.progressRepo.stop() }
    closeSafely("drawer repo") { appContainer.drawerRepo.close() }
    closeSafely("tables repo") { appContainer.tablesRepo.close() }
    closeSafely("dispatch repo") { appContainer.dispatchRepo.close() }
    closeSafely("progress repo") { appContainer.progressRepo.close() }
    rtFfi?.let { closeSafely("runtime ffi") { it.close() } }
    closeSafely("init repo") { appContainer.initRepo.close() }
    closeSafely("sqlite local state repo") { appContainer.sqliteLsRepo.close() }
    closeSafely("plugs repo") { appContainer.plugsRepo.close() }
    closeSafely("config repo") { appContainer.configRepo.close() }
    closeSafely("blobs repo") { appContainer.blobsRepo.close() }
    syncRepo?.let { closeSafely("sync repo") { it.close() } }
    closeSafely("camera preview ffi") { appContainer.cameraPreviewFfi.close() }
    closeSafely("ffi ctx") { appContainer.ffiCtx.close() }
    if (failures.isNotEmpty()) {
        val first = failures.first()
        failures.drop(1).forEach(first::addSuppressed)
        throw first
    }
    println("[APP_SHUTDOWN] flushing to disk: complete")
}

class AppRuntimeViewModel(
    private val ffiServices: AppFfiServices,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : ViewModel() {
    private val runtimeMutex = Mutex()
    private var runtimeJob: Job? = null
    private var activeContainer: AppContainer? = null

    private val _state = MutableStateFlow<FfiRuntimeState>(FfiRuntimeState.Loading)
    val state: StateFlow<FfiRuntimeState> = _state.asStateFlow()

    fun start(appStartElapsedMs: () -> Long = { 0L }) {
        launchRuntimeJob {
            bootstrap(appStartElapsedMs)
        }
    }

    fun openRepo(repoPath: String, appStartElapsedMs: () -> Long = { 0L }) {
        launchRuntimeJob {
            openRepoInternal(repoPath, appStartElapsedMs)
        }
    }

    fun forgetRepo(repoId: String) {
        viewModelScope.launch {
            runCatching {
                val repoConfig =
                    withContext(ioDispatcher) {
                        ffiServices.forgetKnownRepo(repoId)
                    }
                _state.value = FfiRuntimeState.Welcome(repos = repoConfig.knownRepos)
            }.onFailure { exception ->
                if (exception is CancellationException) {
                    throw exception
                }
                _state.value = FfiRuntimeState.Error(exception)
            }
        }
    }

    suspend fun shutdownReadyContainer() {
        runtimeMutex.withLock {
            closeActiveContainer(updateState = true)
        }
    }

    private fun launchRuntimeJob(block: suspend () -> Unit) {
        val job =
            viewModelScope.launch {
                runtimeMutex.withLock {
                    block()
                }
            }
        val previous = runtimeJob
        runtimeJob = job
        previous?.cancel()
        job.invokeOnCompletion {
            if (runtimeJob === job) {
                runtimeJob = null
            }
        }
    }

    private data class OpenRepoResourceState(
        var fcx: FfiCtx? = null,
        var tablesRepo: TablesRepoFfi? = null,
        var blobsRepo: BlobsRepoFfi? = null,
        var plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi? = null,
        var drawerRepo: DrawerRepoFfi? = null,
        var configRepo: ConfigRepoFfi? = null,
        var dispatchRepo: DispatchRepoFfi? = null,
        var progressRepo: ProgressRepoFfi? = null,
        var initRepo: InitRepoFfi? = null,
        var sqliteLsRepo: SqliteLocalStateRepoFfi? = null,
        var cameraPreviewFfi: CameraPreviewFfi? = null,
        var startupProgress: StartupProgressTask? = null,
        var loadedSyncRepo: SyncRepoFfi? = null,
        var loadedRtFfi: RtFfi? = null,
    )

    private data class OpenRepoCoreRepos(
        val tablesRepo: TablesRepoFfi,
        val blobsRepo: BlobsRepoFfi,
        val plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi,
        val drawerRepo: DrawerRepoFfi,
        val configRepo: ConfigRepoFfi,
        val dispatchRepo: DispatchRepoFfi,
        val progressRepo: ProgressRepoFfi,
        val initRepo: InitRepoFfi,
        val sqliteLsRepo: SqliteLocalStateRepoFfi,
        val cameraPreviewFfi: CameraPreviewFfi,
    )

    private data class OpenRepoPrimaryRepos(
        val tablesRepo: TablesRepoFfi,
        val blobsRepo: BlobsRepoFfi,
        val plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi,
        val drawerRepo: DrawerRepoFfi,
        val configRepo: ConfigRepoFfi,
        val dispatchRepo: DispatchRepoFfi,
    )

    private data class OpenRepoSecondaryRepos(
        val initRepo: InitRepoFfi,
        val sqliteLsRepo: SqliteLocalStateRepoFfi,
        val cameraPreviewFfi: CameraPreviewFfi,
    )

    private suspend fun bootstrap(appStartElapsedMs: () -> Long) {
        runCatching {
            closeActiveContainer(updateState = false)
            _state.value = FfiRuntimeState.Loading
            println("[APP_INIT] stage=getRepoConfig start")
            val repoConfig =
                withContext(ioDispatcher) {
                    ffiServices.getRepoConfig()
                }
            println("[APP_INIT] stage=getRepoConfig done")
            val knownRepos = repoConfig.knownRepos
            val lastUsedRepo =
                repoConfig.lastUsedRepoId?.let { lastUsedRepoId ->
                    knownRepos.find { repo -> repo.id == lastUsedRepoId }
                }
            val shouldOpenLastUsedRepo =
                if (lastUsedRepo == null) {
                    false
                } else {
                    val lastUsedRepoPath = lastUsedRepo.path
                    println("[APP_INIT] stage=isRepoUsable start path=$lastUsedRepoPath")
                    val usable =
                        withContext(ioDispatcher) {
                            ffiServices.isRepoUsable(lastUsedRepoPath)
                        }
                    println("[APP_INIT] stage=isRepoUsable done usable=$usable")
                    usable
                }

            if (shouldOpenLastUsedRepo && lastUsedRepo != null) {
                openRepoInternal(lastUsedRepo.path, appStartElapsedMs)
            } else {
                _state.value = FfiRuntimeState.Welcome(repos = knownRepos)
            }
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            println("[APP_INIT] stage=bootstrap failed err=${exception.message}")
            _state.value = FfiRuntimeState.Error(exception)
        }
    }

    private suspend fun openRepoInternal(repoPath: String, appStartElapsedMs: () -> Long) {
        val startupMark = TimeSource.Monotonic.markNow()
        val startupPhaseId = Clock.System.now().toEpochMilliseconds().toString()
        val resources = OpenRepoResourceState()
        runCatching {
            closeActiveContainer(updateState = false)
            _state.value = FfiRuntimeState.OpeningRepo(repoPath = repoPath)
            val container =
                withContext(ioDispatcher) {
                    loadOpenRepoContainer(
                        repoPath = repoPath,
                        appStartElapsedMs = appStartElapsedMs,
                        startupMark = startupMark,
                        startupPhaseId = startupPhaseId,
                        resources = resources,
                    )
                }
            val syncRepo = loadOpenRepoSyncRepo(container)
            resources.loadedSyncRepo = syncRepo
            val rtFfi = loadOpenRepoRuntimeRepo(container)
            resources.loadedRtFfi = rtFfi
            withContext(ioDispatcher) {
                container.progressRepo.addUpdate(
                    STARTUP_PROGRESS_TASK_ID,
                    ProgressUpdate(
                        at = Clock.System.now(),
                        title = "App startup",
                        deets =
                        ProgressUpdateDeets.Completed(
                            state = ProgressFinalState.SUCCEEDED,
                            message =
                            "startup complete (from_app_start_ms=${appStartElapsedMs()})",
                        ),
                    ),
                )
            }
            val readyContainer = container.copy(syncRepo = syncRepo, rtFfi = rtFfi)
            activeContainer = readyContainer
            _state.value = FfiRuntimeState.Ready(readyContainer)
            resources.startupProgress?.complete("startup complete")
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            val startupErrorMessage =
                exception.message ?: exception::class.simpleName ?: "unknown error"
            resources.startupProgress?.fail(startupErrorMessage, startupMark.elapsedNow().toString())
            runCatching { cleanupOpenRepoResources(resources) }.onFailure { cleanupError ->
                exception.addSuppressed(cleanupError)
            }
            activeContainer = null
            _state.value = FfiRuntimeState.Error(exception)
        }
    }

    private suspend fun loadOpenRepoContainer(
        repoPath: String,
        appStartElapsedMs: () -> Long,
        startupMark: TimeMark,
        startupPhaseId: String,
        resources: OpenRepoResourceState,
    ): AppContainer {
        val fcxReady =
            stage("ffiServices.openRepoFfiCtx", resources.startupProgress) {
                ffiServices.openRepoFfiCtx(repoPath)
            }
        resources.fcx = fcxReady
        val loadedProgressRepo =
            stage("ProgressRepoFfi.load", resources.startupProgress) {
                ProgressRepoFfi.load(fcx = fcxReady)
            }
        resources.progressRepo = loadedProgressRepo
        resources.startupProgress =
            StartupProgressTask(
                progressRepo = loadedProgressRepo,
                taskId = STARTUP_PROGRESS_TASK_ID,
                appElapsedMillis = appStartElapsedMs,
                totalStages = OPEN_REPO_STAGE_COUNT,
            )
        val startupProgress = resources.startupProgress ?: error("startup progress must be initialized")
        startupProgress.begin(
            repoPath,
            startupMark.elapsedNow().toString(),
            startupPhaseId,
        )
        val coreRepos = loadOpenRepoCoreRepos(fcxReady, resources)
        return AppContainer(
            ffiCtx = fcxReady,
            drawerRepo = coreRepos.drawerRepo,
            tablesRepo = coreRepos.tablesRepo,
            dispatchRepo = coreRepos.dispatchRepo,
            progressRepo = coreRepos.progressRepo,
            initRepo = coreRepos.initRepo,
            sqliteLsRepo = coreRepos.sqliteLsRepo,
            rtFfi = null,
            plugsRepo = coreRepos.plugsRepo,
            configRepo = coreRepos.configRepo,
            blobsRepo = coreRepos.blobsRepo,
            syncRepo = null,
            cameraPreviewFfi = coreRepos.cameraPreviewFfi,
        )
    }

    private suspend fun loadOpenRepoCoreRepos(fcx: FfiCtx, resources: OpenRepoResourceState): OpenRepoCoreRepos {
        val primaryRepos = loadOpenRepoPrimaryRepos(fcx, resources)
        val secondaryRepos = loadOpenRepoSecondaryRepos(fcx, resources)
        return OpenRepoCoreRepos(
            tablesRepo = primaryRepos.tablesRepo,
            blobsRepo = primaryRepos.blobsRepo,
            plugsRepo = primaryRepos.plugsRepo,
            drawerRepo = primaryRepos.drawerRepo,
            configRepo = primaryRepos.configRepo,
            dispatchRepo = primaryRepos.dispatchRepo,
            progressRepo = resources.progressRepo ?: error("progress repo must be loaded before core repos"),
            initRepo = secondaryRepos.initRepo,
            sqliteLsRepo = secondaryRepos.sqliteLsRepo,
            cameraPreviewFfi = secondaryRepos.cameraPreviewFfi,
        )
    }

    private suspend fun loadOpenRepoPrimaryRepos(fcx: FfiCtx, resources: OpenRepoResourceState): OpenRepoPrimaryRepos {
        val loadedTablesRepo =
            stage("TablesRepoFfi.load", resources.startupProgress) {
                TablesRepoFfi.load(fcx = fcx)
            }
        resources.tablesRepo = loadedTablesRepo
        val loadedBlobsRepo =
            stage("BlobsRepoFfi.load", resources.startupProgress) {
                BlobsRepoFfi.load(fcx = fcx)
            }
        resources.blobsRepo = loadedBlobsRepo
        val loadedPlugsRepo =
            stage("PlugsRepoFfi.load", resources.startupProgress) {
                org.example.daybook.uniffi.PlugsRepoFfi.load(
                    fcx = fcx,
                    blobsRepo = loadedBlobsRepo,
                )
            }
        resources.plugsRepo = loadedPlugsRepo
        val loadedDrawerRepo =
            stage("DrawerRepoFfi.load", resources.startupProgress) {
                DrawerRepoFfi.load(
                    fcx = fcx,
                    plugsRepo = loadedPlugsRepo,
                )
            }
        resources.drawerRepo = loadedDrawerRepo
        val loadedConfigRepo =
            stage("ConfigRepoFfi.load", resources.startupProgress) {
                ConfigRepoFfi.load(
                    fcx = fcx,
                    plugRepo = loadedPlugsRepo,
                )
            }
        resources.configRepo = loadedConfigRepo
        val loadedDispatchRepo =
            stage("DispatchRepoFfi.load", resources.startupProgress) {
                DispatchRepoFfi.load(fcx = fcx)
            }
        resources.dispatchRepo = loadedDispatchRepo
        return OpenRepoPrimaryRepos(
            tablesRepo = loadedTablesRepo,
            blobsRepo = loadedBlobsRepo,
            plugsRepo = loadedPlugsRepo,
            drawerRepo = loadedDrawerRepo,
            configRepo = loadedConfigRepo,
            dispatchRepo = loadedDispatchRepo,
        )
    }

    private suspend fun loadOpenRepoSecondaryRepos(
        fcx: FfiCtx,
        resources: OpenRepoResourceState,
    ): OpenRepoSecondaryRepos {
        val loadedInitRepo =
            stage("InitRepoFfi.load", resources.startupProgress) {
                InitRepoFfi.load(
                    fcx = fcx,
                    progressRepo =
                    resources.progressRepo ?: error("progress repo must be loaded before init repo"),
                )
            }
        resources.initRepo = loadedInitRepo
        val loadedSqliteLsRepo =
            stage("SqliteLocalStateRepoFfi.load", resources.startupProgress) {
                SqliteLocalStateRepoFfi.load(fcx = fcx)
            }
        resources.sqliteLsRepo = loadedSqliteLsRepo
        val loadedCameraPreviewFfi =
            stage("CameraPreviewFfi.load", resources.startupProgress) {
                CameraPreviewFfi.load()
            }
        resources.cameraPreviewFfi = loadedCameraPreviewFfi
        stage("warmUpTablesRepo", resources.startupProgress) {
            warmUpTablesRepo(resources.tablesRepo ?: error("tables repo must be loaded before warm up"))
        }
        return OpenRepoSecondaryRepos(
            initRepo = loadedInitRepo,
            sqliteLsRepo = loadedSqliteLsRepo,
            cameraPreviewFfi = loadedCameraPreviewFfi,
        )
    }

    private suspend fun loadOpenRepoSyncRepo(container: AppContainer): SyncRepoFfi = withContext(ioDispatcher) {
        SyncRepoFfi.load(
            fcx = container.ffiCtx,
            configRepo = container.configRepo,
            blobsRepo = container.blobsRepo,
            drawerRepo = container.drawerRepo,
            progressRepo = container.progressRepo,
        )
    }

    private suspend fun loadOpenRepoRuntimeRepo(container: AppContainer): RtFfi = withContext(ioDispatcher) {
        RtFfi.load(
            fcx = container.ffiCtx,
            drawerRepo = container.drawerRepo,
            plugsRepo = container.plugsRepo,
            dispatchRepo = container.dispatchRepo,
            progressRepo = container.progressRepo,
            blobsRepo = container.blobsRepo,
            configRepo = container.configRepo,
            initRepo = container.initRepo,
            sqliteLsRepo = container.sqliteLsRepo,
            deviceId = "compose-client",
            startupProgressTaskId = STARTUP_PROGRESS_TASK_ID,
        )
    }

    private suspend fun cleanupOpenRepoResources(resources: OpenRepoResourceState) {
        val failures = mutableListOf<Throwable>()
        cleanupRuntimeRepoResources(resources, failures)
        cleanupStorageRepoResources(resources, failures)
        cleanupCoreRepoResources(resources, failures)
        if (failures.isNotEmpty()) {
            val first = failures.first()
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    private suspend fun cleanupRuntimeRepoResources(
        resources: OpenRepoResourceState,
        failures: MutableList<Throwable>,
    ) {
        resources.loadedRtFfi?.let {
            cleanupStopOnIo("runtime ffi", failures) { it.stop() }
            cleanupCloseSafely("runtime ffi", failures) { it.close() }
        }
        resources.loadedSyncRepo?.let {
            cleanupStopOnIo("sync repo", failures) { it.stop() }
            cleanupCloseSafely("sync repo", failures) { it.close() }
        }
        resources.cameraPreviewFfi?.let {
            cleanupCloseSafely("camera preview ffi", failures) { it.close() }
        }
    }

    private suspend fun cleanupStorageRepoResources(
        resources: OpenRepoResourceState,
        failures: MutableList<Throwable>,
    ) {
        resources.sqliteLsRepo?.let {
            cleanupStopOnIo("sqlite local state repo", failures) { it.stop() }
            cleanupCloseSafely("sqlite local state repo", failures) { it.close() }
        }
        resources.initRepo?.let {
            cleanupStopOnIo("init repo", failures) { it.stop() }
            cleanupCloseSafely("init repo", failures) { it.close() }
        }
        resources.progressRepo?.let {
            cleanupStopOnIo("progress repo", failures) { it.stop() }
            cleanupCloseSafely("progress repo", failures) { it.close() }
        }
    }

    private fun cleanupCoreRepoResources(resources: OpenRepoResourceState, failures: MutableList<Throwable>) {
        resources.drawerRepo?.let { cleanupCloseSafely("drawer repo", failures) { it.close() } }
        resources.tablesRepo?.let { cleanupCloseSafely("tables repo", failures) { it.close() } }
        resources.dispatchRepo?.let { cleanupCloseSafely("dispatch repo", failures) { it.close() } }
        resources.plugsRepo?.let { cleanupCloseSafely("plugs repo", failures) { it.close() } }
        resources.configRepo?.let { cleanupCloseSafely("config repo", failures) { it.close() } }
        resources.blobsRepo?.let { cleanupCloseSafely("blobs repo", failures) { it.close() } }
        resources.fcx?.let { cleanupCloseSafely("ffi ctx", failures) { it.close() } }
    }

    private suspend fun cleanupStopOnIo(label: String, failures: MutableList<Throwable>, block: suspend () -> Unit) {
        runCatching {
            withContext(ioDispatcher) {
                println("[APP_INIT] cleanup stopping $label")
                block()
            }
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            failures += exception
            println("[APP_INIT] cleanup failed err=${exception.message}")
        }
    }

    private fun cleanupCloseSafely(label: String, failures: MutableList<Throwable>, block: () -> Unit) {
        runCatching {
            println("[APP_INIT] cleanup closing $label")
            block()
        }.onFailure { exception ->
            if (exception is CancellationException) {
                throw exception
            }
            failures += exception
            println("[APP_INIT] cleanup failed err=${exception.message}")
        }
    }

    private suspend fun closeActiveContainer(updateState: Boolean) {
        val container = activeContainer ?: return
        // Null the reference first so a partial shutdown can't leave callers observing a
        // container that is mid-tear-down; shutdownAppContainer may throw partway.
        activeContainer = null
        shutdownAppContainer(container, ioDispatcher)
        if (updateState && _state.value is FfiRuntimeState.Ready) {
            _state.value = FfiRuntimeState.Loading
        }
    }

    override fun onCleared() {
        runBlocking(ioDispatcher) {
            runtimeJob?.cancel()
            runtimeMutex.withLock {
                closeActiveContainer(updateState = false)
            }
        }
        super.onCleared()
    }

    private suspend fun <T> stage(label: String, startupProgress: StartupProgressTask?, block: suspend () -> T): T =
        if (startupProgress == null) {
            block()
        } else {
            var value: T? = null
            startupProgress.stage(label) {
                value = block()
            }
            @Suppress("UNCHECKED_CAST")
            value as T
        }
}
