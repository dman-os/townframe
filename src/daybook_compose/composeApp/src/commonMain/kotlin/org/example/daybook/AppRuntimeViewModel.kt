package org.example.daybook

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
import kotlin.time.TimeSource

internal suspend fun warmUpTablesRepo(tablesRepo: TablesRepoFfi) {
    tablesRepo.listWindows()
    tablesRepo.listTabs()
    tablesRepo.listPanels()
    tablesRepo.listTables()
}

internal const val STARTUP_PROGRESS_TASK_ID = "app/init/startup"

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
                    "startup phase begin phase=$phaseId (open_elapsed=$startupElapsed from_app_start_ms=${appElapsedMillis()})",
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
                    message = "stage ${doneStages}/${totalStages}: $label",
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

internal suspend fun shutdownAppContainer(appContainer: AppContainer) {
    println("[APP_SHUTDOWN] flushing to disk: begin")
    val failures = mutableListOf<Throwable>()

    fun recordFailure(throwable: Throwable) {
        failures += throwable
        println("[APP_SHUTDOWN] cleanup failed err=${throwable.message}")
    }

    suspend fun stopOnIo(label: String, block: suspend () -> Unit) {
        try {
            withContext(Dispatchers.IO) {
                println("[APP_SHUTDOWN] flushing to disk: stopping $label")
                block()
            }
        } catch (throwable: Throwable) {
            recordFailure(throwable)
        }
    }

    fun closeSafely(label: String, block: () -> Unit) {
        try {
            println("[APP_SHUTDOWN] flushing to disk: closing $label")
            block()
        } catch (throwable: Throwable) {
            recordFailure(throwable)
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
            try {
                val repoConfig =
                    withContext(Dispatchers.IO) {
                        ffiServices.forgetKnownRepo(repoId)
                    }
                _state.value = FfiRuntimeState.Welcome(repos = repoConfig.knownRepos)
            } catch (throwable: Throwable) {
                if (throwable is CancellationException) throw throwable
                _state.value = FfiRuntimeState.Error(throwable)
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

    private suspend fun bootstrap(appStartElapsedMs: () -> Long) {
        try {
            closeActiveContainer(updateState = false)
            _state.value = FfiRuntimeState.Loading
            println("[APP_INIT] stage=getRepoConfig start")
            val repoConfig =
                withContext(Dispatchers.IO) {
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
                        withContext(Dispatchers.IO) {
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
        } catch (throwable: Throwable) {
            if (throwable is CancellationException) {
                throw throwable
            }
            println("[APP_INIT] stage=bootstrap failed err=${throwable.message}")
            _state.value = FfiRuntimeState.Error(throwable)
        }
    }

    private suspend fun openRepoInternal(
        repoPath: String,
        appStartElapsedMs: () -> Long,
    ) {
        val startupMark = TimeSource.Monotonic.markNow()
        val startupPhaseId = Clock.System.now().toEpochMilliseconds().toString()
        val startupStageCount = 10

        var fcx: FfiCtx? = null
        var tablesRepo: TablesRepoFfi? = null
        var blobsRepo: BlobsRepoFfi? = null
        var plugsRepo: org.example.daybook.uniffi.PlugsRepoFfi? = null
        var drawerRepo: DrawerRepoFfi? = null
        var configRepo: ConfigRepoFfi? = null
        var dispatchRepo: DispatchRepoFfi? = null
        var progressRepo: ProgressRepoFfi? = null
        var initRepo: InitRepoFfi? = null
        var sqliteLsRepo: SqliteLocalStateRepoFfi? = null
        var cameraPreviewFfi: CameraPreviewFfi? = null
        var startupProgress: StartupProgressTask? = null
        var loadedSyncRepo: SyncRepoFfi? = null
        var loadedRtFfi: RtFfi? = null

        suspend fun cleanupAttempt() {
            val failures = mutableListOf<Throwable>()

            fun recordFailure(throwable: Throwable) {
                failures += throwable
                println("[APP_INIT] cleanup failed err=${throwable.message}")
            }

            suspend fun stopOnIo(label: String, block: suspend () -> Unit) {
                try {
                    withContext(Dispatchers.IO) {
                        println("[APP_INIT] cleanup stopping $label")
                        block()
                    }
                } catch (throwable: Throwable) {
                    recordFailure(throwable)
                }
            }

            fun closeSafely(label: String, block: () -> Unit) {
                try {
                    println("[APP_INIT] cleanup closing $label")
                    block()
                } catch (throwable: Throwable) {
                    recordFailure(throwable)
                }
            }

            loadedRtFfi?.let {
                stopOnIo("runtime ffi") { it.stop() }
                closeSafely("runtime ffi") { it.close() }
            }
            loadedSyncRepo?.let {
                stopOnIo("sync repo") { it.stop() }
                closeSafely("sync repo") { it.close() }
            }
            cameraPreviewFfi?.let { closeSafely("camera preview ffi") { it.close() } }
            sqliteLsRepo?.let {
                stopOnIo("sqlite local state repo") { it.stop() }
                closeSafely("sqlite local state repo") { it.close() }
            }
            initRepo?.let {
                stopOnIo("init repo") { it.stop() }
                closeSafely("init repo") { it.close() }
            }
            progressRepo?.let {
                stopOnIo("progress repo") { it.stop() }
                closeSafely("progress repo") { it.close() }
            }
            drawerRepo?.let { closeSafely("drawer repo") { it.close() } }
            tablesRepo?.let { closeSafely("tables repo") { it.close() } }
            dispatchRepo?.let { closeSafely("dispatch repo") { it.close() } }
            plugsRepo?.let { closeSafely("plugs repo") { it.close() } }
            configRepo?.let { closeSafely("config repo") { it.close() } }
            blobsRepo?.let { closeSafely("blobs repo") { it.close() } }
            fcx?.let { closeSafely("ffi ctx") { it.close() } }
            if (failures.isNotEmpty()) {
                val first = failures.first()
                failures.drop(1).forEach(first::addSuppressed)
                throw first
            }
        }

        try {
            closeActiveContainer(updateState = false)
            _state.value = FfiRuntimeState.OpeningRepo(repoPath = repoPath)
            val container =
                withContext(Dispatchers.IO) {
                    fcx =
                        stage("ffiServices.openRepoFfiCtx", startupProgress) {
                            ffiServices.openRepoFfiCtx(repoPath)
                        }
                    val fcxReady = fcx ?: error("ffi context initialization failed")
                    progressRepo =
                        stage("ProgressRepoFfi.load", startupProgress) {
                            ProgressRepoFfi.load(fcx = fcxReady)
                        }
                    startupProgress =
                        StartupProgressTask(
                            progressRepo = progressRepo ?: error("progress repo failed to load"),
                            taskId = STARTUP_PROGRESS_TASK_ID,
                            appElapsedMillis = appStartElapsedMs,
                            totalStages = startupStageCount,
                        )
                    startupProgress.begin(
                        repoPath,
                        startupMark.elapsedNow().toString(),
                        startupPhaseId,
                    )
                    tablesRepo =
                        stage("TablesRepoFfi.load", startupProgress) {
                            TablesRepoFfi.load(fcx = fcxReady)
                        }
                    blobsRepo =
                        stage("BlobsRepoFfi.load", startupProgress) {
                            BlobsRepoFfi.load(fcx = fcxReady)
                        }
                    plugsRepo =
                        stage("PlugsRepoFfi.load", startupProgress) {
                            org.example.daybook.uniffi.PlugsRepoFfi.load(
                                fcx = fcxReady,
                                blobsRepo = blobsRepo ?: error("blobs repo failed to load"),
                            )
                        }
                    drawerRepo =
                        stage("DrawerRepoFfi.load", startupProgress) {
                            DrawerRepoFfi.load(
                                fcx = fcxReady,
                                plugsRepo = plugsRepo ?: error("plugs repo failed to load"),
                            )
                        }
                    configRepo =
                        stage("ConfigRepoFfi.load", startupProgress) {
                            ConfigRepoFfi.load(
                                fcx = fcxReady,
                                plugRepo = plugsRepo ?: error("plugs repo failed to load"),
                            )
                        }
                    dispatchRepo =
                        stage("DispatchRepoFfi.load", startupProgress) {
                            DispatchRepoFfi.load(fcx = fcxReady)
                        }
                    initRepo =
                        stage("InitRepoFfi.load", startupProgress) {
                            InitRepoFfi.load(
                                fcx = fcxReady,
                                progressRepo = progressRepo ?: error("progress repo failed to load"),
                            )
                        }
                    sqliteLsRepo =
                        stage("SqliteLocalStateRepoFfi.load", startupProgress) {
                            SqliteLocalStateRepoFfi.load(fcx = fcxReady)
                        }
                    cameraPreviewFfi =
                        stage("CameraPreviewFfi.load", startupProgress) {
                            CameraPreviewFfi.load()
                        }
                    stage("warmUpTablesRepo", startupProgress) {
                        warmUpTablesRepo(tablesRepo ?: error("tables repo failed to load"))
                    }
                    AppContainer(
                        ffiCtx = fcxReady,
                        drawerRepo = drawerRepo ?: error("drawer repo failed to load"),
                        tablesRepo = tablesRepo ?: error("tables repo failed to load"),
                        dispatchRepo = dispatchRepo ?: error("dispatch repo failed to load"),
                        progressRepo = progressRepo ?: error("progress repo failed to load"),
                        initRepo = initRepo ?: error("init repo failed to load"),
                        sqliteLsRepo =
                        sqliteLsRepo ?: error("sqlite local state repo failed to load"),
                        rtFfi = null,
                        plugsRepo = plugsRepo ?: error("plugs repo failed to load"),
                        configRepo = configRepo ?: error("config repo failed to load"),
                        blobsRepo = blobsRepo ?: error("blobs repo failed to load"),
                        syncRepo = null,
                        cameraPreviewFfi =
                        cameraPreviewFfi ?: error("camera preview ffi failed to load"),
                    )
                }

            loadedSyncRepo =
                withContext(Dispatchers.IO) {
                    SyncRepoFfi.load(
                        fcx = container.ffiCtx,
                        configRepo = container.configRepo,
                        blobsRepo = container.blobsRepo,
                        drawerRepo = container.drawerRepo,
                        progressRepo = container.progressRepo,
                    )
                }
            loadedRtFfi =
                withContext(Dispatchers.IO) {
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
            withContext(Dispatchers.IO) {
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
            activeContainer = container.copy(syncRepo = loadedSyncRepo, rtFfi = loadedRtFfi)
            _state.value = FfiRuntimeState.Ready(activeContainer ?: error("ready container missing"))
            startupProgress?.complete("startup complete")
        } catch (throwable: Throwable) {
            if (throwable is CancellationException) {
                throw throwable
            }
            val startupErrorMessage =
                throwable.message ?: throwable::class.simpleName ?: "unknown error"
            startupProgress?.fail(startupErrorMessage, startupMark.elapsedNow().toString())
            runCatching { cleanupAttempt() }.exceptionOrNull()?.let { cleanupError ->
                throwable.addSuppressed(cleanupError)
            }
            activeContainer = null
            _state.value = FfiRuntimeState.Error(throwable)
        }
    }

    private suspend fun closeActiveContainer(updateState: Boolean) {
        val container = activeContainer ?: return
        shutdownAppContainer(container)
        activeContainer = null
        if (updateState && _state.value is FfiRuntimeState.Ready) {
            _state.value = FfiRuntimeState.Loading
        }
    }

    override fun onCleared() {
        runBlocking(Dispatchers.IO) {
            runtimeJob?.cancel()
            runtimeMutex.withLock {
                closeActiveContainer(updateState = false)
            }
        }
        super.onCleared()
    }

    private suspend fun <T> stage(label: String, startupProgress: StartupProgressTask?, block: suspend () -> T): T {
        return if (startupProgress == null) {
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
}
