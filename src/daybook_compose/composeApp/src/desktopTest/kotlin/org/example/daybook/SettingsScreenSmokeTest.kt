package org.example.daybook

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.requiredHeight
import androidx.compose.foundation.layout.requiredWidth
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.Modifier
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performSemanticsAction
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.v2.runComposeUiTest
import androidx.compose.ui.unit.dp
import androidx.compose.ui.semantics.SemanticsActions
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.Test
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.example.daybook.layouts.ProvideScreenChromeSpec
import org.example.daybook.layouts.ScreenChromeSpec
import org.example.daybook.settings.SettingsScreen
import org.example.daybook.settings.SettingsScreenSemantics
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.uniffi.AppFfiCtx
import org.example.daybook.uniffi.BlobsRepoFfi
import org.example.daybook.uniffi.CameraPreviewFfi
import org.example.daybook.uniffi.ConfigRepoFfi
import org.example.daybook.uniffi.DispatchRepoFfi
import org.example.daybook.uniffi.DrawerRepoFfi
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.InitRepoFfi
import org.example.daybook.uniffi.PlugsRepoFfi
import org.example.daybook.uniffi.ProgressRepoFfi
import org.example.daybook.uniffi.RtFfi
import org.example.daybook.uniffi.SqliteLocalStateRepoFfi
import org.example.daybook.uniffi.TablesRepoFfi

@OptIn(ExperimentalTestApi::class)
class SettingsScreenSmokeTest {
    @Test
    fun realRepo_renders_plugs_and_opens_placeholder_add_dialog() = runComposeUiTest {
        val fixture = runBlocking { SettingsRealRepoFixture.create() }
        val outerBackClicks = AtomicInteger(0)
        try {
            val installedPlugs = runBlocking { fixture.plugsRepo.listPlugs() }
            val systemPlug =
                installedPlugs.firstOrNull {
                    it.namespace.contains("daybook", ignoreCase = true) ||
                        it.title.contains("daybook", ignoreCase = true)
                } ?: error("expected a system plug in listPlugs(): $installedPlugs")

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(
                        ScreenChromeSpec(
                            topBar =
                            ScreenChromeSpec.TopBarSpec(
                                showBack = true,
                                onBack = { outerBackClicks.incrementAndGet() },
                            ),
                        ),
                    ) {
                        CompositionLocalProvider(LocalContainer provides fixture.container) {
                            BigDialogHost(narrowScreen = false) {
                                Box(
                                    modifier =
                                    Modifier
                                        .requiredWidth(980.dp)
                                        .requiredHeight(900.dp),
                                ) {
                                    SettingsScreen()
                                }
                            }
                        }
                    }
                }
            }

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.sectionItem("plugs"))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(SettingsScreenSemantics.ROOT).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.SECTION_LIST).assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                .performSemanticsAction(SemanticsActions.OnClick)
            check(outerBackClicks.get() == 1) { "expected settings back to call parent onBack" }
            onNodeWithTag(SettingsScreenSemantics.sectionItem("plugs")).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.sectionItem("mltools")).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.sectionDetail("plugs")).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.PLUGS_LIST).assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithText(systemPlug.title).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.plugRow(systemPlug.id)).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.PLUGS_ADD_BUTTON).performClick()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.PLUGS_ADD_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.PLUGS_ADD_DIALOG).assertIsDisplayed()
            onNodeWithText("Import plug").assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_navigates_to_mltools_detail_on_compact_width() = runComposeUiTest {
        val fixture = runBlocking { SettingsRealRepoFixture.create() }
        val outerBackClicks = AtomicInteger(0)
        try {
            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(
                        ScreenChromeSpec(
                            topBar =
                            ScreenChromeSpec.TopBarSpec(
                                showBack = true,
                                onBack = { outerBackClicks.incrementAndGet() },
                            ),
                        ),
                    ) {
                        CompositionLocalProvider(LocalContainer provides fixture.container) {
                            BigDialogHost(narrowScreen = true) {
                                Box(
                                    modifier =
                                    Modifier
                                        .requiredWidth(420.dp)
                                        .requiredHeight(900.dp),
                                ) {
                                    SettingsScreen()
                                }
                            }
                        }
                    }
                }
            }

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SECTION_LIST)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.SECTION_LIST).assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                .performSemanticsAction(SemanticsActions.OnClick)
            check(outerBackClicks.get() == 1) { "expected settings root back to call parent onBack" }
            onNodeWithTag(SettingsScreenSemantics.sectionItem("mltools")).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.sectionDetail("mltools"))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.sectionDetail("mltools")).assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SECTION_LIST)
                    .fetchSemanticsNodes()
                    .isEmpty()
            }
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.MLTOOLS_PROVISION_BUTTON).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.MLTOOLS_STATUS).assertIsDisplayed()
            onNodeWithTag(SettingsScreenSemantics.MLTOOLS_DOWNLOAD_TASKS).assertIsDisplayed()

            onNodeWithTag(SettingsScreenSemantics.SETTINGS_BACK_BUTTON)
                .performSemanticsAction(SemanticsActions.OnClick)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(SettingsScreenSemantics.SECTION_LIST)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(SettingsScreenSemantics.SECTION_LIST).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }
}

private class SettingsRealRepoFixture(
    val repoRoot: Path,
    val appCtx: AppFfiCtx,
    val ffiCtx: FfiCtx,
    val progressRepo: ProgressRepoFfi,
    val tablesRepo: TablesRepoFfi,
    val blobsRepo: BlobsRepoFfi,
    val plugsRepo: PlugsRepoFfi,
    val drawerRepo: DrawerRepoFfi,
    val configRepo: ConfigRepoFfi,
    val dispatchRepo: DispatchRepoFfi,
    val initRepo: InitRepoFfi,
    val sqliteLsRepo: SqliteLocalStateRepoFfi,
    val rtFfi: RtFfi?,
    val cameraPreviewFfi: CameraPreviewFfi,
    val container: AppContainer,
) {
    @OptIn(ExperimentalPathApi::class)
    fun close() {
        val failures = mutableListOf<Throwable>()

        fun recordFailure(throwable: Throwable) {
            failures += throwable
        }

        suspend fun stopOnIo(block: suspend () -> Unit) {
            try {
                withContext(Dispatchers.IO) {
                    block()
                }
            } catch (throwable: Throwable) {
                recordFailure(throwable)
            }
        }

        fun closeSafely(block: () -> Unit) {
            try {
                block()
            } catch (throwable: Throwable) {
                recordFailure(throwable)
            }
        }

        runBlocking(Dispatchers.IO) {
            if (rtFfi != null) {
                stopOnIo { rtFfi.stop() }
            }
            stopOnIo { initRepo.stop() }
            stopOnIo { sqliteLsRepo.stop() }
            stopOnIo { progressRepo.stop() }
            closeSafely { cameraPreviewFfi.close() }
            if (rtFfi != null) {
                closeSafely { rtFfi.close() }
            }
            closeSafely { drawerRepo.close() }
            closeSafely { tablesRepo.close() }
            closeSafely { dispatchRepo.close() }
            closeSafely { configRepo.close() }
            closeSafely { plugsRepo.close() }
            closeSafely { blobsRepo.close() }
            closeSafely { ffiCtx.close() }
            closeSafely { appCtx.close() }
            closeSafely { repoRoot.deleteRecursively() }
        }

        if (failures.isNotEmpty()) {
            val first = failures.first()
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    companion object {
        suspend fun create(loadRt: Boolean = false): SettingsRealRepoFixture {
            val repoRoot = Files.createTempDirectory("daybook-compose-settings-smoke")
            val appCtx = withContext(Dispatchers.IO) { AppFfiCtx.init() }
            val ffiCtx = withContext(Dispatchers.IO) { FfiCtx.init(repoRoot.toString(), appCtx) }
            val progressRepo = withContext(Dispatchers.IO) { ProgressRepoFfi.load(ffiCtx) }
            val tablesRepo = withContext(Dispatchers.IO) { TablesRepoFfi.load(ffiCtx) }
            val blobsRepo = withContext(Dispatchers.IO) { BlobsRepoFfi.load(ffiCtx) }
            val plugsRepo = withContext(Dispatchers.IO) { PlugsRepoFfi.load(ffiCtx, blobsRepo) }
            val drawerRepo = withContext(Dispatchers.IO) { DrawerRepoFfi.load(ffiCtx, plugsRepo) }
            val configRepo = withContext(Dispatchers.IO) { ConfigRepoFfi.load(ffiCtx, plugsRepo) }
            val dispatchRepo = withContext(Dispatchers.IO) { DispatchRepoFfi.load(ffiCtx) }
            val initRepo = withContext(Dispatchers.IO) { InitRepoFfi.load(ffiCtx, progressRepo) }
            val sqliteLsRepo = withContext(Dispatchers.IO) { SqliteLocalStateRepoFfi.load(ffiCtx) }
            val cameraPreviewFfi = withContext(Dispatchers.IO) { CameraPreviewFfi.load() }
            val rtFfi =
                if (loadRt) {
                    withContext(Dispatchers.IO) {
                        RtFfi.load(
                            fcx = ffiCtx,
                            drawerRepo = drawerRepo,
                            plugsRepo = plugsRepo,
                            dispatchRepo = dispatchRepo,
                            progressRepo = progressRepo,
                            blobsRepo = blobsRepo,
                            configRepo = configRepo,
                            initRepo = initRepo,
                            sqliteLsRepo = sqliteLsRepo,
                            deviceId = "settings-screen-smoke",
                            startupProgressTaskId = null,
                        )
                    }
                } else {
                    null
                }
            val container =
                AppContainer(
                    ffiCtx = ffiCtx,
                    drawerRepo = drawerRepo,
                    tablesRepo = tablesRepo,
                    dispatchRepo = dispatchRepo,
                    progressRepo = progressRepo,
                    initRepo = initRepo,
                    sqliteLsRepo = sqliteLsRepo,
                    rtFfi = rtFfi,
                    plugsRepo = plugsRepo,
                    configRepo = configRepo,
                    blobsRepo = blobsRepo,
                    syncRepo = null,
                    cameraPreviewFfi = cameraPreviewFfi,
                )
            return SettingsRealRepoFixture(
                repoRoot = repoRoot,
                appCtx = appCtx,
                ffiCtx = ffiCtx,
                progressRepo = progressRepo,
                tablesRepo = tablesRepo,
                blobsRepo = blobsRepo,
                plugsRepo = plugsRepo,
                drawerRepo = drawerRepo,
                configRepo = configRepo,
                dispatchRepo = dispatchRepo,
                initRepo = initRepo,
                sqliteLsRepo = sqliteLsRepo,
                rtFfi = rtFfi,
                cameraPreviewFfi = cameraPreviewFfi,
                container = container,
            )
        }
    }
}
