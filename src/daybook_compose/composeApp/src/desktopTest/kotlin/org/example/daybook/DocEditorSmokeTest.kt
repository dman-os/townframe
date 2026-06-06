package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertTextContains
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.v2.runComposeUiTest
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.Test
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.example.daybook.drawer.DocEditorScreen
import org.example.daybook.layouts.ProvideScreenChromeSpec
import org.example.daybook.layouts.ScreenChromeSpec
import org.example.daybook.theme.DaybookTheme
import org.example.daybook.theme.ThemeConfig
import org.example.daybook.ui.buildNoteFacet
import org.example.daybook.ui.encodeJsonString
import org.example.daybook.ui.encodeWellKnownFacet
import org.example.daybook.ui.editor.facetKeyString
import org.example.daybook.ui.editor.noteFacetKey
import org.example.daybook.ui.editor.titleFacetKey
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
import org.example.daybook.uniffi.SqliteLocalStateRepoFfi
import org.example.daybook.uniffi.TablesRepoFfi
import org.example.daybook.uniffi.types.AddDocArgs

@OptIn(ExperimentalTestApi::class)
class DocEditorSmokeTest {
    @Test
    fun realRepo_wires_into_doc_editor_screen() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val titleText = "Smoke test title"
            val noteText = "Smoke test note"
            val docId = fixture.createDoc(titleText = titleText, noteText = noteText)

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                        }
                    }
                }
            }

            runOnIdle {
                docEditorStore.selectDoc(docId)
            }

            waitForIdle()

            onNodeWithTag(DaybookEditorSemantics.Screen).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.Editor).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.TitleField).assertTextContains(titleText)
            onNodeWithTag(DaybookEditorSemantics.noteField(facetKeyString(noteFacetKey()))).assertTextContains(
                noteText,
            )
            onNodeWithTag(DaybookEditorSemantics.Details).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }
}

private class RealRepoFixture(
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
    val cameraPreviewFfi: CameraPreviewFfi,
    val container: AppContainer,
) {
    fun createDoc(titleText: String, noteText: String): String = runBlocking(Dispatchers.IO) {
        val facets =
            mutableMapOf(
                titleFacetKey() to encodeJsonString(titleText),
                noteFacetKey() to encodeWellKnownFacet(buildNoteFacet(noteText)),
            )
        drawerRepo.add(
            AddDocArgs(
                branchPath = "main",
                facets = facets,
                userPath = null,
            ),
        )
    }

    @OptIn(ExperimentalPathApi::class)
    fun close() {
        val failures = mutableListOf<Throwable>()

        fun recordFailure(throwable: Throwable) {
            failures += throwable
        }

        suspend fun stopOnIo(label: String, block: suspend () -> Unit) {
            try {
                withContext(Dispatchers.IO) {
                    block()
                }
            } catch (throwable: Throwable) {
                recordFailure(throwable)
            }
        }

        fun closeSafely(label: String, block: () -> Unit) {
            try {
                block()
            } catch (throwable: Throwable) {
                recordFailure(throwable)
            }
        }

        runBlocking(Dispatchers.IO) {
            stopOnIo("init repo") { initRepo.stop() }
            stopOnIo("sqlite local state repo") { sqliteLsRepo.stop() }
            stopOnIo("progress repo") { progressRepo.stop() }
            closeSafely("camera preview ffi") { cameraPreviewFfi.close() }
            closeSafely("drawer repo") { drawerRepo.close() }
            closeSafely("tables repo") { tablesRepo.close() }
            closeSafely("dispatch repo") { dispatchRepo.close() }
            closeSafely("config repo") { configRepo.close() }
            closeSafely("plugs repo") { plugsRepo.close() }
            closeSafely("blobs repo") { blobsRepo.close() }
            closeSafely("ffi ctx") { ffiCtx.close() }
            closeSafely("app ctx") { appCtx.close() }
            closeSafely("repo root") { repoRoot.deleteRecursively() }
        }

        if (failures.isNotEmpty()) {
            val first = failures.first()
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    companion object {
        suspend fun create(): RealRepoFixture {
            val repoRoot = Files.createTempDirectory("daybook-compose-smoke")
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
            val container =
                AppContainer(
                    ffiCtx = ffiCtx,
                    drawerRepo = drawerRepo,
                    tablesRepo = tablesRepo,
                    dispatchRepo = dispatchRepo,
                    progressRepo = progressRepo,
                    initRepo = initRepo,
                    sqliteLsRepo = sqliteLsRepo,
                    rtFfi = null,
                    plugsRepo = plugsRepo,
                    configRepo = configRepo,
                    blobsRepo = blobsRepo,
                    syncRepo = null,
                    cameraPreviewFfi = cameraPreviewFfi,
                )
            return RealRepoFixture(
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
                cameraPreviewFfi = cameraPreviewFfi,
                container = container,
            )
        }
    }
}
