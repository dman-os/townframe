package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertTextContains
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performCustomAccessibilityActionWithLabel
import androidx.compose.ui.test.performClick
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
import org.example.daybook.ui.view.DaybookViewSemantics
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
import org.example.daybook.uniffi.types.AddDocArgs
import org.example.daybook.uniffi.types.FacetDisplayDeets
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.FacetViewMode
import org.example.daybook.uniffi.types.ViewRef
import org.example.daybook.uniffi.types.WellKnownFacetTag

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

            docEditorStore.selectDoc(docId)

            waitForIdle()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithText(titleText).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.Screen).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.Editor).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.Details).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.TitleField).assertTextContains(titleText)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithText(noteText).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.noteField(facetKeyString(noteFacetKey()))).assertTextContains(
                noteText,
            )
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_actions_are_accessible_and_work() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val secondNoteKey = noteFacetKeyWithId("second")
            val firstNoteLabel = facetKeyString(noteFacetKey())
            val secondNoteLabel = facetKeyString(secondNoteKey)
            val docId = fixture.createDoc(
                titleText = "Block action smoke title",
                noteText = "First note block",
                extraNotes = listOf(secondNoteKey to "Second note block"),
            )

            fun contentFacetLabels(): List<String> =
                docEditorStore.selectedController.value?.state?.value?.contentFacetViews
                    ?.map { descriptor -> facetKeyString(descriptor.facetKey) }
                    .orEmpty()

            fun indexOfFacet(facetKeyLabel: String): Int =
                contentFacetLabels().indexOf(facetKeyLabel)

            fun openBlockActions(facetKeyLabel: String) {
                onNodeWithTag(DaybookEditorSemantics.facetRow(facetKeyLabel))
                    .performCustomAccessibilityActionWithLabel("Block actions")
            }

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

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                val labels = contentFacetLabels()
                labels.contains(firstNoteLabel) && labels.contains(secondNoteLabel)
            }

            openBlockActions(firstNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.addNoteAfterAction(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.addNoteAfterAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                docEditorStore.selectedController.value?.state?.value?.contentFacetViews?.size == 3
            }

            openBlockActions(secondNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.makePrimaryAction(secondNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.makePrimaryAction(secondNoteLabel)).performClick()
            waitUntil(timeoutMillis = 10_000) {
                indexOfFacet(secondNoteLabel) < indexOfFacet(firstNoteLabel)
            }

            openBlockActions(secondNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.moveDownAction(secondNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.moveDownAction(secondNoteLabel)).performClick()
            waitUntil(timeoutMillis = 10_000) {
                indexOfFacet(firstNoteLabel) < indexOfFacet(secondNoteLabel)
            }

            openBlockActions(secondNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.moveUpAction(secondNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.moveUpAction(secondNoteLabel)).performClick()
            waitUntil(timeoutMillis = 10_000) {
                indexOfFacet(secondNoteLabel) < indexOfFacet(firstNoteLabel)
            }
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realFfi_renders_custom_view_facet_in_doc_editor_screen() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create(loadRt = true) }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            fixture.importPlugTestOci()
            fixture.setNoteCustomViewHint()

            val docId = fixture.createDoc(
                titleText = "Custom view smoke title",
                noteText = "This note is rendered through plug_test.",
            )

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

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 20_000) {
                onAllNodesWithText("Sample summary").fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.pluginFacet(facetKeyString(noteFacetKey()))).assertIsDisplayed()
            onNodeWithTag(DaybookViewSemantics.Root).assertIsDisplayed()
            onNodeWithText("Sample summary").assertIsDisplayed()
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
    val rtFfi: RtFfi?,
    val cameraPreviewFfi: CameraPreviewFfi,
    val container: AppContainer,
) {
    fun createDoc(
        titleText: String,
        noteText: String,
        extraNotes: List<Pair<FacetKey, String>> = emptyList(),
    ): String = runBlocking(Dispatchers.IO) {
        val facets =
            mutableMapOf(
                titleFacetKey() to encodeJsonString(titleText),
                noteFacetKey() to encodeWellKnownFacet(buildNoteFacet(noteText)),
            )
        for ((facetKey, content) in extraNotes) {
            facets[facetKey] = encodeWellKnownFacet(buildNoteFacet(content))
        }
        drawerRepo.add(
            AddDocArgs(
                branchPath = "main",
                facets = facets,
                userPath = null,
            ),
        )
    }

    fun importPlugTestOci() = runBlocking(Dispatchers.IO) {
        plugsRepo.importFromOciLayout(plugTestOciPath().toString())
    }

    fun setNoteCustomViewHint() = runBlocking(Dispatchers.IO) {
        configRepo.setFacetDisplayHint(
            NOTE_DISPLAY_HINT_KEY,
            FacetDisplayHint(
                alwaysVisible = true,
                displayTitle = "Plug test sample",
                deets =
                FacetDisplayDeets.CustomView(
                    view = ViewRef(plugId = PLUG_TEST_ID, viewKey = PLUG_TEST_SAMPLE_VIEW_KEY),
                    mode = FacetViewMode.DISPLAY,
                    priority = 0,
                ),
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
            if (rtFfi != null) {
                stopOnIo("rt ffi") { rtFfi.stop() }
            }
            stopOnIo("init repo") { initRepo.stop() }
            stopOnIo("sqlite local state repo") { sqliteLsRepo.stop() }
            stopOnIo("progress repo") { progressRepo.stop() }
            closeSafely("camera preview ffi") { cameraPreviewFfi.close() }
            if (rtFfi != null) {
                closeSafely("rt ffi") { rtFfi.close() }
            }
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
        suspend fun create(loadRt: Boolean = false): RealRepoFixture {
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
                            deviceId = "doc-editor-custom-view-smoke",
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
                rtFfi = rtFfi,
                cameraPreviewFfi = cameraPreviewFfi,
                container = container,
            )
        }
    }
}

private const val NOTE_DISPLAY_HINT_KEY = "org.example.daybook.note"
private const val PLUG_TEST_ID = "@daybook/test"
private const val PLUG_TEST_SAMPLE_VIEW_KEY = "sample-summary-card"

private fun noteFacetKeyWithId(id: String): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), id)

private fun plugTestOciPath(): Path {
    var cursor: Path? = Path.of("").toAbsolutePath()
    while (cursor != null) {
        val candidate = cursor.resolve("target/oci/@daybook/test")
        if (Files.isDirectory(candidate)) {
            return candidate
        }
        cursor = cursor.parent
    }
    error(
        "Missing OCI plug artifact at target/oci/@daybook/test. Build it with: " +
            "cargo run -p xtask -- build-plug-oci --plug-root ./src/plug_test",
    )
}
