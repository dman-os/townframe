package org.example.daybook

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.requiredHeight
import androidx.compose.foundation.layout.requiredWidth
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertTextContains
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsNotFocused
import androidx.compose.ui.test.assertIsNotSelected
import androidx.compose.ui.test.assertIsSelected
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performCustomAccessibilityActionWithLabel
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTextClearance
import androidx.compose.ui.test.performScrollToNode
import androidx.compose.ui.test.performMouseInput
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.longClick
import androidx.compose.ui.test.v2.runComposeUiTest
import androidx.compose.ui.semantics.SemanticsProperties
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
import org.example.daybook.ui.decodeWellKnownFacet
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
import org.example.daybook.uniffi.types.DocPatch
import org.example.daybook.uniffi.types.FacetDisplayDeets
import org.example.daybook.uniffi.types.FacetDisplayHint
import org.example.daybook.uniffi.types.FacetKey
import org.example.daybook.uniffi.types.FacetTag
import org.example.daybook.uniffi.types.FacetViewMode
import org.example.daybook.uniffi.types.Note
import org.example.daybook.uniffi.types.ViewRef
import org.example.daybook.uniffi.types.WellKnownFacetTag
import org.example.daybook.uniffi.types.WellKnownFacet

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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitForIdle()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithText(titleText).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.SCREEN).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.EDITOR).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.DETAILS).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.TITLE_FIELD).assertTextContains(titleText)
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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                val labels = contentFacetLabels()
                labels.contains(firstNoteLabel) && labels.contains(secondNoteLabel)
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
    fun realRepo_block_details_dialog_updates_note_mime_and_persists() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val waitMillis = 60_000L
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val noteKey = noteFacetKey()
            val noteLabel = facetKeyString(noteKey)
            val docId = fixture.createDoc(
                titleText = "Block details smoke title",
                noteText = "Block details note",
            )
            fixture.setCoreNoteEditorConfig(
                """
                {
                  "mimeOptions": [
                    {
                      "mime": "text/x-test-note-config",
                      "label": "Config supplied format",
                      "description": "Loaded from the core plug config doc."
                    }
                  ]
                }
                """.trimIndent(),
            )

            fun openBlockActions(facetKeyLabel: String) {
                onNodeWithTag(DaybookEditorSemantics.facetRow(facetKeyLabel))
                    .performCustomAccessibilityActionWithLabel("Block actions")
            }

            fun currentNoteMime(): String? =
                docEditorStore.selectedController.value?.state?.value?.noteEditors
                    ?.get(noteKey)
                    ?.mime

            fun persistedNoteMime(): String? {
                val bundle = runBlocking { fixture.drawerRepo.getBundle(docId, "main") }
                val raw = bundle?.doc?.facets?.get(noteKey) ?: return null
                val decoded = decodeWellKnownFacet<WellKnownFacet.Note>(raw)
                return decoded.getOrNull()?.v1?.mime
            }

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(noteLabel)
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel)).performClick()

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_BLOCK_SECTION).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_SOURCE_SECTION).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsSourceFacetCard(noteLabel)).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Facet key"))
                .assertTextContains(noteLabel)
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Facet tag"))
                .assertTextContains("org.example.daybook.note")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Facet id"))
                .assertTextContains("main")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Source facet count"))
                .assertTextContains("1")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCurrentFormatSummary(noteLabel))
                .assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format label"))
                .assertTextContains("Plain text")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format description"))
                .assertTextContains("Basic plain text notes.")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Raw MIME"))
                .assertTextContains("text/plain")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Facet created"))
                .assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Facet last modified"))
                .assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Current MIME"))
                .assertTextContains("text/plain")
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                .performScrollToNode(hasText("Change format"))
            onNodeWithTag(DaybookEditorSemantics.blockDetailsChangeFormatAction(noteLabel)).performClick()

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_FORMAT_PICKER_SECTION)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_FORMAT_PICKER_SECTION).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsFormatSearchField(noteLabel))
                .performTextInput("Config supplied")
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(
                    DaybookEditorSemantics.blockDetailsFormatOption(noteLabel, "text/x-test-note-config"),
                ).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsFormatOption(noteLabel, "text/x-test-note-config"))
                .performClick()

            waitUntil(timeoutMillis = waitMillis) {
                currentNoteMime() == "text/x-test-note-config"
            }
            waitUntil(timeoutMillis = waitMillis) {
                persistedNoteMime() == "text/x-test-note-config"
            }
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format label"))
                    .fetchSemanticsNodes()
                    .firstOrNull()
                    ?.config
                    ?.get(SemanticsProperties.Text)
                    ?.any { it.text.contains("Config supplied format") } == true
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Raw MIME"))
                .assertTextContains("text/x-test-note-config")
            onNodeWithTag(DaybookEditorSemantics.noteField(noteLabel)).assertTextContains("Block details note")
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_details_dialog_accepts_custom_note_mime_and_persists() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val waitMillis = 60_000L
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val noteKey = noteFacetKey()
            val noteLabel = facetKeyString(noteKey)
            val docId = fixture.createDoc(
                titleText = "Block details custom MIME entry title",
                noteText = "Custom MIME entry note",
            )

            fun openBlockActions(facetKeyLabel: String) {
                onNodeWithTag(DaybookEditorSemantics.facetRow(facetKeyLabel))
                    .performCustomAccessibilityActionWithLabel("Block actions")
            }

            fun currentNoteMime(): String? =
                docEditorStore.selectedController.value?.state?.value?.noteEditors
                    ?.get(noteKey)
                    ?.mime

            fun persistedNoteMime(): String? {
                val bundle = runBlocking { fixture.drawerRepo.getBundle(docId, "main") }
                val raw = bundle?.doc?.facets?.get(noteKey) ?: return null
                val decoded = decodeWellKnownFacet<WellKnownFacet.Note>(raw)
                return decoded.getOrNull()?.v1?.mime
            }

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(noteLabel)
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel)).performClick()

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                .performScrollToNode(hasText("Change format"))
            onNodeWithTag(DaybookEditorSemantics.blockDetailsChangeFormatAction(noteLabel)).performClick()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithText("Choose note format")
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeAction(noteLabel)).performClick()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_CUSTOM_MIME_SECTION)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_CUSTOM_MIME_SECTION).assertIsDisplayed()

            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeInput(noteLabel))
                .performTextInput("bad mime")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeConfirmAction(noteLabel))
                .performClick()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeError(noteLabel))
                .assertIsDisplayed()

            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeInput(noteLabel))
                .performTextClearance()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeInput(noteLabel))
                .performTextInput("text/x-user-custom")
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCustomMimeConfirmAction(noteLabel))
                .performClick()

            waitUntil(timeoutMillis = waitMillis) {
                currentNoteMime() == "text/x-user-custom"
            }
            waitUntil(timeoutMillis = waitMillis) {
                persistedNoteMime() == "text/x-user-custom"
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCurrentFormatSummary(noteLabel))
                .assertIsDisplayed()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format label"))
                    .fetchSemanticsNodes()
                    .firstOrNull()
                    ?.config
                    ?.get(SemanticsProperties.Text)
                    ?.any { it.text.contains("Current custom format") } == true
            }
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format description"))
                    .fetchSemanticsNodes()
                    .firstOrNull()
                    ?.config
                    ?.get(SemanticsProperties.Text)
                    ?.any { it.text.contains("This note uses a MIME type not listed in note editor config.") } == true
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Raw MIME"))
                .assertTextContains("text/x-user-custom")

            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                .performScrollToNode(hasText("Change format"))
            onNodeWithTag(DaybookEditorSemantics.blockDetailsChangeFormatAction(noteLabel)).performClick()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(
                    DaybookEditorSemantics.blockDetailsFormatOption(noteLabel, "text/x-user-custom"),
                ).fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsFormatOption(noteLabel, "text/x-user-custom"))
                .assertIsSelected()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_details_dialog_preserves_unknown_current_mime() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val waitMillis = 60_000L
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val noteKey = noteFacetKey()
            val noteLabel = facetKeyString(noteKey)
            val docId = fixture.createDoc(
                titleText = "Block details custom mime title",
                noteText = "Custom mime note",
                extraRawFacets =
                    listOf(
                        noteKey to encodeWellKnownFacet(
                            WellKnownFacet.Note(
                                Note(
                                    mime = "application/x-weird-note",
                                    content = "Custom mime note",
                                ),
                            ),
                        ),
                    ),
            )

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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(noteLabel)
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsAction(noteLabel)).performClick()

            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsCurrentFormatSummary(noteLabel))
                .assertIsDisplayed()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format label"))
                    .fetchSemanticsNodes()
                    .firstOrNull()
                    ?.config
                    ?.get(SemanticsProperties.Text)
                    ?.any { it.text.contains("Current custom format") } == true
            }
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Format description"))
                    .fetchSemanticsNodes()
                    .firstOrNull()
                    ?.config
                    ?.get(SemanticsProperties.Text)
                    ?.any { it.text.contains("This note uses a MIME type not listed in note editor config.") } == true
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsMetadataValue("Raw MIME"))
                .assertTextContains("application/x-weird-note")
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                .performScrollToNode(hasText("Change format"))
            onNodeWithTag(DaybookEditorSemantics.blockDetailsChangeFormatAction(noteLabel)).performClick()
            waitUntil(timeoutMillis = waitMillis) {
                onAllNodesWithText("Choose note format")
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsFormatOption(noteLabel, "application/x-weird-note"))
                .assertIsSelected()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_can_add_block_below_using_picker() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Add block smoke title",
                noteText = "Initial note block",
            )

            fun contentFacetViews() =
                docEditorStore.selectedController.value?.state?.value?.contentFacetViews.orEmpty()

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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(firstNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.addBlockAfterAction(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.addBlockAfterAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.ADD_BLOCK_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.ADD_BLOCK_SEARCH_FIELD).performTextInput("note")
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.addBlockOption("note"))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.addBlockOption("note")).performClick()

            waitUntil(timeoutMillis = 10_000) {
                contentFacetViews().size == 2
            }
            val newFacetKey = contentFacetViews().last().facetKey
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(facetKeyString(newFacetKey)))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.noteField(facetKeyString(newFacetKey))).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_can_add_block_below_using_picker_in_narrow_layout() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Add block dock smoke title",
                noteText = "Initial note block",
            )

            fun contentFacetViews() =
                docEditorStore.selectedController.value?.state?.value?.contentFacetViews.orEmpty()

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
                            BigDialogHost(narrowScreen = true) {
                                Box(
                                    modifier =
                                    Modifier.requiredWidth(540.dp).requiredHeight(800.dp),
                                ) {
                                    DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                                }
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(firstNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.addBlockAfterAction(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.addBlockAfterAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.ADD_BLOCK_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.ADD_BLOCK_DIALOG).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.ADD_BLOCK_SEARCH_FIELD).assertIsNotFocused()
            onNodeWithTag(DaybookEditorSemantics.ADD_BLOCK_SEARCH_FIELD).performTextInput("note")
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.addBlockOption("note"))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.addBlockOption("note")).performClick()

            waitUntil(timeoutMillis = 10_000) {
                contentFacetViews().size == 2
            }
            val newFacetKey = contentFacetViews().last().facetKey
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(facetKeyString(newFacetKey)))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.noteField(facetKeyString(newFacetKey))).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_narrow_note_focus_does_not_show_action_bar_without_ime() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Focused note gate smoke title",
                noteText = "Initial note block",
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = true) {
                                Box(
                                    modifier =
                                    Modifier.requiredWidth(540.dp).requiredHeight(800.dp),
                                ) {
                                    DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                                }
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.noteField(firstNoteLabel)).performTextInput("x")
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.FOCUSED_NOTE_ACCESSORY_BAR)
                    .fetchSemanticsNodes()
                    .isEmpty()
            }
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_can_collapse_and_expand() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Collapse smoke title",
                noteText = "Collapsed note preview",
            )

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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            openBlockActions(firstNoteLabel)
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.toggleBlockCollapseAction(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.toggleBlockCollapseAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(
                    testTag = DaybookEditorSemantics.collapsedFacetBlock(firstNoteLabel),
                    useUnmergedTree = true,
                )
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(
                testTag = DaybookEditorSemantics.collapsedFacetBlock(firstNoteLabel),
                useUnmergedTree = true,
            ).assertIsDisplayed()
            onNodeWithText("Collapsed note preview").assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockActions(firstNoteLabel)).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performCustomAccessibilityActionWithLabel("Expand block")

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.noteField(firstNoteLabel)).assertIsDisplayed()
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.collapsedFacetBlock(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isEmpty()
            }
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_long_press_selects_and_tap_clears_selection() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Selection smoke title",
                noteText = "Selection note",
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performTouchInput {
                    longClick()
                }

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel)).assertIsSelected()
            onNodeWithTag(DaybookEditorSemantics.SELECTION_CANCEL_ACTION).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.SELECTION_SELECT_ALL_ACTION).assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR).assertIsDisplayed()

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR)
                    .fetchSemanticsNodes()
                    .isEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel)).assertIsNotSelected()
            onAllNodesWithTag(DaybookEditorSemantics.SELECTION_CANCEL_ACTION)
                .fetchSemanticsNodes()
                .isEmpty()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_shell_long_press_single_selection_exposes_details_action() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Selection details smoke title",
                noteText = "Selection details note",
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performTouchInput {
                    longClick()
                }

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockDetailsAction(firstNoteLabel))
                .assertIsDisplayed()
            onNodeWithTag(DaybookEditorSemantics.blockDetailsAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.BLOCK_DETAILS_DIALOG).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_handle_quick_select_action_enters_selection_mode() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Quick select smoke title",
                noteText = "Quick select note",
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performMouseInput {
                    moveTo(topRight)
                }
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.blockActions(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.blockActions(firstNoteLabel))
                .performMouseInput {
                    moveTo(center)
                }
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.selectBlockQuickAction(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.selectBlockQuickAction(firstNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel)).assertIsSelected()
            onNodeWithTag(DaybookEditorSemantics.SELECTION_CANCEL_ACTION).assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_menu_sheet_single_selection_collapse_action_works() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val firstNoteLabel = facetKeyString(noteFacetKey())
            val docId = fixture.createDoc(
                titleText = "Single selection menu title",
                noteText = "Collapsed note preview",
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performTouchInput {
                    longClick()
                }
            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.selectionActionBarAction("toggle-collapse"))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.selectionActionBarAction("toggle-collapse")).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(
                    testTag = DaybookEditorSemantics.collapsedFacetBlock(firstNoteLabel),
                    useUnmergedTree = true,
                )
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(
                testTag = DaybookEditorSemantics.collapsedFacetBlock(firstNoteLabel),
                useUnmergedTree = true,
            ).assertIsDisplayed()
            onNodeWithText("Collapsed note preview").assertIsDisplayed()
            onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                .fetchSemanticsNodes()
                .isEmpty()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realRepo_block_menu_sheet_multi_selection_only_shows_collapse_action() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create() }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            val secondNoteKey = noteFacetKeyWithId("second")
            val firstNoteLabel = facetKeyString(noteFacetKey())
            val secondNoteLabel = facetKeyString(secondNoteKey)
            val docId = fixture.createDoc(
                titleText = "Multi selection menu title",
                noteText = "First note block",
                extraNotes = listOf(secondNoteKey to "Second note block"),
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.noteField(firstNoteLabel))
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel))
                .performTouchInput {
                    longClick()
                }
            onNodeWithTag(DaybookEditorSemantics.facetRow(secondNoteLabel)).performClick()

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithText("2 selected")
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }

            waitUntil(timeoutMillis = 10_000) {
                onAllNodesWithTag(DaybookEditorSemantics.BLOCK_SELECTION_ACTION_BAR)
                    .fetchSemanticsNodes()
                    .isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.selectionActionBarAction("collapse-selected")).assertIsDisplayed()
            onAllNodesWithTag(DaybookEditorSemantics.selectionActionBarAction("add-below"))
                .fetchSemanticsNodes()
                .isEmpty()
            onAllNodesWithTag(DaybookEditorSemantics.selectionActionBarAction("make-primary"))
                .fetchSemanticsNodes()
                .isEmpty()
            onAllNodesWithTag(DaybookEditorSemantics.selectionActionBarAction("move-up"))
                .fetchSemanticsNodes()
                .isEmpty()
            onAllNodesWithTag(DaybookEditorSemantics.selectionActionBarAction("move-down"))
                .fetchSemanticsNodes()
                .isEmpty()
            onAllNodesWithTag(DaybookEditorSemantics.blockDetailsAction(firstNoteLabel))
                .fetchSemanticsNodes()
                .isEmpty()
            onNodeWithTag(DaybookEditorSemantics.facetRow(firstNoteLabel)).assertIsSelected()
            onNodeWithTag(DaybookEditorSemantics.facetRow(secondNoteLabel)).assertIsSelected()
            onNodeWithText("2 selected").assertIsDisplayed()
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
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 20_000) {
                onAllNodesWithText("Sample summary").fetchSemanticsNodes().isNotEmpty()
            }
            onNodeWithTag(DaybookEditorSemantics.pluginFacet(facetKeyString(noteFacetKey()))).assertIsDisplayed()
            onNodeWithTag(DaybookViewSemantics.ROOT).assertIsDisplayed()
            onNodeWithText("Sample summary").assertIsDisplayed()
        } finally {
            fixture.close()
        }
    }

    @Test
    fun realFfi_renders_dayledger_custom_view_facet_in_doc_editor_screen() = runComposeUiTest {
        val fixture = runBlocking { RealRepoFixture.create(loadRt = true) }
        try {
            val drawerVm = DrawerViewModel(fixture.drawerRepo)
            val docEditorStore = DocEditorStoreViewModel(fixture.drawerRepo)

            fixture.importDayledgerOci()
            fixture.setLedgerMetaCustomViewHint()

            val ledgerMetaKey = ledgerMetaFacetKey()
            val docId = fixture.createDoc(
                titleText = "Ledger smoke title",
                noteText = "Ledger note facet",
                extraRawFacets = listOf(ledgerMetaKey to ledgerMetaFacetJson()),
            )

            setContent {
                DaybookTheme(themeConfig = ThemeConfig.Light) {
                    ProvideScreenChromeSpec(ScreenChromeSpec()) {
                        CompositionLocalProvider(
                            LocalContainer provides fixture.container,
                            LocalDrawerViewModel provides drawerVm,
                            LocalDocEditorStore provides docEditorStore,
                        ) {
                            BigDialogHost(narrowScreen = false) {
                                DocEditorScreen(contentType = DaybookContentType.LIST_ONLY)
                            }
                        }
                    }
                }
            }

            docEditorStore.selectDoc(docId)

            waitUntil(timeoutMillis = 20_000) {
                onAllNodesWithText("Ledger ID: ledger-1").fetchSemanticsNodes().isNotEmpty()
            }
            onAllNodesWithTag(DaybookEditorSemantics.pluginFacet(facetKeyString(ledgerMetaKey)))
                .fetchSemanticsNodes()
                .isNotEmpty()
            onAllNodesWithTag(DaybookViewSemantics.ROOT).fetchSemanticsNodes().isNotEmpty()
            onAllNodesWithTag(DaybookViewSemantics.kind("card")).fetchSemanticsNodes().isNotEmpty()
            onAllNodesWithTag(DaybookViewSemantics.kind("section")).fetchSemanticsNodes().isNotEmpty()
            onAllNodesWithTag(DaybookViewSemantics.kind("list")).fetchSemanticsNodes().isNotEmpty()
            onNodeWithText("Ledger Overview").assertIsDisplayed()
            onNodeWithText("Ledger ID: ledger-1").assertIsDisplayed()
            onNodeWithText("Journal commodity: USD").assertIsDisplayed()
            onNodeWithText("Account refs: 2").assertIsDisplayed()
            onNodeWithText("Transaction refs: 1").assertIsDisplayed()
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
        extraRawFacets: List<Pair<FacetKey, String>> = emptyList(),
    ): String = runBlocking(Dispatchers.IO) {
        val facets =
            mutableMapOf(
                titleFacetKey() to encodeJsonString(titleText),
                noteFacetKey() to encodeWellKnownFacet(buildNoteFacet(noteText)),
            )
        for ((facetKey, content) in extraNotes) {
            facets[facetKey] = encodeWellKnownFacet(buildNoteFacet(content))
        }
        for ((facetKey, content) in extraRawFacets) {
            facets[facetKey] = content
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

    fun importDayledgerOci() = runBlocking(Dispatchers.IO) {
        plugsRepo.importFromOciLayout(dayledgerOciPath().toString())
    }

    fun setCoreNoteEditorConfig(configJson: String) = runBlocking(Dispatchers.IO) {
        val configDocId = drawerRepo.getOrInitPlugConfigDocId("@daybook/core")
        val configFacetKey =
            FacetKey(
                FacetTag.Any("org.example.daybook.note-editor-config"),
                "main",
            )
        val bundle = drawerRepo.getBundle(configDocId, "main")
        drawerRepo.update(
            DocPatch(
                id = configDocId,
                facetsSet =
                    mapOf(
                        configFacetKey to configJson,
                    ),
                facetsRemove = emptyList(),
                userPath = null,
            ),
            branchPath = "main",
            heads = bundle?.branchHeads,
        )
        val saved = drawerRepo.get(configDocId, "main")
        check(saved?.facets?.get(configFacetKey)?.contains("text/x-test-note-config") == true) {
            "Failed to write core note editor config facet"
        }
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

    fun setLedgerMetaCustomViewHint() = runBlocking(Dispatchers.IO) {
        configRepo.setFacetDisplayHint(
            LEDGER_META_DISPLAY_HINT_KEY,
            FacetDisplayHint(
                alwaysVisible = true,
                displayTitle = "Ledger overview",
                deets =
                FacetDisplayDeets.CustomView(
                    view = ViewRef(plugId = DAYLEDGER_PLUG_ID, viewKey = LEDGER_META_VIEW_KEY),
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
private const val LEDGER_META_DISPLAY_HINT_KEY = "org.example.dayledger.meta"
private const val DAYLEDGER_PLUG_ID = "@daybook/dayledger"
private const val LEDGER_META_VIEW_KEY = "ledger-meta"

private fun noteFacetKeyWithId(id: String): FacetKey =
    FacetKey(FacetTag.WellKnown(WellKnownFacetTag.NOTE), id)

private fun ledgerMetaFacetKey(): FacetKey = FacetKey(FacetTag.Any(LEDGER_META_DISPLAY_HINT_KEY), "main")

private fun ledgerMetaFacetJson(): String =
    """
        {
          "ledgerId": "ledger-1",
          "title": "Ledger Overview",
          "journalCommodity": "USD",
          "accountRefs": [
            "db+facet:///doc/assets/main",
            "db+facet:///doc/income/main"
          ],
          "transactionRefs": [
            "db+facet:///doc/txn-1/main"
          ]
        }
    """.trimIndent()

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

private fun dayledgerOciPath(): Path {
    var cursor: Path? = Path.of("").toAbsolutePath()
    while (cursor != null) {
        val candidate = cursor.resolve("target/oci/@daybook/dayledger")
        if (Files.isDirectory(candidate)) {
            return candidate
        }
        cursor = cursor.parent
    }
    error(
        "Missing OCI plug artifact at target/oci/@daybook/dayledger. Build it with: " +
            "cargo x build-plug-oci --plug-root ./src/plug_dayledger",
    )
}
