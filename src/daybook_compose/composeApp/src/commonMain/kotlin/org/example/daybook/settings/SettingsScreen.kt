@file:Suppress("FunctionNaming")

package org.example.daybook.settings

import androidx.compose.animation.AnimatedContent
import androidx.compose.animation.ContentTransform
import androidx.compose.animation.EnterTransition
import androidx.compose.animation.ExitTransition
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Extension
import androidx.compose.material.icons.filled.FolderOpen
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedCard
import androidx.compose.material3.OutlinedTextField
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
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavBackStack
import androidx.navigation3.runtime.NavEntry
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.NavMetadataKey
import androidx.navigation3.runtime.contains
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.metadata
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.navigation3.runtime.rememberSaveableStateHolderNavEntryDecorator
import androidx.navigation3.scene.Scene
import androidx.navigation3.scene.SceneStrategy
import androidx.navigation3.scene.SceneStrategyScope
import androidx.navigation3.ui.NavDisplay
import androidx.savedstate.serialization.SavedStateConfiguration
import io.github.vinceglb.filekit.dialogs.compose.rememberDirectoryPickerLauncher
import io.github.vinceglb.filekit.path
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalBigDialogController
import org.example.daybook.LocalContainer
import org.example.daybook.MltoolsBackendRow
import org.example.daybook.MltoolsProvisionState
import org.example.daybook.layouts.DaybookScaffold
import org.example.daybook.layouts.LocalScreenChromeSpec
import org.example.daybook.layouts.ScreenChromeSpec
import org.example.daybook.progress.ProgressAmountBlock
import org.example.daybook.uniffi.FfiException
import org.example.daybook.uniffi.PlugSummary
import org.example.daybook.uniffi.PlugsRepoFfi
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets

@Serializable
private enum class SettingsSection(val id: String, val title: String, val subtitle: String) {
    PLUGS(
        id = "plugs",
        title = "Plugs",
        subtitle = "Installed plugs and import flow",
    ),
    MLTOOLS(
        id = "mltools",
        title = "MLTools",
        subtitle = "Provisioning and model download state",
    ),
}

private fun sectionIcon(section: SettingsSection) = when (section) {
    SettingsSection.PLUGS -> Icons.Default.Extension
    SettingsSection.MLTOOLS -> Icons.Default.Settings
}

@Serializable
private sealed interface SettingsNavKey : NavKey {
    @Serializable
    data class SectionList(val lastSection: SettingsSection = SettingsSection.PLUGS) : SettingsNavKey

    @Serializable
    data class SectionDetail(val section: SettingsSection) : SettingsNavKey
}

private val settingsNavConfig =
    SavedStateConfiguration {
        serializersModule =
            SerializersModule {
                polymorphic(NavKey::class) {
                    subclass(SettingsNavKey.SectionList::class, SettingsNavKey.SectionList.serializer())
                    subclass(SettingsNavKey.SectionDetail::class, SettingsNavKey.SectionDetail.serializer())
                }
            }
    }

private const val SETTINGS_LIST_PANE_WEIGHT = 0.26f
private const val SETTINGS_DETAIL_PANE_WEIGHT = 0.74f

@Suppress("LongMethod")
@Composable
fun SettingsScreen(modifier: Modifier = Modifier) {
    val container = LocalContainer.current
    val configVm = viewModel { ConfigViewModel(container.configRepo, container.progressRepo) }
    val backStack = rememberNavBackStack(settingsNavConfig, SettingsNavKey.SectionList())

    val mltoolsConfig by configVm.mltoolsConfig.collectAsState()
    val provisionState by configVm.mltoolsProvisionState.collectAsState()
    val downloadTasks by configVm.mltoolsDownloadTasks.collectAsState()
    val configError by configVm.error.collectAsState()

    BoxWithConstraints(
        modifier =
        modifier
            .fillMaxSize()
            .testTag(SettingsScreenSemantics.ROOT),
    ) {
        val wideLayout = maxWidth >= 960.dp
        val currentRoute = backStack.currentSettingsRoute()
        val currentSection = backStack.currentSettingsSection()
        val parentTopBar = LocalScreenChromeSpec.current.topBar
        val showBack =
            when {
                wideLayout -> parentTopBar.showBack
                currentRoute is SettingsNavKey.SectionDetail -> true
                else -> parentTopBar.showBack
            }
        val onBack =
            when {
                wideLayout -> parentTopBar.onBack

                currentRoute is SettingsNavKey.SectionDetail -> {
                    {
                        backStack.removeLastOrNull()
                        Unit
                    }
                }

                else -> parentTopBar.onBack
            }
        val topBarTitle =
            if (wideLayout) {
                "Settings"
            } else if (currentRoute is SettingsNavKey.SectionDetail) {
                currentSection.title
            } else {
                "Settings"
            }

        LaunchedEffect(wideLayout) {
            if (wideLayout) {
                backStack.ensureSettingsWideDetail()
            }
        }

        DaybookScaffold(
            modifier = Modifier.fillMaxSize(),
            topBar =
            ScreenChromeSpec.TopBarSpec(
                title = topBarTitle,
                showBack = showBack,
                onBack = onBack,
                navigationIconContentDescription = "Back to settings",
                navigationIconTestTag = SettingsScreenSemantics.SETTINGS_BACK_BUTTON,
                pinned = true,
            ),
        ) { scaffoldPadding ->
            val listDetailSceneStrategy = rememberSettingsListDetailSceneStrategy(wideLayout)

            NavDisplay(
                backStack = backStack,
                onBack = { backStack.removeLastOrNull() },
                sceneStrategies = listOf(listDetailSceneStrategy),
                entryDecorators = listOf(
                    rememberSaveableStateHolderNavEntryDecorator(),
                    rememberViewModelStoreNavEntryDecorator(),
                ),
                modifier =
                Modifier
                    .fillMaxSize()
                    .padding(scaffoldPadding),
                transitionSpec = { ContentTransform(EnterTransition.None, ExitTransition.None) },
                popTransitionSpec = { ContentTransform(EnterTransition.None, ExitTransition.None) },
                predictivePopTransitionSpec = {
                    ContentTransform(EnterTransition.None, ExitTransition.None)
                },
                entryProvider = entryProvider {
                    entry<SettingsNavKey.SectionList>(
                        metadata = SettingsListDetailScene.listPane(),
                    ) {
                        SettingsSectionList(
                            modifier = Modifier.fillMaxSize(),
                            wideLayout = wideLayout,
                            onSectionSelected = { section ->
                                backStack.showSettingsSection(section)
                            },
                        )
                    }

                    entry<SettingsNavKey.SectionDetail>(
                        metadata = SettingsListDetailScene.detailPane(),
                    ) { route ->
                        SettingsDetailPane(
                            SettingsDetailPaneState(
                                section = route.section,
                                wideLayout = wideLayout,
                                configVm = configVm,
                                mltoolsConfig = mltoolsConfig,
                                provisionState = provisionState,
                                downloadTasks = downloadTasks,
                                configError = configError,
                                plugsRepo = container.plugsRepo,
                            ),
                        )
                    }
                },
            )
        }
    }
}

private fun NavBackStack<NavKey>.currentSettingsRoute(): SettingsNavKey =
    lastOrNull() as? SettingsNavKey ?: SettingsNavKey.SectionList()

private fun NavBackStack<NavKey>.currentSettingsSection(): SettingsSection =
    when (val currentRoute = currentSettingsRoute()) {
        is SettingsNavKey.SectionDetail -> currentRoute.section
        is SettingsNavKey.SectionList -> currentRoute.lastSection
    }

private fun NavBackStack<NavKey>.showSettingsSection(section: SettingsSection) {
    val listIndex = indexOfLast { it is SettingsNavKey.SectionList }
    if (listIndex >= 0) {
        this[listIndex] = SettingsNavKey.SectionList(lastSection = section)
    } else {
        add(SettingsNavKey.SectionList(lastSection = section))
    }
    removeAll { it is SettingsNavKey.SectionDetail }
    add(SettingsNavKey.SectionDetail(section = section))
}

private fun NavBackStack<NavKey>.ensureSettingsWideDetail() {
    if (lastOrNull() !is SettingsNavKey.SectionDetail) {
        showSettingsSection(currentSettingsSection())
    }
}

private class SettingsListDetailScene<T : Any>(
    override val key: Any,
    override val previousEntries: List<NavEntry<T>>,
    val listEntry: NavEntry<T>,
    val detailEntry: NavEntry<T>,
) : Scene<T> {
    override val entries: List<NavEntry<T>> = listOf(listEntry, detailEntry)

    override val content: @Composable (() -> Unit) = {
        Row(modifier = Modifier.fillMaxSize()) {
            Column(
                modifier =
                Modifier
                    .weight(SETTINGS_LIST_PANE_WEIGHT)
                    .widthIn(max = 280.dp),
            ) {
                listEntry.Content()
            }
            Column(modifier = Modifier.weight(SETTINGS_DETAIL_PANE_WEIGHT)) {
                AnimatedContent(
                    targetState = detailEntry,
                    contentKey = { entry -> entry.contentKey },
                    transitionSpec = {
                        slideInHorizontally(initialOffsetX = { it }) togetherWith
                            slideOutHorizontally(targetOffsetX = { -it })
                    },
                ) { entry ->
                    entry.Content()
                }
            }
        }
    }

    companion object {
        fun listPane() = metadata {
            put(ListKey, true)
        }

        fun detailPane() = metadata {
            put(DetailKey, true)
        }
    }

    object ListKey : NavMetadataKey<Boolean>
    object DetailKey : NavMetadataKey<Boolean>
}

@Composable
private fun rememberSettingsListDetailSceneStrategy(wideLayout: Boolean): SettingsListDetailSceneStrategy<NavKey> =
    remember(wideLayout) {
        SettingsListDetailSceneStrategy(wideLayout)
    }

private class SettingsListDetailSceneStrategy<T : Any>(private val wideLayout: Boolean) : SceneStrategy<T> {
    override fun SceneStrategyScope<T>.calculateScene(entries: List<NavEntry<T>>): Scene<T>? {
        if (!wideLayout) return null
        val detailEntry =
            entries.lastOrNull()?.takeIf { it.metadata.contains(SettingsListDetailScene.DetailKey) }
        val listEntry =
            entries.findLast { it.metadata.contains(SettingsListDetailScene.ListKey) }
        return if (detailEntry != null && listEntry != null) {
            SettingsListDetailScene(
                key = listEntry.contentKey,
                previousEntries = entries.dropLast(1),
                listEntry = listEntry,
                detailEntry = detailEntry,
            )
        } else {
            null
        }
    }
}

@Composable
private fun SettingsSectionList(
    modifier: Modifier = Modifier,
    wideLayout: Boolean,
    onSectionSelected: (SettingsSection) -> Unit,
) {
    Column(
        modifier =
        modifier
            .testTag(SettingsScreenSemantics.SECTION_LIST)
            .padding(
                start = if (wideLayout) 12.dp else 16.dp,
                top = 16.dp,
                end = if (wideLayout) 12.dp else 16.dp,
                bottom = 16.dp,
            ),
        verticalArrangement = if (wideLayout) Arrangement.spacedBy(4.dp) else Arrangement.spacedBy(12.dp),
    ) {
        SettingsSection.entries.forEach { section ->
            if (wideLayout) {
                SettingsSectionSidebarButton(
                    section = section,
                    onClick = { onSectionSelected(section) },
                )
            } else {
                SettingsSectionCard(
                    section = section,
                    onClick = { onSectionSelected(section) },
                )
            }
        }
    }
}

@Composable
private fun SettingsSectionSidebarButton(section: SettingsSection, onClick: () -> Unit) {
    TextButton(
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(SettingsScreenSemantics.sectionItem(section.id)),
        onClick = onClick,
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.Start,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Icon(
                imageVector = sectionIcon(section),
                contentDescription = null,
            )
            Spacer(modifier = Modifier.width(12.dp))
            Text(
                text = section.title,
                // style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Medium,
            )
        }
    }
}

@Composable
private fun SettingsSectionCard(section: SettingsSection, onClick: () -> Unit) {
    ListItem(
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(SettingsScreenSemantics.sectionItem(section.id))
            .clickable(role = Role.Button, onClick = onClick),
        headlineContent = {
            Text(
                text = section.title,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold,
            )
        },
        supportingContent = {
            Text(
                text = section.subtitle,
                style = MaterialTheme.typography.bodyMedium,
            )
        },
        leadingContent = {
            Icon(
                imageVector = sectionIcon(section),
                contentDescription = null,
            )
        },
    )
}

private data class SettingsDetailPaneState(
    val section: SettingsSection,
    val wideLayout: Boolean,
    val configVm: ConfigViewModel,
    val mltoolsConfig: org.example.daybook.MltoolsConfigSummary,
    val provisionState: MltoolsProvisionState,
    val downloadTasks: List<ProgressTask>,
    val configError: org.example.daybook.ConfigError?,
    val plugsRepo: PlugsRepoFfi,
)

@Composable
private fun SettingsDetailPane(state: SettingsDetailPaneState) {
    AnimatedContent<SettingsSection>(
        modifier = Modifier.fillMaxSize(),
        targetState = state.section,
        transitionSpec = {
            slideInHorizontally { width -> width } togetherWith
                slideOutHorizontally { width -> -width }
        },
        label = "settings-section-detail",
    ) { currentSection ->
        when (currentSection) {
            SettingsSection.PLUGS -> {
                PlugsSettingsPane(
                    wideLayout = state.wideLayout,
                    plugsRepo = state.plugsRepo,
                )
            }

            SettingsSection.MLTOOLS -> {
                MltoolsSettingsPane(
                    wideLayout = state.wideLayout,
                    configVm = state.configVm,
                    mltoolsConfig = state.mltoolsConfig,
                    provisionState = state.provisionState,
                    downloadTasks = state.downloadTasks,
                    configError = state.configError,
                )
            }
        }
    }
}

@Suppress("LongMethod")
@Composable
private fun PlugsSettingsPane(wideLayout: Boolean, plugsRepo: PlugsRepoFfi) {
    var reloadToken by rememberSaveable { mutableIntStateOf(0) }
    val plugsState = rememberInstalledPlugsState(plugsRepo, reloadToken)
    val bigDialogController = LocalBigDialogController.current

    Column(
        modifier =
        Modifier
            .fillMaxSize()
            .padding(24.dp)
            .testTag(SettingsScreenSemantics.sectionDetail(SettingsSection.PLUGS.id))
            .verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        if (wideLayout) {
            SettingsSectionInlineHeading(section = SettingsSection.PLUGS)
        }

        SettingsActionRow(
            SettingsActionRowState(
                description = "Import a plug package into Daybook.",
                actionLabel = "Add plug",
                actionTestTag = SettingsScreenSemantics.PLUGS_ADD_BUTTON,
                onAction = {
                    bigDialogController.show {
                        ImportPlugWizardDialog(
                            plugsRepo = plugsRepo,
                            onClose = bigDialogController::dismiss,
                            onImportSuccess = { reloadToken += 1 },
                        )
                    }
                },
            ),
        )

        when (plugsState) {
            is InstalledPlugsState.Loading -> {
                LoadingState(text = "Loading installed plugs…")
            }

            is InstalledPlugsState.Error -> {
                ErrorStateCard(
                    message = (plugsState as InstalledPlugsState.Error).message,
                    actionLabel = "Retry",
                    onAction = { reloadToken += 1 },
                )
            }

            is InstalledPlugsState.Ready -> {
                val plugs = (plugsState as InstalledPlugsState.Ready).plugs
                if (plugs.isEmpty()) {
                    EmptyStateCard(
                        title = "No installed plugs",
                        message = "The repo currently has no discovered plugs.",
                    )
                } else {
                    Text(
                        text = "${plugs.size} installed plugs",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    Column(
                        modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_LIST),
                        verticalArrangement = Arrangement.spacedBy(12.dp),
                    ) {
                        plugs.forEach { plug ->
                            PlugSummaryCard(plug = plug)
                        }
                    }
                }
            }
        }
    }
}

@Suppress("LongMethod", "LongParameterList")
@Composable
private fun MltoolsSettingsPane(
    wideLayout: Boolean,
    configVm: ConfigViewModel,
    mltoolsConfig: org.example.daybook.MltoolsConfigSummary,
    provisionState: MltoolsProvisionState,
    downloadTasks: List<ProgressTask>,
    configError: org.example.daybook.ConfigError?,
) {
    Column(
        modifier =
        Modifier
            .fillMaxSize()
            .padding(24.dp)
            .testTag(SettingsScreenSemantics.sectionDetail(SettingsSection.MLTOOLS.id))
            .verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(20.dp),
    ) {
        if (wideLayout) {
            SettingsSectionInlineHeading(section = SettingsSection.MLTOOLS)
        }

        SettingsActionRow(
            SettingsActionRowState(
                description = "Provision the default OCR, embed, and LLM stack.",
                actionLabel =
                when (provisionState) {
                    is MltoolsProvisionState.Failed -> "Retry provision"
                    is MltoolsProvisionState.Running -> "Provisioning…"
                    else -> "Provision"
                },
                actionTestTag = SettingsScreenSemantics.MLTOOLS_PROVISION_BUTTON,
                enabled = provisionState !is MltoolsProvisionState.Running,
                onAction = { configVm.provisionMobileDefaultMltools() },
                showProgress = provisionState is MltoolsProvisionState.Running,
            ),
        )

        configError?.let {
            ErrorStateCard(
                message = it.message,
                actionLabel = "Dismiss",
                onAction = { configVm.clearError() },
            )
        }

        StatusCard(
            title = "Provision status",
            status = when (val state = provisionState) {
                is MltoolsProvisionState.Idle -> "Ready to provision"
                is MltoolsProvisionState.Running -> "Downloading and configuring models"
                is MltoolsProvisionState.Succeeded -> "Provisioned successfully"
                is MltoolsProvisionState.Failed -> "Failed: ${state.message}"
            },
            showProgress = provisionState is MltoolsProvisionState.Running,
            modifier = Modifier.testTag(SettingsScreenSemantics.MLTOOLS_STATUS),
        )

        BackendSection(
            title = "OCR backends",
            rows = mltoolsConfig.ocr,
        )
        BackendSection(
            title = "Embed backends",
            rows = mltoolsConfig.embed,
        )
        BackendSection(
            title = "LLM backends",
            rows = mltoolsConfig.llm,
        )

        Text(
            text = "MLTools download tasks",
            style = MaterialTheme.typography.titleMedium,
            fontWeight = FontWeight.SemiBold,
            modifier =
            Modifier
                .testTag(SettingsScreenSemantics.MLTOOLS_DOWNLOAD_TASKS)
                .semantics { heading() },
        )

        if (downloadTasks.isEmpty()) {
            EmptyStateCard(
                title = "No model download tasks",
                message = "Provisioning has not created any active download work yet.",
            )
        } else {
            Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
                downloadTasks.forEach { task ->
                    MltoolsDownloadTaskRow(task)
                }
            }
        }
    }
}

@Composable
private fun SettingsSectionInlineHeading(section: SettingsSection) {
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        Text(
            text = section.title,
            style = MaterialTheme.typography.headlineSmall,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.semantics { heading() },
        )
        Text(
            text = section.subtitle,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

private data class SettingsActionRowState(
    val description: String,
    val actionLabel: String,
    val actionTestTag: String,
    val onAction: () -> Unit,
    val enabled: Boolean = true,
    val showProgress: Boolean = false,
)

@Composable
private fun SettingsActionRow(state: SettingsActionRowState) {
    OutlinedCard(modifier = Modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier.padding(16.dp),
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Column(
                modifier = Modifier.weight(1f),
                verticalArrangement = Arrangement.spacedBy(4.dp),
            ) {
                Text(
                    text = state.description,
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurface,
                )
            }
            Button(
                onClick = state.onAction,
                enabled = state.enabled,
                modifier = Modifier.testTag(state.actionTestTag),
            ) {
                if (state.showProgress) {
                    CircularProgressIndicator(
                        modifier = Modifier
                            .padding(end = 8.dp)
                            .height(16.dp)
                            .width(16.dp),
                        strokeWidth = 2.dp,
                    )
                }
                Text(state.actionLabel)
            }
        }
    }
}

@Composable
private fun PlugSummaryCard(plug: PlugSummary) {
    OutlinedCard(
        modifier = Modifier
            .fillMaxWidth()
            .testTag(SettingsScreenSemantics.plugRow(plug.id)),
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(
                    text = plug.title,
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.SemiBold,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                Text(
                    text = "${plug.namespace}/${plug.name} · ${plug.version}",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            if (plug.desc.isNotBlank()) {
                Text(
                    text = plug.desc,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            Text(
                text = buildPlugCountsText(plug),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun BackendSection(title: String, rows: List<MltoolsBackendRow>) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            text = title,
            style = MaterialTheme.typography.titleSmall,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.semantics { heading() },
        )

        if (rows.isEmpty()) {
            Text(
                text = "Not configured",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        } else {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                rows.forEach { row ->
                    OutlinedCard(modifier = Modifier.fillMaxWidth()) {
                        Row(
                            modifier = Modifier.padding(16.dp),
                            horizontalArrangement = Arrangement.spacedBy(12.dp),
                            verticalAlignment = Alignment.Top,
                        ) {
                            Text(
                                text = row.backend,
                                style = MaterialTheme.typography.bodyMedium,
                                fontWeight = FontWeight.SemiBold,
                                modifier = Modifier.widthIn(min = 96.dp, max = 176.dp),
                            )
                            Text(
                                text = row.details,
                                style = MaterialTheme.typography.bodyMedium,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.weight(1f),
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun StatusCard(title: String, status: String, showProgress: Boolean, modifier: Modifier = Modifier) {
    OutlinedCard(modifier = modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier.padding(16.dp),
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text(
                    text = title,
                    style = MaterialTheme.typography.titleSmall,
                    fontWeight = FontWeight.SemiBold,
                )
                Text(
                    text = status,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            if (showProgress) {
                CircularProgressIndicator(modifier = Modifier.height(24.dp).width(24.dp), strokeWidth = 2.dp)
            }
        }
    }
}

@Composable
private fun ErrorStateCard(message: String, actionLabel: String, onAction: () -> Unit) {
    OutlinedCard(
        modifier = Modifier.fillMaxWidth(),
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Text(
                text = "Error",
                style = MaterialTheme.typography.titleSmall,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.error,
            )
            Text(
                text = message,
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            TextButton(onClick = onAction) {
                Text(actionLabel)
            }
        }
    }
}

@Composable
private fun EmptyStateCard(title: String, message: String) {
    OutlinedCard(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            Text(
                text = title,
                style = MaterialTheme.typography.titleSmall,
                fontWeight = FontWeight.SemiBold,
            )
            Text(
                text = message,
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun LoadingState(text: String) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(12.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        CircularProgressIndicator(modifier = Modifier.height(18.dp).width(18.dp), strokeWidth = 2.dp)
        Text(
            text = text,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

@Composable
private fun MltoolsDownloadTaskRow(task: ProgressTask) {
    val latest = task.latestUpdate?.update?.deets
    val stateText =
        when (task.state) {
            ProgressTaskState.ACTIVE -> "Active"
            ProgressTaskState.SUCCEEDED -> "Succeeded"
            ProgressTaskState.FAILED -> "Failed"
            ProgressTaskState.CANCELLED -> "Cancelled"
            ProgressTaskState.DISMISSED -> "Dismissed"
        }

    val progress = when (latest) {
        is ProgressUpdateDeets.Amount -> latest
        else -> null
    }

    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(6.dp),
    ) {
        Text(
            text = task.title ?: task.id,
            style = MaterialTheme.typography.bodyMedium,
            fontWeight = FontWeight.SemiBold,
        )
        Text(
            text = stateText,
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        if (progress != null) {
            ProgressAmountBlock(progress, modifier = Modifier.fillMaxWidth())
        }
        if (latest is ProgressUpdateDeets.Status) {
            Text(
                text = latest.message,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        if (latest is ProgressUpdateDeets.Completed && latest.message != null) {
            Text(
                text = latest.message,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        HorizontalDivider()
    }
}

private sealed interface PlugImportWizardState {
    data class ChoosingPath(val path: String = "", val errorMessage: String? = null) : PlugImportWizardState

    data class Reviewing(
        val path: String,
        val preview: PlugSummary? = null,
        val isPreviewLoading: Boolean = false,
        val isImporting: Boolean = false,
        val errorMessage: String? = null,
    ) : PlugImportWizardState

    data class Success(val path: String, val preview: PlugSummary) : PlugImportWizardState
}

@Composable
private fun ImportPlugWizardDialog(plugsRepo: PlugsRepoFfi, onClose: () -> Unit, onImportSuccess: () -> Unit) {
    var wizardState by remember {
        mutableStateOf<PlugImportWizardState>(PlugImportWizardState.ChoosingPath())
    }
    val scope = rememberCoroutineScope()
    val picker = rememberDirectoryPickerLauncher { directory ->
        val selectedPath = directory?.path ?: return@rememberDirectoryPickerLauncher
        wizardState = PlugImportWizardState.ChoosingPath(path = selectedPath)
    }
    ImportPlugWizardDialogContent(
        wizardState = wizardState,
        actions = ImportPlugWizardDialogActions(
            plugsRepo = plugsRepo,
            scope = scope,
            onClose = onClose,
            onImportSuccess = onImportSuccess,
            onWizardStateChange = { nextState -> wizardState = nextState },
            onBrowse = { picker.launch() },
        ),
    )
}

@Composable
private fun ImportPlugWizardDialogContent(wizardState: PlugImportWizardState, actions: ImportPlugWizardDialogActions) {
    when (val currentState = wizardState) {
        is PlugImportWizardState.ChoosingPath -> ImportPlugWizardDialogChoosingPath(currentState, actions)
        is PlugImportWizardState.Reviewing -> ImportPlugWizardDialogReviewing(currentState, actions)
        is PlugImportWizardState.Success -> ImportPlugWizardDialogSuccess(currentState, actions.onClose)
    }
}

private data class ImportPlugWizardDialogActions(
    val plugsRepo: PlugsRepoFfi,
    val scope: CoroutineScope,
    val onClose: () -> Unit,
    val onImportSuccess: () -> Unit,
    val onWizardStateChange: (PlugImportWizardState) -> Unit,
    val onBrowse: () -> Unit,
)

@Composable
private fun ImportPlugWizardDialogChoosingPath(
    state: PlugImportWizardState.ChoosingPath,
    actions: ImportPlugWizardDialogActions,
) {
    ImportPlugWizardChoosePathStep(
        state = ImportPlugWizardChoosePathStepState(
            path = state.path,
            errorMessage = state.errorMessage,
            onPathChange = { nextPath ->
                actions.onWizardStateChange(PlugImportWizardState.ChoosingPath(path = nextPath, errorMessage = null))
            },
            onBrowse = actions.onBrowse,
            onCancel = actions.onClose,
            onReview = { selectedPath ->
                val path = selectedPath.trim()
                if (path.isBlank()) {
                    actions.onWizardStateChange(
                        PlugImportWizardState.ChoosingPath(
                            path = selectedPath,
                            errorMessage = "Choose an OCI layout directory.",
                        ),
                    )
                } else {
                    actions.onWizardStateChange(PlugImportWizardState.Reviewing(path = path, isPreviewLoading = true))
                    launchPlugPreview(
                        path = path,
                        plugsRepo = actions.plugsRepo,
                        scope = actions.scope,
                        onWizardStateChange = actions.onWizardStateChange,
                    )
                }
            },
        ),
    )
}

@Composable
private fun ImportPlugWizardDialogReviewing(
    state: PlugImportWizardState.Reviewing,
    actions: ImportPlugWizardDialogActions,
) {
    ImportPlugWizardReviewStep(
        state = ImportPlugWizardReviewStepState(
            path = state.path,
            preview = state.preview,
            isPreviewLoading = state.isPreviewLoading,
            isImporting = state.isImporting,
            errorMessage = state.errorMessage,
            onCancel = actions.onClose,
            onChangePath = {
                actions.onWizardStateChange(PlugImportWizardState.ChoosingPath(path = state.path))
            },
            onImport = import@{
                val preview = state.preview ?: return@import
                actions.onWizardStateChange(state.copy(isImporting = true, errorMessage = null))
                launchPlugImport(
                    request = PlugImportRequest(
                        path = state.path,
                        preview = preview,
                        plugsRepo = actions.plugsRepo,
                        scope = actions.scope,
                        onImportSuccess = actions.onImportSuccess,
                        onWizardStateChange = actions.onWizardStateChange,
                    ),
                )
            },
        ),
    )
}

@Composable
private fun ImportPlugWizardDialogSuccess(state: PlugImportWizardState.Success, onClose: () -> Unit) {
    ImportPlugWizardSuccessStep(
        path = state.path,
        preview = state.preview,
        onClose = onClose,
    )
}

@Composable
private fun ImportPlugWizardChoosePathStep(state: ImportPlugWizardChoosePathStepState) {
    var localPath by rememberSaveable { mutableStateOf(state.path) }
    LaunchedEffect(state.path) {
        if (localPath != state.path) {
            localPath = state.path
        }
    }
    ImportPlugWizardChoosePathStepContent(
        state = state.copy(
            path = localPath,
            onPathChange = { nextPath ->
                localPath = nextPath
                state.onPathChange(nextPath)
            },
            onReview = { state.onReview(localPath) },
        ),
    )
}

@Composable
private fun ImportPlugWizardChoosePathStepContent(state: ImportPlugWizardChoosePathStepState) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(24.dp)
            .testTag(SettingsScreenSemantics.PLUGS_ADD_DIALOG),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        ImportPlugWizardChoosePathHeader()
        ImportPlugWizardChoosePathDirectoryField(path = state.path, onPathChange = state.onPathChange)
        ImportPlugWizardChoosePathBrowseButton(onBrowse = state.onBrowse)
        ImportPlugWizardChoosePathError(message = state.errorMessage)
        ImportPlugWizardChoosePathActions(onCancel = state.onCancel, onReview = { state.onReview(state.path) })
    }
}

@Composable
private fun ImportPlugWizardChoosePathHeader() {
    Text(text = "Import plug", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.SemiBold)
    Text(text = "Choose an OCI layout directory to review before import.", style = MaterialTheme.typography.bodyLarge)
}

@Composable
private fun ImportPlugWizardChoosePathDirectoryField(path: String, onPathChange: (String) -> Unit) {
    OutlinedTextField(
        value = path,
        onValueChange = onPathChange,
        label = { Text("OCI layout directory") },
        singleLine = true,
        modifier = Modifier.fillMaxWidth().testTag(SettingsScreenSemantics.PLUGS_IMPORT_PATH_FIELD),
    )
}

@Composable
private fun ImportPlugWizardChoosePathBrowseButton(onBrowse: () -> Unit) {
    Button(onClick = onBrowse, modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_BROWSE_BUTTON)) {
        Icon(imageVector = Icons.Default.FolderOpen, contentDescription = null)
        Spacer(modifier = Modifier.width(8.dp))
        Text("Browse")
    }
}

@Composable
private fun ImportPlugWizardChoosePathError(message: String?) {
    message ?: return
    Text(
        text = message,
        style = MaterialTheme.typography.bodyMedium,
        color = MaterialTheme.colorScheme.error,
        modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_ERROR),
    )
}

@Composable
private fun ImportPlugWizardChoosePathActions(onCancel: () -> Unit, onReview: () -> Unit) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(12.dp, Alignment.End),
    ) {
        TextButton(onClick = onCancel) { Text("Cancel") }
        Button(onClick = onReview, modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_CONFIRM_BUTTON)) {
            Text("Review")
        }
    }
}

@Composable
private fun ImportPlugWizardReviewStep(state: ImportPlugWizardReviewStepState) {
    ImportPlugWizardReviewStepContent(state)
}

@Composable
private fun ImportPlugWizardReviewStepContent(state: ImportPlugWizardReviewStepState) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(24.dp)
            .testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        ImportPlugWizardReviewHeader(state.path)
        ImportPlugWizardReviewStatus(state)
        ImportPlugWizardReviewError(state.errorMessage)
        ImportPlugWizardReviewActions(state)
    }
}

@Composable
private fun ImportPlugWizardReviewHeader(path: String) {
    Text(text = "Review plug", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.SemiBold)
    Text(text = path, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
}

@Composable
private fun ImportPlugWizardReviewStatus(state: ImportPlugWizardReviewStepState) {
    when {
        state.isPreviewLoading -> {
            Row(
                modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_LOADING),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                CircularProgressIndicator(modifier = Modifier.width(18.dp).height(18.dp), strokeWidth = 2.dp)
                Text("Loading preview…", style = MaterialTheme.typography.bodyMedium)
            }
        }

        state.preview != null -> PlugImportPreviewCard(state.preview)
    }
}

@Composable
private fun ImportPlugWizardReviewError(message: String?) {
    message ?: return
    Text(
        text = message,
        style = MaterialTheme.typography.bodyMedium,
        color = MaterialTheme.colorScheme.error,
        modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_ERROR),
    )
}

@Composable
private fun ImportPlugWizardReviewActions(state: ImportPlugWizardReviewStepState) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(12.dp, Alignment.End),
    ) {
        TextButton(onClick = state.onCancel, enabled = !state.isImporting) { Text("Cancel") }
        TextButton(
            onClick = state.onChangePath,
            enabled = !state.isPreviewLoading && !state.isImporting,
            modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_CHANGE_PATH_BUTTON),
        ) {
            Text("Change path")
        }
        Button(
            onClick = state.onImport,
            enabled = state.preview != null && !state.isPreviewLoading && !state.isImporting,
            modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_IMPORT_BUTTON),
        ) {
            if (state.isImporting) {
                CircularProgressIndicator(
                    modifier = Modifier.padding(end = 8.dp).height(16.dp).width(16.dp),
                    strokeWidth = 2.dp,
                )
            }
            Text("Import")
        }
    }
}

@Composable
private fun ImportPlugWizardSuccessStep(path: String, preview: PlugSummary, onClose: () -> Unit) {
    Column(
        modifier =
        Modifier
            .fillMaxWidth()
            .padding(24.dp)
            .testTag(SettingsScreenSemantics.PLUGS_ADD_DIALOG),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text(
            text = "Import complete",
            style = MaterialTheme.typography.headlineSmall,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_SUCCESS),
        )
        PlugImportPreviewCard(preview)
        Text(
            text = "Imported from:",
            style = MaterialTheme.typography.bodyLarge,
        )
        Text(
            text = path,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.End,
        ) {
            TextButton(
                onClick = onClose,
                modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_CLOSE_BUTTON),
            ) {
                Text("Close")
            }
        }
    }
}

@Composable
private fun PlugImportPreviewCard(preview: PlugSummary) {
    OutlinedCard(
        modifier =
        Modifier
            .fillMaxWidth()
            .testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_PREVIEW),
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(
                text = preview.title,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold,
                modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_PREVIEW_TITLE),
            )
            Text(
                text = "${preview.id} · ${preview.version}",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_PREVIEW_ID),
            )
            if (preview.desc.isNotBlank()) {
                Text(
                    text = preview.desc,
                    style = MaterialTheme.typography.bodyMedium,
                    modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_PREVIEW_DESCRIPTION),
                )
            }
            Text(
                text = buildPlugCountsText(preview),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag(SettingsScreenSemantics.PLUGS_IMPORT_REVIEW_PREVIEW_COUNTS),
            )
        }
    }
}

private data class ImportPlugWizardChoosePathStepState(
    val path: String,
    val errorMessage: String?,
    val onPathChange: (String) -> Unit,
    val onBrowse: () -> Unit,
    val onCancel: () -> Unit,
    val onReview: (String) -> Unit,
)

private data class ImportPlugWizardReviewStepState(
    val path: String,
    val preview: PlugSummary?,
    val isPreviewLoading: Boolean,
    val isImporting: Boolean,
    val errorMessage: String?,
    val onCancel: () -> Unit,
    val onChangePath: () -> Unit,
    val onImport: () -> Unit,
)

private data class PlugImportRequest(
    val path: String,
    val preview: PlugSummary,
    val plugsRepo: PlugsRepoFfi,
    val scope: CoroutineScope,
    val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    val onImportSuccess: () -> Unit,
    val onWizardStateChange: (PlugImportWizardState) -> Unit,
)

private fun launchPlugPreview(
    path: String,
    plugsRepo: PlugsRepoFfi,
    scope: CoroutineScope,
    onWizardStateChange: (PlugImportWizardState) -> Unit,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    scope.launch {
        try {
            val preview = withContext(ioDispatcher) { plugsRepo.inspectOciLayout(path) }
            withContext(Dispatchers.Main.immediate) {
                onWizardStateChange(
                    PlugImportWizardState.Reviewing(
                        path = path,
                        preview = preview,
                    ),
                )
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: FfiException) {
            withContext(Dispatchers.Main.immediate) {
                onWizardStateChange(
                    PlugImportWizardState.ChoosingPath(
                        path = path,
                        errorMessage = buildImportErrorMessage("Preview", error),
                    ),
                )
            }
        }
    }
}

private fun launchPlugImport(request: PlugImportRequest) {
    request.scope.launch {
        try {
            withContext(request.ioDispatcher) { request.plugsRepo.importFromOciLayout(request.path) }
            withContext(Dispatchers.Main.immediate) {
                request.onImportSuccess()
                request.onWizardStateChange(
                    PlugImportWizardState.Success(path = request.path, preview = request.preview),
                )
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: FfiException) {
            withContext(Dispatchers.Main.immediate) {
                request.onWizardStateChange(
                    PlugImportWizardState.Reviewing(
                        path = request.path,
                        preview = request.preview,
                        errorMessage = buildImportErrorMessage("Import", error),
                    ),
                )
            }
        }
    }
}

private fun buildImportErrorMessage(action: String, error: FfiException): String {
    val message = error.message?.takeIf { it.isNotBlank() } ?: "unknown error"
    return "$action failed: $message"
}

@Composable
private fun rememberInstalledPlugsState(
    plugsRepo: PlugsRepoFfi,
    refreshToken: Int,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
): InstalledPlugsState {
    val state by androidx.compose.runtime.produceState<InstalledPlugsState>(
        initialValue = InstalledPlugsState.Loading,
        key1 = plugsRepo,
        key2 = refreshToken,
    ) {
        value = InstalledPlugsState.Loading
        value =
            try {
                val plugs =
                    withContext(ioDispatcher) { plugsRepo.listPlugs() }
                        .sortedWith(
                            compareBy<PlugSummary> { it.title.lowercase() }
                                .thenBy { it.namespace.lowercase() }
                                .thenBy { it.name.lowercase() }
                                .thenBy { it.version },
                        )
                InstalledPlugsState.Ready(plugs)
            } catch (error: CancellationException) {
                throw error
            } catch (error: FfiException) {
                InstalledPlugsState.Error(
                    "Failed to load installed plugs: ${error.message ?: "unknown error"}",
                )
            }
    }
    return state
}

private sealed interface InstalledPlugsState {
    data object Loading : InstalledPlugsState

    data class Ready(val plugs: List<PlugSummary>) : InstalledPlugsState

    data class Error(val message: String) : InstalledPlugsState
}

private fun buildPlugCountsText(plug: PlugSummary): String = listOf(
    countSummary(plug.facetCount.toInt(), "facet"),
    countSummary(plug.viewCount.toInt(), "view"),
    countSummary(plug.routineCount.toInt(), "routine"),
    countSummary(plug.processorCount.toInt(), "processor"),
    countSummary(plug.commandCount.toInt(), "command"),
).joinToString(" • ")

private fun countSummary(count: Int, label: String): String = if (count == 1) {
    "1 $label"
} else {
    "$count ${label}s"
}
