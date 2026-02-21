@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.settings

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import org.example.daybook.ConfigViewModel
import org.example.daybook.LocalContainer
import org.example.daybook.MltoolsBackendRow
import org.example.daybook.MltoolsProvisionState
import org.example.daybook.progress.ProgressAmountBlock
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsScreen(modifier: Modifier = Modifier) {
    val container = LocalContainer.current
    val configVm = viewModel { ConfigViewModel(container.configRepo, container.progressRepo) }

    val mltoolsConfig by configVm.mltoolsConfig.collectAsState()
    val provisionState by configVm.mltoolsProvisionState.collectAsState()
    val downloadTasks by configVm.mltoolsDownloadTasks.collectAsState()

    val sidebarPos = "RIGHT"
    val sidebarAutoHide = false

    Column(
        modifier =
            modifier
                .verticalScroll(rememberScrollState())
                .padding(16.dp)
    ) {
        Text(
            text = "Layout",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(bottom = 8.dp)
        )

        HorizontalDivider(modifier = Modifier.padding(bottom = 16.dp))

        Row(
            modifier = Modifier.padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Sidebar Position",
                modifier = Modifier.weight(1f),
                style = MaterialTheme.typography.bodyLarge
            )
            Switch(
                checked = sidebarPos == "RIGHT",
                onCheckedChange = { _ -> }
            )
            Text(
                text = if (sidebarPos == "RIGHT") "Right" else "Left",
                modifier = Modifier.padding(start = 8.dp),
                style = MaterialTheme.typography.bodyMedium
            )
        }

        Row(
            modifier = Modifier.padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Sidebar Auto-Hide",
                modifier = Modifier.weight(1f),
                style = MaterialTheme.typography.bodyLarge
            )
            Switch(
                checked = sidebarAutoHide,
                onCheckedChange = { _ -> }
            )
            Text(
                text = if (sidebarAutoHide) "On" else "Off",
                modifier = Modifier.padding(start = 8.dp),
                style = MaterialTheme.typography.bodyMedium
            )
        }

        Row(
            modifier = Modifier.padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Reset Layout Settings",
                modifier = Modifier.weight(1f),
                style = MaterialTheme.typography.bodyLarge
            )
            ResetLayoutButton(onReset = {})
        }

        Text(
            text = "MLTools",
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.padding(top = 20.dp, bottom = 8.dp)
        )
        HorizontalDivider(modifier = Modifier.padding(bottom = 12.dp))

        Row(
            modifier = Modifier.fillMaxWidth().padding(vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    text = "mobile_default",
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.SemiBold
                )
                val statusText =
                    when (val state = provisionState) {
                        is MltoolsProvisionState.Idle -> "Ready to download"
                        is MltoolsProvisionState.Running -> "Downloading and configuring models"
                        is MltoolsProvisionState.Succeeded -> "Provisioned successfully"
                        is MltoolsProvisionState.Failed -> "Failed: ${state.message}"
                    }
                Text(
                    text = statusText,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            Button(
                onClick = { configVm.provisionMobileDefaultMltools() },
                enabled = provisionState !is MltoolsProvisionState.Running,
            ) {
                if (provisionState is MltoolsProvisionState.Running) {
                    CircularProgressIndicator(modifier = Modifier.padding(end = 8.dp), strokeWidth = 2.dp)
                }
                Text(
                    if (provisionState is MltoolsProvisionState.Failed) {
                        "Retry"
                    } else {
                        "Download"
                    }
                )
            }
        }

        MltoolsBackendSection(title = "OCR Backends", rows = mltoolsConfig.ocr)
        MltoolsBackendSection(title = "Embed Backends", rows = mltoolsConfig.embed)
        MltoolsBackendSection(title = "LLM Backends", rows = mltoolsConfig.llm)

        Text(
            text = "MLTools Download Tasks",
            style = MaterialTheme.typography.titleMedium,
            modifier = Modifier.padding(top = 12.dp, bottom = 8.dp)
        )

        if (downloadTasks.isEmpty()) {
            Text(
                text = "No model download tasks yet",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 8.dp)
            )
        } else {
            Column(
                modifier = Modifier.fillMaxWidth(),
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                downloadTasks.forEach { task ->
                    MltoolsDownloadTaskRow(task)
                }
            }
        }
    }
}

@Composable
private fun MltoolsBackendSection(title: String, rows: List<MltoolsBackendRow>) {
    Text(
        text = title,
        style = MaterialTheme.typography.titleSmall,
        modifier = Modifier.padding(top = 8.dp, bottom = 4.dp)
    )

    if (rows.isEmpty()) {
        Text(
            text = "Not configured",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(bottom = 4.dp)
        )
        return
    }

    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        rows.forEach { row ->
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.Top
            ) {
                Text(
                    text = row.backend,
                    style = MaterialTheme.typography.bodySmall,
                    fontWeight = FontWeight.SemiBold,
                    modifier = Modifier.weight(0.3f)
                )
                Text(
                    text = row.details,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.weight(0.7f)
                )
            }
        }
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
        modifier = Modifier.fillMaxWidth().padding(8.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        Text(
            text = task.title ?: task.id,
            style = MaterialTheme.typography.bodyMedium,
            fontWeight = FontWeight.SemiBold
        )
        Text(
            text = stateText,
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        if (progress != null) {
            ProgressAmountBlock(progress, modifier = Modifier.fillMaxWidth())
        }
        if (latest is ProgressUpdateDeets.Status) {
            Text(
                text = latest.message,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        if (latest is ProgressUpdateDeets.Completed && latest.message != null) {
            Text(
                text = latest.message,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        HorizontalDivider(modifier = Modifier.padding(top = 4.dp))
    }
}
