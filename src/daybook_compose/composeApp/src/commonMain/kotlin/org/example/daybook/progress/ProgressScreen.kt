@file:OptIn(kotlin.time.ExperimentalTime::class)

package org.example.daybook.progress

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
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.CheckCircle
import androidx.compose.material.icons.filled.Circle
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.Sync
import androidx.compose.material.icons.filled.Error
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material.icons.filled.Warning
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ElevatedCard
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import kotlinx.coroutines.delay
import org.example.daybook.LocalContainer
import org.example.daybook.uniffi.core.ProgressFinalState
import org.example.daybook.uniffi.core.ProgressTask
import org.example.daybook.uniffi.core.ProgressTaskState
import org.example.daybook.uniffi.core.ProgressUpdateDeets
import org.example.daybook.uniffi.core.ProgressUpdateEntry

@Composable
fun ProgressList(modifier: Modifier = Modifier) {
    val progressRepo = LocalContainer.current.progressRepo
    val vm = viewModel { ProgressViewModel(progressRepo) }
    val state by vm.state.collectAsState()

    when (val data = state) {
        is ProgressState.Loading -> {
            Column(
                modifier = modifier.fillMaxSize(),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center
            ) {
                CircularProgressIndicator()
                Spacer(Modifier.height(12.dp))
                Text("Loading progress", style = MaterialTheme.typography.titleMedium)
            }
        }

        is ProgressState.Error -> {
            Column(
                modifier = modifier.fillMaxSize().padding(16.dp),
                verticalArrangement = Arrangement.Center
            ) {
                ElevatedCard {
                    Column(Modifier.fillMaxWidth().padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Icon(Icons.Default.Error, contentDescription = null, tint = MaterialTheme.colorScheme.error)
                            Spacer(Modifier.size(8.dp))
                            Text("Progress unavailable", style = MaterialTheme.typography.titleMedium)
                            Spacer(Modifier.weight(1f))
                            IconButton(onClick = { vm.refresh() }) {
                                Icon(Icons.Default.Refresh, contentDescription = "Retry")
                            }
                        }
                        Text(
                            data.error.message(),
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            }
        }

        is ProgressState.Data -> {
            val selectedTask = data.tasks.firstOrNull { it.id == data.selectedTaskId }
            val activeCount = data.tasks.count { it.state == ProgressTaskState.ACTIVE }
            Column(modifier = modifier.fillMaxSize().padding(12.dp)) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        "$activeCount active • ${data.tasks.size} total",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.weight(1f))
                    IconButton(onClick = { vm.refresh() }) {
                        Icon(Icons.Default.Refresh, contentDescription = "Refresh")
                    }
                    IconButton(onClick = { vm.clearCompleted() }) {
                        Icon(Icons.Default.Delete, contentDescription = "Clear completed")
                    }
                }
                if (selectedTask != null) {
                    ProgressDetailScreen(
                        task = selectedTask,
                        updates = data.selectedTaskUpdates,
                        onBack = { vm.selectTask(null) }
                    )
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxWidth().weight(1f),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        items(data.tasks, key = { it.id }) { task ->
                            ProgressTaskRow(
                                task = task,
                                onClick = { vm.selectTask(task.id) },
                                onDismiss = { vm.dismiss(task.id) }
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun ProgressTaskRow(
    task: ProgressTask,
    onClick: () -> Unit,
    onDismiss: () -> Unit
) {
    val containerColor =
        MaterialTheme.colorScheme.surfaceContainerLow
    ElevatedCard(
        modifier = Modifier.fillMaxWidth().clickable(onClick = onClick),
    ) {
        Column(
            modifier = Modifier.fillMaxWidth().background(containerColor).padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
                Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                    val typeInfo = progressTypeInfo(task)
                    Icon(typeInfo.icon, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
                    Spacer(Modifier.size(8.dp))
                    val endAtSecs = taskEndTimestamp(task)
                    Column(Modifier.weight(1f)) {
                        Text(
                            typeInfo.label,
                            style = MaterialTheme.typography.titleSmall,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            LiveDurationText(
                                createdAtSecs = task.createdAt.epochSeconds,
                                endAtSecs = endAtSecs,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                            Spacer(Modifier.size(6.dp))
                            Text(
                                task.title ?: task.id,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis
                            )
                        }
                    }
                    IconButton(onClick = onDismiss) {
                        Icon(Icons.Default.Delete, contentDescription = "Dismiss")
                    }
                }
            when (val updateEntry = task.latestUpdate?.update?.deets) {
            is ProgressUpdateDeets.Amount -> {
                val progress =
                    if (updateEntry.total == null || updateEntry.total == 0UL) {
                        null
                    } else {
                        updateEntry.done.toFloat() / updateEntry.total.toFloat()
                    }
                    if (progress != null) {
                        LinearProgressIndicator(
                            progress = { progress.coerceIn(0f, 1f) },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                    val totalText = updateEntry.total?.toString() ?: "?"
                    Text(
                        "${updateEntry.done}/${totalText} ${updateEntry.unit}",
                        style = MaterialTheme.typography.bodyMedium
                    )
                    if (updateEntry.message != null) {
                        Text(
                            updateEntry.message,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                is ProgressUpdateDeets.Status -> {
                    TimelineUpdateRow(
                        at = task.latestUpdate?.at?.epochSeconds ?: task.updatedAt.epochSeconds,
                        deets = updateEntry
                    )
                }

                is ProgressUpdateDeets.Completed -> {
                    TimelineUpdateRow(
                        at = task.latestUpdate?.at?.epochSeconds ?: task.updatedAt.epochSeconds,
                        deets = updateEntry
                    )
                }

                null -> {
                    Text(
                        "No updates yet",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        }
    }
}

@Composable
private fun TimelineUpdateRow(at: Long, deets: ProgressUpdateDeets) {
    val icon: androidx.compose.ui.graphics.vector.ImageVector
    val tint: Color
    val title: String
    val body: String
    when (deets) {
        is ProgressUpdateDeets.Status -> {
            title = "Status"
            body = deets.message
            when (deets.severity.name) {
                "ERROR" -> {
                    icon = Icons.Default.Error
                    tint = MaterialTheme.colorScheme.error
                }

                "WARN" -> {
                    icon = Icons.Default.Warning
                    tint = Color(0xFFE18D00)
                }

                else -> {
                    icon = Icons.Default.Info
                    tint = MaterialTheme.colorScheme.primary
                }
            }
        }

        is ProgressUpdateDeets.Amount -> {
            icon = Icons.Default.Info
            tint = MaterialTheme.colorScheme.primary
            title = "Progress"
            body =
                "${deets.done}/${deets.total ?: "?"} ${deets.unit}" +
                    if (deets.message != null) " • ${deets.message}" else ""
        }

        is ProgressUpdateDeets.Completed -> {
            val stateLabel = deets.state.name.lowercase().replaceFirstChar { it.uppercase() }
            val completedLabel = "Completed"
            when (deets.state) {
                ProgressFinalState.SUCCEEDED -> {
                    icon = Icons.Default.CheckCircle
                    tint = MaterialTheme.colorScheme.primary
                }
                ProgressFinalState.FAILED -> {
                    icon = Icons.Default.Error
                    tint = MaterialTheme.colorScheme.error
                }
                ProgressFinalState.CANCELLED -> {
                    icon = Icons.Default.Warning
                    tint = Color(0xFFE18D00)
                }
                ProgressFinalState.DISMISSED -> {
                    icon = Icons.Default.Close
                    tint = MaterialTheme.colorScheme.onSurfaceVariant
                }
            }
            title = stateLabel
            body = deets.message ?: completedLabel
        }
    }

    Row(
        modifier = Modifier.fillMaxWidth(),
        verticalAlignment = Alignment.Top
    ) {
        Icon(icon, contentDescription = null, tint = tint, modifier = Modifier.padding(top = 2.dp))
        Spacer(Modifier.size(8.dp))
        Column(Modifier.weight(1f)) {
            Row(modifier = Modifier.fillMaxWidth()) {
                Text(title, style = MaterialTheme.typography.labelLarge)
                Spacer(Modifier.weight(1f))
                Text(
                    formatClock(at),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            Text(
                body,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun ProgressDetailScreen(
    task: ProgressTask,
    updates: List<ProgressUpdateEntry>,
    onBack: () -> Unit
) {
    ElevatedCard(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(12.dp)
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            val typeInfo = progressTypeInfo(task)
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(typeInfo.icon, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
                Spacer(Modifier.size(8.dp))
                Column(Modifier.weight(1f)) {
                    Text(
                        typeInfo.label,
                        style = MaterialTheme.typography.titleMedium,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        LiveDurationText(
                            createdAtSecs = task.createdAt.epochSeconds,
                            endAtSecs = taskEndTimestamp(task),
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Spacer(Modifier.size(6.dp))
                        Text(
                            task.title ?: task.id,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }
                IconButton(onClick = onBack) {
                    Icon(Icons.Default.Close, contentDescription = "Back to list")
                }
            }
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically
            ) {
                TaskStateIcon(task.state)
                Spacer(Modifier.size(8.dp))
                Text(
                    task.state.name.lowercase()
                        .replaceFirstChar { it.uppercase() },
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            if (task.tags.isNotEmpty()) {
                Column(
                    modifier = Modifier.fillMaxWidth(),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    task.tags.forEach { tag ->
                        Surface(
                            shape = RoundedCornerShape(100.dp),
                            color = MaterialTheme.colorScheme.secondaryContainer
                        ) {
                            Text(
                                text = tag,
                                style = MaterialTheme.typography.labelSmall,
                                modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
                            )
                        }
                    }
                }
            }
            Spacer(Modifier.height(4.dp))
            Text("Timeline", style = MaterialTheme.typography.titleSmall)
            Spacer(Modifier.height(4.dp))
            LazyColumn(
                modifier = Modifier.fillMaxWidth().heightIn(max = 220.dp),
                verticalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                items(updates, key = { it.sequence }) { update ->
                    TimelineUpdateRow(at = update.at.epochSeconds, deets = update.update.deets)
                }
            }
        }
    }
}

@Composable
private fun TaskStateIcon(state: ProgressTaskState) {
    val (icon, tint) = when (state) {
        ProgressTaskState.ACTIVE -> Icons.Default.Info to MaterialTheme.colorScheme.primary
        ProgressTaskState.SUCCEEDED -> Icons.Default.CheckCircle to MaterialTheme.colorScheme.primary
        ProgressTaskState.FAILED -> Icons.Default.Error to MaterialTheme.colorScheme.error
        ProgressTaskState.CANCELLED -> Icons.Default.Warning to Color(0xFFE18D00)
        ProgressTaskState.DISMISSED -> Icons.Default.Close to MaterialTheme.colorScheme.onSurfaceVariant
    }
    Icon(icon, contentDescription = state.name, tint = tint)
}

@Composable
private fun LiveDurationText(
    createdAtSecs: Long,
    endAtSecs: Long? = null,
    modifier: Modifier = Modifier,
    style: TextStyle = MaterialTheme.typography.bodySmall,
    color: Color = MaterialTheme.colorScheme.onSurfaceVariant
) {
    if (endAtSecs != null) {
        val elapsed = (endAtSecs - createdAtSecs).coerceAtLeast(0)
        Text(
            text = formatDuration(elapsed),
            modifier = modifier,
            style = style,
            color = color
        )
        return
    }

    var elapsedSeconds by remember(createdAtSecs) {
        mutableStateOf(((System.currentTimeMillis() / 1000) - createdAtSecs).coerceAtLeast(0))
    }

    LaunchedEffect(createdAtSecs) {
        while (true) {
            delay(1000)
            elapsedSeconds = ((System.currentTimeMillis() / 1000) - createdAtSecs).coerceAtLeast(0)
        }
    }

    Text(
        text = formatDuration(elapsedSeconds),
        modifier = modifier,
        style = style,
        color = color
    )
}


private fun formatDuration(secondsRaw: Long): String {
    val seconds = secondsRaw.coerceAtLeast(0)
    val hours = seconds / 3600
    val mins = (seconds % 3600) / 60
    val secs = seconds % 60
    return if (hours > 0) {
        "${hours}h ${mins}m ${secs}s"
    } else if (mins > 0) {
        "${mins}m ${secs}s"
    } else {
        "${secs}s"
    }
}

private fun taskEndTimestamp(task: ProgressTask): Long? =
    when (task.state) {
        ProgressTaskState.ACTIVE -> null
        else -> task.latestUpdate?.at?.epochSeconds ?: task.updatedAt.epochSeconds
    }

private data class ProgressTypeInfo(
    val label: String,
    val icon: androidx.compose.ui.graphics.vector.ImageVector
)

private fun progressTypeInfo(task: ProgressTask): ProgressTypeInfo {
    val typeTag = task.tags.firstOrNull { it.startsWith("/type/") }
    val type = typeTag?.removePrefix("/type/")?.trim().orEmpty()
    return when (type) {
        "dispatch" -> ProgressTypeInfo(label = "Dispatch", icon = Icons.Default.Sync)
        else -> ProgressTypeInfo(label = task.title ?: task.id, icon = Icons.Default.Circle)
    }
}

private fun formatClock(unixSecs: Long): String {
    val secs = unixSecs % 86_400
    val h = secs / 3600
    val m = (secs % 3600) / 60
    val s = secs % 60
    return "%02d:%02d:%02d".format(h, m, s)
}
