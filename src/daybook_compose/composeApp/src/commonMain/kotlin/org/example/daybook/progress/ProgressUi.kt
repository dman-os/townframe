package org.example.daybook.progress

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.example.daybook.uniffi.core.ProgressUnit
import org.example.daybook.uniffi.core.ProgressUpdateDeets

@Composable
fun ProgressAmountBlock(
    amount: ProgressUpdateDeets.Amount,
    modifier: Modifier = Modifier,
) {
    Column(modifier = modifier) {
        val progress = progressFraction(amount)
        if (progress != null) {
            LinearProgressIndicator(
                progress = { progress },
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(bottom = 4.dp),
            )
        }
        Text(
            text = formatAmountSummary(amount.done, amount.total, amount.unit),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        amount.message?.let { message ->
            Text(
                text = message,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

fun progressFraction(amount: ProgressUpdateDeets.Amount): Float? {
    val total = amount.total ?: return null
    if (total == 0UL) {
        return null
    }
    return (amount.done.toFloat() / total.toFloat()).coerceIn(0f, 1f)
}

fun formatAmountSummary(done: ULong, total: ULong?, unit: ProgressUnit): String {
    val doneLabel = formatUnitValue(done, unit)
    val totalLabel = total?.let { formatUnitValue(it, unit) } ?: "?"
    return "$doneLabel / $totalLabel"
}

fun formatUnitValue(value: ULong, unit: ProgressUnit): String =
    when (unit) {
        is ProgressUnit.Bytes -> formatBytes(value)
        is ProgressUnit.Generic -> "${value.toString()} ${unit.label}"
    }

fun formatBytes(bytes: ULong): String {
    if (bytes < 1024UL) {
        return "$bytes B"
    }
    val units = arrayOf("KB", "MB", "GB", "TB")
    var value = bytes.toDouble()
    var unitIndex = -1
    while (value >= 1024.0 && unitIndex < units.lastIndex) {
        value /= 1024.0
        unitIndex += 1
    }
    return if (value >= 100) {
        "${value.toInt()} ${units[unitIndex]}"
    } else {
        String.format("%.1f %s", value, units[unitIndex])
    }
}
