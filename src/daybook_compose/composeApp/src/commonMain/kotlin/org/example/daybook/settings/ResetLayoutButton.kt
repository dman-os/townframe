@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package org.example.daybook.settings

import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.graphics.Color
import kotlinx.coroutines.delay

@Composable
fun ResetLayoutButton(
    onReset: () -> Unit
) {
    var isConfirming by remember { mutableStateOf(false) }
    var isConfirmEnabled by remember { mutableStateOf(false) }
    
    // Enable confirm button after 1 second delay
    LaunchedEffect(isConfirming) {
        if (isConfirming) {
            isConfirmEnabled = false
            delay(1000) // 1 second delay
            isConfirmEnabled = true
        }
    }
    
    Button(
        onClick = {
            if (!isConfirming) {
                // First click - enter confirmation state
                isConfirming = true
            } else if (isConfirmEnabled) {
                // Second click after delay - actually reset
                onReset()
                isConfirming = false
                isConfirmEnabled = false
            }
        },
        enabled = !isConfirming || isConfirmEnabled,
        colors = ButtonDefaults.buttonColors(
            containerColor = if (isConfirming) Color(0xFFD32F2F) else ButtonDefaults.buttonColors().containerColor,
            disabledContainerColor = if (isConfirming) Color(0xFFD32F2F) else ButtonDefaults.buttonColors().disabledContainerColor
        )
    ) {
        Text(
            text = if (isConfirming) "Confirm" else "Reset"
        )
    }
}
