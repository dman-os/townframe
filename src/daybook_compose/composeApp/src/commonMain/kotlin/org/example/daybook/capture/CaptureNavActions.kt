package org.example.daybook.capture

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow

/**
 * Cross-screen nav actions for Capture re-tap behavior from the bottom bar.
 */
object CaptureNavActions {
    private val _modeCycleRequests = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    val modeCycleRequests: SharedFlow<Unit> = _modeCycleRequests.asSharedFlow()

    fun requestModeCycle() {
        _modeCycleRequests.tryEmit(Unit)
    }
}

