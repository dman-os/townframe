package org.example.daybook

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class CoalescingIntentRunner<T>(
    private val scope: CoroutineScope,
    private val debounceMs: Long,
    private val merge: (T, T) -> T,
    private val onIntent: suspend (T) -> Unit
) {
    private val lock = Mutex()
    private var pending: T? = null
    private var drainJob: Job? = null

    fun submit(intent: T) {
        scope.launch {
            lock.withLock {
                pending = pending?.let { merge(it, intent) } ?: intent
                if (drainJob?.isActive != true) {
                    drainJob = scope.launch {
                        delay(debounceMs)
                        drainLoop()
                    }
                }
            }
        }
    }

    fun cancel() {
        drainJob?.cancel()
    }

    private suspend fun drainLoop() {
        while (true) {
            val next =
                lock.withLock {
                    val value = pending
                    pending = null
                    if (value == null) {
                        drainJob = null
                    }
                    value
                }

            if (next == null) {
                return
            }

            onIntent(next)
        }
    }
}
