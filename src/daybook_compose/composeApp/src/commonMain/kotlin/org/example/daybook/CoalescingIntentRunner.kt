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
    private val onIntent: suspend (T) -> Unit,
) {
    private val lock = Mutex()
    private var pending: T? = null
    private var drainJob: Job? = null
    private var cancelled = false

    fun submit(intent: T) {
        scope.launch {
            lock.withLock {
                if (cancelled) {
                    return@withLock
                }
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

    /**
     * Terminal: drops the pending intent and refuses later [submit]s.
     *
     * [submit] launches onto [scope] first and only takes [lock] later, so a
     * submit queued just before this call used to find no active drain job and
     * start a fresh one *after* the owner went away. That is how a debounced
     * drawer refresh outlived teardown and reached an already-destroyed FFI
     * repo. Cancelling [drainJob] alone cannot express that, so the flag does.
     */
    fun cancel() {
        cancelled = true
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
