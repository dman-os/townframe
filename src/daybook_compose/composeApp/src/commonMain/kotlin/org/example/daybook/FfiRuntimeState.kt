package org.example.daybook

import org.example.daybook.uniffi.core.KnownRepoEntry

sealed interface FfiRuntimeState {
    data object Loading : FfiRuntimeState

    data class Welcome(val repos: List<KnownRepoEntry>) : FfiRuntimeState

    data class OpeningRepo(val repoPath: String) : FfiRuntimeState

    data class Ready(val container: AppContainer) : FfiRuntimeState

    data class Error(val throwable: Throwable) : FfiRuntimeState
}
