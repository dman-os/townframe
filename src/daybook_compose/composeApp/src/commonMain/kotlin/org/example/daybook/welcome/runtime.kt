package org.example.daybook

import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import org.example.daybook.uniffi.FfiCtx
import org.example.daybook.uniffi.core.RepoConfig

interface AppFfiServices {
    suspend fun getRepoConfig(): RepoConfig
    suspend fun isRepoUsable(repoPath: String): Boolean
    suspend fun openRepoFfiCtx(repoPath: String): FfiCtx
    suspend fun forgetKnownRepo(repoId: String): RepoConfig
}

private class DefaultAppFfiServices : AppFfiServices {
    override suspend fun getRepoConfig(): RepoConfig = withAppFfiCtx { gcx ->
        gcx.getRepoConfig()
    }

    override suspend fun isRepoUsable(repoPath: String): Boolean = withAppFfiCtx { gcx ->
        gcx.isRepoUsable(repoPath)
    }

    override suspend fun openRepoFfiCtx(repoPath: String): FfiCtx = withAppFfiCtx { gcx ->
        FfiCtx.init(repoPath, gcx)
    }

    override suspend fun forgetKnownRepo(repoId: String): RepoConfig = withAppFfiCtx { gcx ->
        gcx.forgetKnownRepo(repoId)
        gcx.getRepoConfig()
    }
}

@Composable
fun rememberAppFfiServices(): AppFfiServices = remember { DefaultAppFfiServices() }
