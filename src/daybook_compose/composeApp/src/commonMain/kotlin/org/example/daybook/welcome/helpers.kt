package org.example.daybook

import org.example.daybook.uniffi.AppFfiCtx
import org.example.daybook.uniffi.FfiException

internal suspend inline fun <T> withAppFfiCtx(crossinline block: suspend (AppFfiCtx) -> T): T {
    val gcx = AppFfiCtx.init()
    try {
        return block(gcx)
    } finally {
        gcx.close()
    }
}

internal fun describeThrowable(error: Throwable): String {
    val parts = mutableListOf<String>()
    var current: Throwable? = error
    var depth = 0
    while (current != null && depth < 4) {
        val className = current::class.simpleName ?: current::class.qualifiedName ?: "Throwable"
        val ffiMessage =
            (current as? FfiException)
                ?.message()
                ?.takeIf { it.isNotBlank() }
        val message = ffiMessage ?: current.message?.takeIf { it.isNotBlank() }
        val piece =
            when {
                message != null -> "$className: $message"
                else -> current.toString()
            }
        if (piece.isNotBlank()) {
            parts += piece
        }
        current = current.cause
        depth += 1
    }
    return parts.distinct().joinToString(" | ").ifBlank { error.toString() }
}

internal data class DestinationResolution(
    val path: String,
    val note: String? = null
)

internal suspend fun resolveNonClashingDestination(
    gcx: AppFfiCtx,
    requestedPath: String,
    autoRename: Boolean
): DestinationResolution {
    val base = requestedPath.trim()
    if (base.isBlank()) return DestinationResolution(path = base)

    val firstCheck = gcx.checkCloneDestination(base)
    val hasCollision = firstCheck.exists && firstCheck.isDir && !firstCheck.isEmpty
    if (!hasCollision || !autoRename) {
        return DestinationResolution(path = base)
    }

    val parent = parentPathOf(base)
    val leaf = leafNameOf(base).ifBlank { "daybook-repo" }
    for (idx in 2..9999) {
        val candidateLeaf = "$leaf-$idx"
        val candidate = joinPath(parent, candidateLeaf)
        val candidateCheck = gcx.checkCloneDestination(candidate)
        if (!candidateCheck.exists || (candidateCheck.isDir && candidateCheck.isEmpty)) {
            return DestinationResolution(
                path = candidate,
                note = "Destination existed; using $candidateLeaf."
            )
        }
    }

    error(
        "Unable to allocate non-clashing destination under '$parent' for base '$leaf' after trying suffixes 2..9999"
    )
}

internal fun parentPathOf(path: String): String {
    val normalized = path.trim().trimEnd('/', '\\')
    val slash = normalized.lastIndexOf('/')
    return if (slash <= 0) "" else normalized.substring(0, slash)
}

internal fun leafNameOf(path: String): String {
    val normalized = path.trim().trimEnd('/', '\\')
    val slash = normalized.lastIndexOf('/')
    return if (slash < 0) normalized else normalized.substring(slash + 1)
}

internal fun joinPath(parent: String, leaf: String): String {
    val parentTrimmed = parent.trim().trimEnd('/', '\\')
    val leafTrimmed = leaf.trim().trimStart('/', '\\')
    return when {
        parentTrimmed.isBlank() -> leafTrimmed
        leafTrimmed.isBlank() -> parentTrimmed
        else -> "$parentTrimmed/$leafTrimmed"
    }
}
