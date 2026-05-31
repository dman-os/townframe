package org.example.daybook

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.example.daybook.uniffi.AppFfiCtx
import org.example.daybook.uniffi.FfiException

internal suspend inline fun <T> withAppFfiCtx(crossinline block: suspend (AppFfiCtx) -> T): T {
    val gcx = withContext(Dispatchers.IO) { AppFfiCtx.init() }
    return try {
        block(gcx)
    } finally {
        withContext(Dispatchers.IO) {
            gcx.close()
        }
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
    val parsed = parsePath(path)
    if (parsed.parts.isEmpty()) return parsed.root
    return composePath(
        root = parsed.root,
        parts = parsed.parts.dropLast(1),
        separator = parsed.separator
    )
}

internal fun leafNameOf(path: String): String {
    val parsed = parsePath(path)
    return parsed.parts.lastOrNull() ?: ""
}

internal fun joinPath(parent: String, leaf: String): String {
    val parentParsed = parsePath(parent)
    val leafTrimmed = leaf.trim().trimStart('/', '\\')
    val leafParts = leafTrimmed.replace('\\', '/').split('/').filter { it.isNotBlank() }
    return when {
        parentParsed.root.isBlank() && parentParsed.parts.isEmpty() -> leafTrimmed
        leafParts.isEmpty() -> composePath(parentParsed.root, parentParsed.parts, parentParsed.separator)
        else ->
            composePath(
                root = parentParsed.root,
                parts = parentParsed.parts + leafParts,
                separator = parentParsed.separator
            )
    }
}

private data class ParsedPath(
    val root: String,
    val parts: List<String>,
    val separator: Char
)

private fun parsePath(path: String): ParsedPath {
    val trimmed = path.trim()
    if (trimmed.isBlank()) return ParsedPath(root = "", parts = emptyList(), separator = '/')

    val separator = if (trimmed.contains('\\') && !trimmed.contains('/')) '\\' else '/'
    val normalized = trimmed.trimEnd('/', '\\').replace('\\', '/')
    if (normalized.isBlank()) {
        return ParsedPath(root = if (trimmed.startsWith("/")) "/" else "", parts = emptyList(), separator = separator)
    }

    val drivePrefix =
        if (normalized.length >= 2 && normalized[1] == ':') {
            normalized.substring(0, 2)
        } else {
            ""
        }
    val hasAbsoluteSlashAfterDrive = drivePrefix.isNotBlank() && normalized.getOrNull(2) == '/'
    val root =
        when {
            normalized.startsWith("/") -> "/"
            drivePrefix.isNotBlank() && hasAbsoluteSlashAfterDrive ->
                if (separator == '\\') "$drivePrefix\\" else "$drivePrefix/"
            drivePrefix.isNotBlank() -> drivePrefix
            else -> ""
        }
    val body =
        when {
            root == "/" -> normalized.removePrefix("/")
            drivePrefix.isNotBlank() && hasAbsoluteSlashAfterDrive -> normalized.drop(3)
            drivePrefix.isNotBlank() -> normalized.drop(2).trimStart('/')
            else -> normalized
        }
    val parts = body.split('/').filter { it.isNotBlank() }
    return ParsedPath(root = root, parts = parts, separator = separator)
}

private fun composePath(root: String, parts: List<String>, separator: Char): String {
    val joined = parts.joinToString(separator.toString())
    if (root.isBlank()) return joined
    if (parts.isEmpty()) return root
    return when {
        root == "/" || root.endsWith("/") || root.endsWith("\\") -> root + joined
        root.length == 2 && root[1] == ':' -> "$root$separator$joined"
        else -> "$root$separator$joined"
    }
}
