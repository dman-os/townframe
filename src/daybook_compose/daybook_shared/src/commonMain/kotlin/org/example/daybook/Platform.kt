package org.example.daybook

interface Platform {
    val name: String
}

expect fun getPlatform(): Platform
