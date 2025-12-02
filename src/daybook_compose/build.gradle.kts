plugins {
    // this is necessary to avoid the plugins to be loaded multiple times
    // in each subproject's classloader
    alias(libs.plugins.androidApplication) apply false
    alias(libs.plugins.androidLibrary) apply false
    alias(libs.plugins.composeHotReload) apply false
    alias(libs.plugins.composeMultiplatform) apply false
    alias(libs.plugins.composeCompiler) apply false
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.kotlinAndroid) apply false
    alias(libs.plugins.gobleyCargo) apply false
    alias(libs.plugins.gobleyUniffi) apply false
    kotlin("plugin.atomicfu") version libs.versions.kotlin apply false
    id("com.github.ben-manes.versions") version "0.51.0"
}

tasks.register("printJavaHome") {
    doLast {
        println("java.home = " + System.getProperty("java.home"))
        println("java.version = " + System.getProperty("java.version"))
    }
}
